from django.db import DatabaseError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class OverriddenFeatures:
    """Reads through to the real backend features except for the flags the
    test overrides.
    """

    def __init__(self, features, overrides):
        self._features = features
        self._overrides = overrides

    def __getattr__(self, name):
        if name in self._overrides:
            return self._overrides[name]
        return getattr(self._features, name)


class OverriddenFeaturesConnection:
    """Wraps a connection so that only ``features`` is substituted.

    The compiler keeps ``connection`` as a plain instance attribute, so
    swapping it here affects a single compiler and leaves the shared
    connection -- and any other test -- untouched.
    """

    def __init__(self, connection, overrides):
        self._connection = connection
        self.features = OverriddenFeatures(connection.features, overrides)

    def __getattr__(self, name):
        return getattr(self._connection, name)


class TestCombinatorPartOrdering(AsyncioTestCase):
    """``_get_combinator_part_sql`` drops the ORDER BY of a combined query's
    parts on backends that cannot carry ordering there.

    PostgreSQL supports it, so the branch is unreachable with the real feature
    flags and each case below substitutes them on a single compiler.
    """

    def _union_compiler(self, queryset=None, **overrides):
        if queryset is None:
            queryset = TestModel.async_objects.order_by("name")
        compiler = queryset.union(queryset).query.get_compiler(using="default")
        compiler.connection = OverriddenFeaturesConnection(
            compiler.connection, overrides
        )
        return compiler

    async def test_ordering_kept_by_default(self):
        # Baseline: with PostgreSQL's real flags the condition is false and
        # the parts keep their ORDER BY.
        compiler = self._union_compiler()

        self.assertIn("ORDER BY", compiler.as_sql()[0])

    async def test_ordering_cleared_when_subquery_required(self):
        # requires_compound_order_by_subquery and not
        # ignores_unnecessary_order_by_in_subqueries -- the backend would need
        # a subquery to order a part, so the ordering is dropped instead.
        compiler = self._union_compiler(
            requires_compound_order_by_subquery=True,
            ignores_unnecessary_order_by_in_subqueries=False,
        )

        self.assertNotIn("ORDER BY", compiler.as_sql()[0])

    async def test_ordering_kept_when_backend_ignores_it(self):
        # A backend that discards pointless ordering in subqueries does not
        # need the compiler's help, so the ORDER BY survives.
        compiler = self._union_compiler(
            requires_compound_order_by_subquery=True,
            ignores_unnecessary_order_by_in_subqueries=True,
        )

        self.assertIn("ORDER BY", compiler.as_sql()[0])

    async def test_ordering_cleared_without_parentheses_support(self):
        # The second half of the condition stands on its own: without
        # parentheses the parts are inlined, so ordering cannot be kept.
        compiler = self._union_compiler(
            supports_parentheses_in_compound=False,
        )

        self.assertNotIn("ORDER BY", compiler.as_sql()[0])

    async def test_ordering_kept_when_parentheses_supported(self):
        # Same flag the other way round, with the first half of the condition
        # held false, to show it is what drives the previous case.
        compiler = self._union_compiler(
            supports_parentheses_in_compound=True,
            requires_compound_order_by_subquery=False,
        )

        self.assertIn("ORDER BY", compiler.as_sql()[0])

    async def test_unordered_parts_are_left_alone(self):
        # get_order_by() is empty, so the query is never cloned even though
        # the feature flags ask for the ordering to be cleared.
        compiler = self._union_compiler(
            queryset=TestModel.async_objects.filter(value=1),
            requires_compound_order_by_subquery=True,
            ignores_unnecessary_order_by_in_subqueries=False,
        )
        query = compiler.query

        self.assertNotIn("ORDER BY", compiler.as_sql()[0])
        self.assertIs(compiler.query, query)

    async def test_ordering_kept_when_clearing_is_unsafe(self):
        # clear_ordering(force=False) declines to drop ordering that the query
        # depends on: DISTINCT ON requires a matching ORDER BY.
        compiler = self._union_compiler(
            queryset=TestModel.async_objects.order_by("name").distinct("name"),
            requires_compound_order_by_subquery=True,
            ignores_unnecessary_order_by_in_subqueries=False,
        )

        self.assertIn("ORDER BY", compiler.as_sql()[0])


class TestCombinatorRejectsSlicingAndOrdering(AsyncioTestCase):
    """get_combinator_sql() refuses slicing and ordering on the parts of a
    combined query when the backend cannot support them there.

    PostgreSQL can, so these guards only run with
    supports_slicing_ordering_in_compound overridden.
    """

    def _union_compiler(self, queryset):
        other = TestModel.async_objects.filter(value=1)
        compiler = queryset.union(other).query.get_compiler(using="default")
        compiler.connection = OverriddenFeaturesConnection(
            compiler.connection,
            {"supports_slicing_ordering_in_compound": False},
        )
        return compiler

    async def test_ordered_part_is_rejected(self):
        compiler = self._union_compiler(
            TestModel.async_objects.order_by("name")
        )

        with self.assertRaises(DatabaseError) as ctx:
            compiler.as_sql()

        self.assertEqual(
            str(ctx.exception),
            "ORDER BY not allowed in subqueries of compound statements.",
        )

    async def test_sliced_part_is_rejected(self):
        # Checked before the ordering guard, so a sliced part reports slicing
        # even though it is ordered too.
        compiler = self._union_compiler(
            TestModel.async_objects.order_by("name")[:2]
        )

        with self.assertRaises(DatabaseError) as ctx:
            compiler.as_sql()

        self.assertEqual(
            str(ctx.exception),
            "LIMIT/OFFSET not allowed in subqueries of compound statements.",
        )

    async def test_ordering_on_the_outer_query_is_allowed(self):
        # Only the parts are checked -- ordering the combined query itself is
        # what a caller is expected to do instead.
        compiler = self._union_compiler(
            TestModel.async_objects.filter(value=2)
        )
        compiler.query.order_by = ("name",)

        self.assertIn("ORDER BY", compiler.as_sql()[0])

    async def test_unordered_unsliced_parts_are_accepted(self):
        compiler = self._union_compiler(
            TestModel.async_objects.filter(value=2)
        )

        self.assertNotIn("ORDER BY", compiler.as_sql()[0])

    async def test_supported_backend_allows_ordered_parts(self):
        # The same query compiles fine on PostgreSQL's real feature flags,
        # confirming the guard is what rejects it above.
        queryset = TestModel.async_objects.order_by("name")
        other = TestModel.async_objects.filter(value=1)
        compiler = queryset.union(other).query.get_compiler(using="default")

        self.assertIn("ORDER BY", compiler.as_sql()[0])
