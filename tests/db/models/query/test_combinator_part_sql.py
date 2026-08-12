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
        # Query.get_compiler() re-resolves the connection from `using`, so the
        # substituted features would not reach the compilers built for each
        # part while it is set.
        compiler.using = None
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
