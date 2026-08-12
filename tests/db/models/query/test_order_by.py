from django.core.exceptions import FieldError
from django.db.models import (
    Count,
    F,
)
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestOrderBy(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Test1", value=3).async_save()
        await TestModel(name="Test2", value=1).async_save()
        await TestModel(name="Test3", value=2).async_save()

    async def test_order_by_single_field(self):
        results = [
            obj async for obj in TestModel.async_objects.order_by("value")
        ]

        self.assertEqual(
            len(results), 3, "Should return all objects ordered by 'value'"
        )
        self.assertEqual(results[0].value, 1, "First object value should be 1")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 3, "Third object value should be 3")

    async def test_order_by_descending(self):
        results = [
            obj async for obj in TestModel.async_objects.order_by("-value")
        ]

        self.assertEqual(
            len(results),
            3,
            "Should return all objects ordered by 'value' descending",
        )
        self.assertEqual(results[0].value, 3, "First object value should be 3")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 1, "Third object value should be 1")

    async def test_order_by_multiple_fields(self):
        results = [
            obj
            async for obj in TestModel.async_objects.order_by("value", "name")
        ]

        self.assertEqual(
            len(results),
            3,
            "Should return all objects ordered by 'value' and then 'name'",
        )
        self.assertEqual(results[0].value, 1, "First object value should be 1")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 3, "Third object value should be 3")

    async def test_order_by_invalid_field(self):
        with self.assertRaises(FieldError):
            [
                obj
                async for obj in TestModel.async_objects.order_by(
                    "nonexistent_field"
                )
            ]

    async def test_order_by_is_sliced_error(self):
        with self.assertRaises(TypeError):
            [
                obj
                async for obj in TestModel.async_objects[:1].order_by("value")
            ]


class TestOrderByIsSubsetGroupBy(AsyncioTestCase):
    """Query.orderby_issubset_groupby decides whether ordering may be cleared
    when a query is wrapped in an aggregate subquery. Ordering that is not
    covered by an explicit GROUP BY must be kept, otherwise the inner SELECT
    would reference a column missing from its GROUP BY clause.
    """

    def _grouped(self):
        return TestModel.async_objects.values("value").annotate(
            count=Count("id")
        )

    async def test_extra_order_by_is_never_a_subset(self):
        # Raw SQL from extra(order_by=...) cannot be compared against
        # resolved expressions, so it is conservatively treated as unsafe
        # even though "id" would otherwise be a plain column.
        query = self._grouped().extra(order_by=["id"]).query.clone()
        self.assertEqual(query.extra_order_by, ["id"])

        self.assertFalse(query.orderby_issubset_groupby)

    async def test_extra_order_by_without_group_by(self):
        # extra_order_by short-circuits before the group_by check, so this
        # is False despite there being no aggregation at all.
        query = TestModel.async_objects.extra(order_by=["id"]).query.clone()
        self.assertIsNone(query.group_by)

        self.assertFalse(query.orderby_issubset_groupby)

    async def test_no_group_by_is_a_subset(self):
        # group_by is None -- no aggregation, nothing to violate.
        query = TestModel.async_objects.order_by("name").query.clone()
        self.assertIsNone(query.group_by)

        self.assertTrue(query.orderby_issubset_groupby)

    async def test_implicit_group_by_is_a_subset(self):
        # group_by is True -- generated from the select, so ordering by any
        # selected field is necessarily covered.
        query = TestModel.async_objects.order_by("name").query.clone()
        query.group_by = True

        self.assertTrue(query.orderby_issubset_groupby)

    async def test_empty_order_by_is_a_subset(self):
        # An explicit GROUP BY with no ordering at all: trivially a subset,
        # and short-circuited before the clone().
        query = self._grouped().query.clone()
        self.assertIsInstance(query.group_by, tuple)
        self.assertEqual(query.order_by, ())

        self.assertTrue(query.orderby_issubset_groupby)

    async def test_order_by_field_in_group_by(self):
        # "value" resolves to the same Col that is in the GROUP BY.
        query = self._grouped().order_by("value").query.clone()

        self.assertTrue(query.orderby_issubset_groupby)

    async def test_order_by_field_not_in_group_by(self):
        # "name" is not grouped, so the ordering has to be preserved.
        query = self._grouped().order_by("name").query.clone()

        self.assertFalse(query.orderby_issubset_groupby)

    async def test_order_by_expression_is_not_a_subset(self):
        # An OrderBy expression resolves to OrderBy(...), never to a bare Col,
        # so it can never compare equal to a GROUP BY entry -- even when it
        # wraps a column that *is* grouped.
        query = self._grouped().order_by(F("value").asc()).query.clone()

        self.assertFalse(query.orderby_issubset_groupby)

    async def test_descending_string_order_by_in_group_by(self):
        # The "-" prefix is stripped before resolving, so "-value" compares
        # against the same Col as "value" and is recognised as a subset.
        query = self._grouped().order_by("-value").query.clone()

        self.assertTrue(query.orderby_issubset_groupby)

    async def test_descending_string_order_by_not_in_group_by(self):
        # Stripping the prefix must not make an ungrouped field look grouped:
        # "-name" is still not covered by the GROUP BY.
        query = self._grouped().order_by("-name").query.clone()

        self.assertFalse(query.orderby_issubset_groupby)

    async def test_random_order_by_is_not_a_subset(self):
        # "?" has no column to compare against a GROUP BY entry, so it is
        # conservatively reported as not a subset.
        query = self._grouped().order_by("?").query.clone()

        self.assertFalse(query.orderby_issubset_groupby)

    async def test_random_order_by_after_grouped_field(self):
        # A single "?" is enough to disqualify the whole ordering, even when
        # every other entry is covered by the GROUP BY.
        query = self._grouped().order_by("value", "?").query.clone()

        self.assertFalse(query.orderby_issubset_groupby)


class TestClearOrderingCombinedQueries(AsyncioTestCase):
    """clear_ordering() recurses into combined_queries.

    union() keeps the ordering of its first operand on that part rather than
    on the outer query -- the remaining operands are cleared as they are
    combined -- so clearing only the outer query would leave that ORDER BY in
    place.
    """

    def _union_query(self, left=None, right=None):
        if left is None:
            left = TestModel.async_objects.order_by("name")
        if right is None:
            right = TestModel.async_objects.order_by("value")
        return left.union(right).query.clone()

    async def test_ordering_of_combined_queries_is_cleared(self):
        query = self._union_query()
        # Only the first operand still carries ordering at this point.
        self.assertEqual(
            [q.order_by for q in query.combined_queries],
            [("name",), ()],
        )

        query.clear_ordering(force=True)

        self.assertEqual(
            [q.order_by for q in query.combined_queries], [(), ()]
        )

    async def test_clear_default_is_propagated(self):
        # clear_default reaches the parts, unlike force.
        query = self._union_query()
        for combined_query in query.combined_queries:
            combined_query.default_ordering = True

        query.clear_ordering(force=True, clear_default=True)

        self.assertEqual(
            [q.default_ordering for q in query.combined_queries],
            [False, False],
        )

    async def test_default_ordering_of_parts_is_kept(self):
        # The mirror case: clear_default=False leaves the parts' default
        # ordering alone.
        query = self._union_query()
        for combined_query in query.combined_queries:
            combined_query.default_ordering = True

        query.clear_ordering(force=True, clear_default=False)

        self.assertEqual(
            [q.default_ordering for q in query.combined_queries],
            [True, True],
        )

    async def test_force_is_not_propagated_to_combined_queries(self):
        # The recursion always passes force=False, so a part that cannot be
        # cleared safely keeps its ordering even when the outer call forces
        # it. Slicing is what makes clearing unsafe here.
        query = self._union_query(
            left=TestModel.async_objects.order_by("name")[:2]
        )

        query.clear_ordering(force=True)

        self.assertEqual(
            [q.order_by for q in query.combined_queries], [("name",), ()]
        )

    async def test_plain_query_has_no_combined_queries(self):
        # Without a combinator the loop body never runs.
        query = TestModel.async_objects.order_by("name").query.clone()
        self.assertEqual(query.combined_queries, ())

        query.clear_ordering(force=True)

        self.assertEqual(query.order_by, ())
