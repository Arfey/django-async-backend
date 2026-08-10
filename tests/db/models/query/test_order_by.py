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

    async def test_descending_string_order_by_raises(self):
        # Known upstream defect: the "-" prefix is passed to F() verbatim
        # instead of being stripped, so a descending string ordering blows up
        # rather than reporting whether it is a subset. Matches the behaviour
        # of the identical property in Django itself.
        query = self._grouped().order_by("-value").query.clone()

        with self.assertRaises(FieldError) as ctx:
            query.orderby_issubset_groupby

        self.assertIn("Cannot resolve keyword '-value'", str(ctx.exception))
