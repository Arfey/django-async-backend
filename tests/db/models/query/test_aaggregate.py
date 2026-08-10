from django.db.models import (
    Count,
    Sum,
)
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAAggregate(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()
        await TestModel(name="Item3", value=2).async_save()

    async def test_aggregate_without_grouping(self):
        result = await TestModel.async_objects.aaggregate(total=Sum("value"))

        self.assertEqual(result, {"total": 5})

    async def test_aggregate_with_filter(self):
        result = await TestModel.async_objects.filter(value=2).aaggregate(
            total=Sum("value"), count=Count("id")
        )

        self.assertEqual(result, {"total": 4, "count": 2})


class TestClearOrderingGroupBy(AsyncioTestCase):

    def _grouped_query(self, order_by):
        qs = (
            TestModel.async_objects.values("value")
            .annotate(count=Count("id"))
            .order_by(order_by)
        )
        return qs.query.clone()

    async def test_ordering_not_in_group_by_is_preserved(self):
        query = self._grouped_query("name")
        self.assertIsInstance(query.group_by, tuple)
        ordering = query.order_by

        query.clear_ordering(force=False)

        self.assertEqual(query.order_by, ordering)

    async def test_ordering_on_grouped_field_is_also_preserved(self):
        query = self._grouped_query("value")
        self.assertIsInstance(query.group_by, tuple)
        ordering = query.order_by

        query.clear_ordering(force=False)

        self.assertEqual(query.order_by, ordering)

    async def test_ordering_is_cleared_without_group_by(self):
        query = TestModel.async_objects.order_by("name").query.clone()
        self.assertNotIsInstance(query.group_by, tuple)

        query.clear_ordering(force=False)

        self.assertEqual(query.order_by, ())

    async def test_force_clears_ordering_not_in_group_by(self):
        query = self._grouped_query("name")

        query.clear_ordering(force=True)

        self.assertEqual(query.order_by, ())
