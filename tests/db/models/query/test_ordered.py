from django.db.models import Count
from test_app.models import (
    DefaultOrderingModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestQuerySetOrdered(AsyncioTestCase):
    async def test_unordered_queryset(self):
        qs = TestModel.async_objects.all()

        self.assertFalse(qs.ordered)
        self.assertIsNone(qs._result_cache)

    async def test_order_by(self):
        qs = TestModel.async_objects.order_by("name")

        self.assertTrue(qs.ordered)

    async def test_order_by_cleared(self):
        qs = TestModel.async_objects.order_by("name").order_by()

        self.assertFalse(qs.ordered)

    async def test_extra_order_by(self):
        qs = TestModel.async_objects.extra(order_by=["name"])

        self.assertTrue(qs.ordered)

    async def test_none_is_ordered(self):
        """An empty queryset counts as ordered."""
        qs = TestModel.async_objects.none()

        self.assertTrue(qs.ordered)

    async def test_default_ordering_from_meta(self):
        qs = DefaultOrderingModel.async_objects.all()

        self.assertTrue(qs.ordered)

    async def test_default_ordering_ignored_for_group_by(self):
        """A default ordering doesn't affect GROUP BY queries."""
        qs = (
            DefaultOrderingModel.async_objects.annotate(
                count=Count("pk"),
            )
            .values("name")
            .annotate(total=Count("value"))
        )

        self.assertFalse(qs.ordered)

    async def test_ordered_does_not_query(self):
        await TestModel.async_objects.acreate(name="Item1")
        qs = TestModel.async_objects.order_by("name")

        self.assertTrue(qs.ordered)
        self.assertIsNone(qs._result_cache)
