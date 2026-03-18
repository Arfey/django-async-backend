from django.db.models import Count
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAnnotate(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)
        await TestModel.async_object.acreate(name="Test3", value=3)

    async def test_annotate_count(self):
        results = [
            obj
            async for obj in TestModel.async_object.values("value")
            .annotate(count=Count("value"))
            .order_by("value")
        ]

        self.assertEqual(
            len(results), 3, "Should return 3 distinct value groups"
        )
        self.assertEqual(
            results[0]["value"], 1, "First group value should be 1"
        )
        self.assertEqual(
            results[0]["count"], 1, "First group count should be 1"
        )
        self.assertEqual(
            results[1]["value"], 2, "Second group value should be 2"
        )
        self.assertEqual(
            results[1]["count"], 1, "Second group count should be 1"
        )
        self.assertEqual(
            results[2]["value"], 3, "Third group value should be 3"
        )
        self.assertEqual(
            results[2]["count"], 1, "Third group count should be 1"
        )

    async def test_annotate_with_filter(self):
        results = [
            obj
            async for obj in TestModel.async_object.filter(value=1)
            .values("value")
            .annotate(count=Count("value"))
        ]

        self.assertEqual(
            len(results), 1, "Should return 1 group for filtered value"
        )
        self.assertEqual(results[0]["value"], 1, "Group value should be 1")
        self.assertEqual(results[0]["count"], 1, "Group count should be 1")

    async def test_annotate_no_results(self):
        results = [
            obj
            async for obj in TestModel.async_object.filter(value=99)
            .values("value")
            .annotate(count=Count("value"))
        ]

        self.assertEqual(
            len(results),
            0,
            "Should return no groups when no objects match the filter",
        )
