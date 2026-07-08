from django.db.models import Count
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestALast(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1").async_save()
        await TestModel(name="Item2").async_save()
        await TestModel(name="Item3").async_save()

    async def test_alast_with_results(self):
        last_item = await TestModel.async_objects.alast()
        self.assertIsNotNone(last_item, "alast should return the last object")
        self.assertEqual(
            last_item.name, "Item3", "The last object should be 'Item3'"
        )

    async def test_alast_no_results(self):
        last_item = await TestModel.async_objects.filter(name="Item4").alast()
        self.assertIsNone(
            last_item, "alast should return None when no objects exist"
        )

    async def test_alast_with_ordering(self):
        last_item = await TestModel.async_objects.order_by("name").alast()
        self.assertIsNotNone(last_item, "alast should return the last object")
        self.assertEqual(
            last_item.name,
            "Item3",
            "The last object should be 'Item3' with ascending order",
        )

    async def test_alast_check_ordering_error(self):
        with self.assertRaises(TypeError):
            await TestModel.async_objects.values("name").annotate(
                count=Count("name")
            ).alast()
