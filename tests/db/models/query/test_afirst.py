from django.db.models import Count
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAFirst(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1")
        await TestModel.async_object.acreate(name="Item2")
        await TestModel.async_object.acreate(name="Item3")

    async def test_afirst_with_results(self):
        first_item = await TestModel.async_object.afirst()
        self.assertIsNotNone(
            first_item, "afirst should return the first object"
        )
        self.assertEqual(
            first_item.name, "Item1", "The first object should be 'Item1'"
        )

    async def test_afirst_no_results(self):
        first_item = await TestModel.async_object.filter(name="Item4").afirst()
        self.assertIsNone(
            first_item, "afirst should return None when no objects exist"
        )

    async def test_afirst_with_ordering(self):
        first_item = await TestModel.async_object.order_by("-name").afirst()
        self.assertIsNotNone(
            first_item, "afirst should return the first object"
        )
        self.assertEqual(
            first_item.name,
            "Item3",
            "The first object should be 'Item3' with descending order",
        )

    async def test_afirst_check_ordering_error(self):
        with self.assertRaises(TypeError):
            await TestModel.async_object.values("name").annotate(
                count=Count("name")
            ).afirst()
