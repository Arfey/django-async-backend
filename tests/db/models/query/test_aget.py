from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAGet(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1")
        await TestModel.async_object.acreate(name="Item2")
        await TestModel.async_object.acreate(name="Item3")

    async def test_aget_with_results(self):
        item = await TestModel.async_object.aget(name="Item1")
        self.assertIsNotNone(item, "aget should return the matching object")
        self.assertEqual(
            item.name, "Item1", "The object should have the name 'Item1'"
        )

    async def test_aget_no_results(self):
        with self.assertRaises(TestModel.DoesNotExist):
            await TestModel.async_object.aget(name="NonExistent")

    async def test_aget_multiple_results(self):
        await TestModel.async_object.acreate(name="Item4", value=1)
        await TestModel.async_object.acreate(name="Item5", value=1)
        with self.assertRaises(TestModel.MultipleObjectsReturned):
            await TestModel.async_object.aget(value=1)

    async def test_aget_with_filters(self):
        item = await TestModel.async_object.filter(name="Item1").aget()
        self.assertIsNotNone(item, "aget should return the matching object")
        self.assertEqual(
            item.name, "Item1", "The object should have the name 'Item1'"
        )
