from django.db import NotSupportedError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAGet(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1").async_save()
        await TestModel(name="Item2").async_save()
        await TestModel(name="Item3").async_save()

    async def test_aget_with_results(self):
        item = await TestModel.async_objects.aget(name="Item1")
        self.assertIsNotNone(item, "aget should return the matching object")
        self.assertEqual(
            item.name, "Item1", "The object should have the name 'Item1'"
        )

    async def test_aget_no_results(self):
        with self.assertRaises(TestModel.DoesNotExist):
            await TestModel.async_objects.aget(name="NonExistent")

    async def test_aget_multiple_results(self):
        await TestModel(name="Item4", value=1).async_save()
        await TestModel(name="Item5", value=1).async_save()
        with self.assertRaises(TestModel.MultipleObjectsReturned):
            await TestModel.async_objects.aget(value=1)

    async def test_aget_with_filters(self):
        item = await TestModel.async_objects.filter(name="Item1").aget()
        self.assertIsNotNone(item, "aget should return the matching object")
        self.assertEqual(
            item.name, "Item1", "The object should have the name 'Item1'"
        )

    async def test_union_combinator(self):
        queryset1 = TestModel.async_objects.filter(name="Item1")
        queryset2 = TestModel.async_objects.filter(name="Item2")
        combined_queryset = queryset1.union(queryset2)

        with self.assertRaises(NotSupportedError):
            await combined_queryset.aget(id=1)
