from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestDifference(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=1)
        await TestModel.async_object.acreate(name="Item2", value=2)
        await TestModel.async_object.acreate(name="Item3", value=3)

    async def test_difference_with_results(self):
        queryset1 = TestModel.async_object.filter(value__lte=2)
        queryset2 = TestModel.async_object.filter(value__gte=2)
        difference_queryset = queryset1.difference(queryset2)

        results = [item async for item in difference_queryset]
        self.assertEqual(len(results), 1, "Difference should return unique items")
        self.assertEqual(results[0].name, "Item1", "Difference should include only 'Item1'")

    async def test_difference_no_results(self):
        queryset1 = TestModel.async_object.filter(value__lte=1)
        queryset2 = TestModel.async_object.filter(value__gte=2)
        difference_queryset = queryset1.difference(queryset2)

        results = [item async for item in difference_queryset]
        self.assertEqual(
            len(results),
            1,
            "Difference should return items from the first queryset",
        )
        self.assertEqual(results[0].name, "Item1", "Difference should include only 'Item1'")

    async def test_difference_with_empty_queryset(self):
        queryset1 = TestModel.async_object.filter(value__lte=1)
        queryset2 = TestModel.async_object.none()
        difference_queryset = queryset1.difference(queryset2)

        results = [item async for item in difference_queryset]
        self.assertEqual(
            len(results),
            1,
            "Difference with an empty queryset should return the first queryset",
        )
        self.assertEqual(results[0].name, "Item1", "Difference should include only 'Item1'")

    async def test_difference_with_multiple_querysets(self):
        queryset1 = TestModel.async_object.filter(value__lte=3)
        queryset2 = TestModel.async_object.filter(value=2)
        queryset3 = TestModel.async_object.filter(value=3)
        difference_queryset = queryset1.difference(queryset2, queryset3)

        results = [item async for item in difference_queryset]
        self.assertEqual(
            len(results),
            1,
            "Difference should exclude items from all other querysets",
        )
        self.assertEqual(results[0].name, "Item1", "Difference should include only 'Item1'")
