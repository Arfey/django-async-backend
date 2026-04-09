from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestUnion(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=1)
        await TestModel.async_object.acreate(name="Item2", value=2)
        await TestModel.async_object.acreate(name="Item3", value=3)

    async def test_union_with_results(self):
        queryset1 = TestModel.async_object.filter(value__lte=2)
        queryset2 = TestModel.async_object.filter(value__gte=2)
        union_queryset = queryset1.union(queryset2)

        results = [item async for item in union_queryset]
        self.assertEqual(len(results), 3, "Union should return all unique items")
        self.assertTrue(
            any(item.name == "Item1" for item in results),
            "Union should include 'Item1'",
        )
        self.assertTrue(
            any(item.name == "Item2" for item in results),
            "Union should include 'Item2'",
        )
        self.assertTrue(
            any(item.name == "Item3" for item in results),
            "Union should include 'Item3'",
        )

    async def test_union_no_results(self):
        queryset1 = TestModel.async_object.filter(value__lte=0)
        queryset2 = TestModel.async_object.filter(value__gte=4)
        union_queryset = queryset1.union(queryset2)

        results = [item async for item in union_queryset]
        self.assertEqual(
            len(results),
            0,
            "Union should return no items if both querysets are empty",
        )

    async def test_union_with_empty_queryset(self):
        queryset1 = TestModel.async_object.filter(value__lte=1)
        queryset2 = TestModel.async_object.none()
        union_queryset = queryset1.union(queryset2)

        results = [item async for item in union_queryset]
        self.assertEqual(
            len(results),
            1,
            "Union with an empty queryset should return the first queryset",
        )
        self.assertEqual(results[0].name, "Item1", "Union should include only 'Item1'")

        queryset1 = TestModel.async_object.none()
        queryset2 = TestModel.async_object.none()
        union_queryset = queryset1.union(queryset2)

        results = [item async for item in union_queryset]
        self.assertEqual(len(results), 0)

    async def test_union_with_multiple_querysets(self):
        queryset1 = TestModel.async_object.filter(value__lte=1)
        queryset2 = TestModel.async_object.filter(value=2)
        queryset3 = TestModel.async_object.filter(value=3)
        union_queryset = queryset1.union(queryset2, queryset3)

        results = [item async for item in union_queryset]
        self.assertEqual(len(results), 3, "Union should include items from all querysets")
        self.assertTrue(
            any(item.name == "Item1" for item in results),
            "Union should include 'Item1'",
        )
        self.assertTrue(
            any(item.name == "Item2" for item in results),
            "Union should include 'Item2'",
        )
        self.assertTrue(
            any(item.name == "Item3" for item in results),
            "Union should include 'Item3'",
        )
