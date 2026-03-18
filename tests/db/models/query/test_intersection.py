from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestIntersection(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=1)
        await TestModel.async_object.acreate(name="Item2", value=2)
        await TestModel.async_object.acreate(name="Item3", value=3)

    async def test_intersection_with_results(self):
        queryset1 = TestModel.async_object.filter(value__lte=2)
        queryset2 = TestModel.async_object.filter(value__gte=1)
        intersection_queryset = queryset1.intersection(queryset2)

        results = [item async for item in intersection_queryset]
        self.assertEqual(
            len(results), 2, "Intersection should return common items"
        )
        self.assertTrue(
            any(item.name == "Item1" for item in results),
            "Intersection should include 'Item1'",
        )
        self.assertTrue(
            any(item.name == "Item2" for item in results),
            "Intersection should include 'Item2'",
        )

    async def test_intersection_no_results(self):
        queryset1 = TestModel.async_object.filter(value__lte=1)
        queryset2 = TestModel.async_object.filter(value__gte=3)
        intersection_queryset = queryset1.intersection(queryset2)

        results = [item async for item in intersection_queryset]
        self.assertEqual(
            len(results),
            0,
            "Intersection should return no items if there are no common "
            "elements",
        )

    async def test_intersection_with_empty_queryset(self):
        queryset1 = TestModel.async_object.filter(value__lte=1)
        queryset2 = TestModel.async_object.none()
        intersection_queryset = queryset1.intersection(queryset2)

        results = [item async for item in intersection_queryset]
        self.assertEqual(
            len(results),
            0,
            "Intersection with an empty queryset should return no items",
        )

    async def test_intersection_with_multiple_querysets(self):
        queryset1 = TestModel.async_object.filter(value__lte=3)
        queryset2 = TestModel.async_object.filter(value__gte=2)
        queryset3 = TestModel.async_object.filter(value=3)
        intersection_queryset = queryset1.intersection(queryset2, queryset3)

        results = [item async for item in intersection_queryset]
        self.assertEqual(
            len(results),
            1,
            "Intersection should return items common to all querysets",
        )
        self.assertEqual(
            results[0].name,
            "Item3",
            "Intersection should include only 'Item3'",
        )
