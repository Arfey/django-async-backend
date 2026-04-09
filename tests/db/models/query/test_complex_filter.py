from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestComplexFilter(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)
        await TestModel.async_object.acreate(name="Test3", value=3)

    async def test_complex_filter_by_range(self):
        results = [obj async for obj in TestModel.async_object.complex_filter({"value__gte": 2, "value__lte": 3})]

        self.assertEqual(len(results), 2, "Should return 2 objects within the range")
        self.assertEqual(results[0].value, 2, "First object value should be 2")
        self.assertEqual(results[1].value, 3, "Second object value should be 3")

    async def test_complex_filter_with_exclusion(self):
        results = [obj async for obj in TestModel.async_object.complex_filter({"value__gte": 1}).exclude(name="Test1")]

        self.assertEqual(len(results), 2, "Should return 2 objects excluding 'Test1'")
        self.assertNotEqual(results[0].name, "Test1", "First object should not be 'Test1'")
        self.assertNotEqual(results[1].name, "Test1", "Second object should not be 'Test1'")

    async def test_complex_filter_invalid_field(self):
        with self.assertRaises(FieldError):
            [obj async for obj in TestModel.async_object.complex_filter({"nonexistent_field__gte": 1})]

    async def test_complex_filter_multiple_conditions(self):
        results = [
            obj
            async for obj in TestModel.async_object.complex_filter({"name__startswith": "Test", "value__in": [1, 3]})
        ]

        self.assertEqual(len(results), 2, "Should return 2 objects matching the conditions")
        self.assertEqual(results[0].name, "Test1", "First object name should be 'Test1'")
        self.assertEqual(results[1].name, "Test3", "Second object name should be 'Test3'")
