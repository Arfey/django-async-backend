from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestFilter(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)

    async def test_filter_by_name(self):
        results = [obj async for obj in TestModel.async_object.filter(name="Test1")]

        self.assertEqual(len(results), 1, "Should return 1 object")
        self.assertEqual(results[0].name, "Test1", "Object name should be 'Test1'")

    async def test_filter_no_results(self):
        results = [obj async for obj in TestModel.async_object.filter(name="Nonexistent")]

        self.assertEqual(len(results), 0, "Should return 0 objects when none match")

    async def test_filter_invalid_field(self):
        with self.assertRaises(FieldError):
            [obj async for obj in TestModel.async_object.filter(nonexistent_field="value")]

    async def test_filter_multiple_conditions(self):
        results = [obj async for obj in TestModel.async_object.filter(name="Test1", value=1)]

        self.assertEqual(len(results), 1, "Should return 1 object matching both conditions")
        self.assertEqual(results[0].name, "Test1", "Object name should be 'Test1'")
        self.assertEqual(results[0].value, 1, "Object value should be 1")
