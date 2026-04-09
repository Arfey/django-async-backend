from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestExclude(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)

    async def test_exclude_by_name(self):
        results = [obj async for obj in TestModel.async_object.exclude(name="Test1")]

        self.assertEqual(len(results), 1, "Should exclude 1 object")
        self.assertEqual(results[0].name, "Test2", "Remaining object name should be 'Test2'")

    async def test_exclude_no_results(self):
        results = [obj async for obj in TestModel.async_object.exclude(name="Nonexistent")]

        self.assertEqual(
            len(results),
            2,
            "Should return all objects when none match the exclude condition",
        )

    async def test_exclude_invalid_field(self):
        with self.assertRaises(FieldError):
            [obj async for obj in TestModel.async_object.exclude(nonexistent_field="value")]

    async def test_exclude_multiple_conditions(self):
        results = [obj async for obj in TestModel.async_object.exclude(name="Test1", value=1)]

        self.assertEqual(len(results), 1, "Should exclude 1 object matching both conditions")
        self.assertEqual(results[0].name, "Test2", "Remaining object name should be 'Test2'")
