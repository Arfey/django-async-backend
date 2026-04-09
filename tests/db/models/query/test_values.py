from django.core.exceptions import FieldError
from django.db.models import Count
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestValues(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)

    async def test_values(self):
        results = [obj async for obj in TestModel.async_object.values("name", "value")]

        self.assertEqual(len(results), 2, "Should return 2 value dictionaries")
        self.assertEqual(results[0]["name"], "Test1", "First object name should be 'Test1'")
        self.assertEqual(results[0]["value"], 1, "First object value should be 1")
        self.assertEqual(results[1]["name"], "Test2", "Second object name should be 'Test2'")
        self.assertEqual(results[1]["value"], 2, "Second object value should be 2")

    async def test_values_no_objects(self):
        results = [obj async for obj in TestModel.async_object.filter(id=10).values("name", "value")]
        self.assertEqual(
            len(results),
            0,
            "Should return 0 value dictionaries when none exist",
        )

    async def test_values_with_filter(self):
        results = [obj async for obj in TestModel.async_object.filter(name="Test1").values("name", "value")]

        self.assertEqual(len(results), 1, "Should return 1 value dictionary")
        self.assertEqual(results[0]["name"], "Test1", "First object name should be 'Test1'")
        self.assertEqual(results[0]["value"], 1, "First object value should be 1")

    async def test_values_invalid_field(self):
        with self.assertRaises(FieldError):
            [obj async for obj in TestModel.async_object.values("nonexistent_field")]

    async def test_values_annotate(self):
        results = [
            obj
            async for obj in TestModel.async_object.annotate(count=Count("name"))
            .order_by("name")
            .values("name", "count")
        ]

        self.assertEqual(
            results,
            [{"name": "Test1", "count": 1}, {"name": "Test2", "count": 1}],
            "Results should be ordered by 'name' and include correct counts",
        )

    async def test_values_no_fields(self):
        results = [obj async for obj in TestModel.async_object.values()]

        self.assertEqual(len(results), 2, "Should return 2 value dictionaries")
        self.assertIn("id", results[0], "Result should include 'id' field by default")
        self.assertIn("name", results[0], "Result should include 'name' field by default")
        self.assertIn(
            "value",
            results[0],
            "Result should include 'value' field by default",
        )
        self.assertEqual(results[0]["name"], "Test1", "First object name should be 'Test1'")
        self.assertEqual(results[1]["name"], "Test2", "Second object name should be 'Test2'")
