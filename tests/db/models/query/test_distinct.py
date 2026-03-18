from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestDistinct(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)
        await TestModel.async_object.acreate(name="Test3", value=1)

    async def test_distinct_single_field(self):
        results = [
            obj async for obj in TestModel.async_object.distinct("value")
        ]

        self.assertEqual(
            len(results), 2, "Should return distinct objects based on 'value'"
        )
        self.assertEqual(
            results[0].value, 1, "First distinct value should be 1"
        )
        self.assertEqual(
            results[1].value, 2, "Second distinct value should be 2"
        )

    async def test_distinct_multiple_fields(self):
        results = [
            obj
            async for obj in TestModel.async_object.distinct(
                "value", "name"
            ).order_by("name")
        ]

        self.assertEqual(
            len(results),
            3,
            "Should return distinct objects based on 'value' and 'name'",
        )
        self.assertEqual(
            results[0].name,
            "Test1",
            "First distinct object name should be 'Test1'",
        )
        self.assertEqual(
            results[1].name,
            "Test2",
            "Second distinct object name should be 'Test2'",
        )
        self.assertEqual(
            results[2].name,
            "Test3",
            "Third distinct object name should be 'Test3'",
        )

    async def test_distinct_invalid_field(self):
        with self.assertRaises(FieldError):
            [
                obj
                async for obj in TestModel.async_object.distinct(
                    "nonexistent_field"
                )
            ]

    async def test_distinct_with_slice_error(self):
        with self.assertRaises(TypeError):
            [obj async for obj in TestModel.async_object[:1].distinct("value")]
