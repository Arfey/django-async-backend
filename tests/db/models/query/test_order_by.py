from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestOrderBy(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=3)
        await TestModel.async_object.acreate(name="Test2", value=1)
        await TestModel.async_object.acreate(name="Test3", value=2)

    async def test_order_by_single_field(self):
        results = [
            obj async for obj in TestModel.async_object.order_by("value")
        ]

        self.assertEqual(
            len(results), 3, "Should return all objects ordered by 'value'"
        )
        self.assertEqual(results[0].value, 1, "First object value should be 1")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 3, "Third object value should be 3")

    async def test_order_by_descending(self):
        results = [
            obj async for obj in TestModel.async_object.order_by("-value")
        ]

        self.assertEqual(
            len(results),
            3,
            "Should return all objects ordered by 'value' descending",
        )
        self.assertEqual(results[0].value, 3, "First object value should be 3")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 1, "Third object value should be 1")

    async def test_order_by_multiple_fields(self):
        results = [
            obj
            async for obj in TestModel.async_object.order_by("value", "name")
        ]

        self.assertEqual(
            len(results),
            3,
            "Should return all objects ordered by 'value' and then 'name'",
        )
        self.assertEqual(results[0].value, 1, "First object value should be 1")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 3, "Third object value should be 3")

    async def test_order_by_invalid_field(self):
        with self.assertRaises(FieldError):
            [
                obj
                async for obj in TestModel.async_object.order_by(
                    "nonexistent_field"
                )
            ]

    async def test_order_by_is_sliced_error(self):
        with self.assertRaises(TypeError):
            [obj async for obj in TestModel.async_object[:1].order_by("value")]
