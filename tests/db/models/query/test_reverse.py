from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestReverse(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)
        await TestModel.async_object.acreate(name="Test3", value=3)

    async def test_reverse_order(self):
        results = [obj async for obj in TestModel.async_object.order_by("value").reverse()]

        self.assertEqual(len(results), 3, "Should return all objects in reverse order")
        self.assertEqual(results[0].value, 3, "First object value should be 3")
        self.assertEqual(results[1].value, 2, "Second object value should be 2")
        self.assertEqual(results[2].value, 1, "Third object value should be 1")

    async def test_reverse_with_filter(self):
        results = [obj async for obj in TestModel.async_object.filter(value__gte=2).order_by("value").reverse()]

        self.assertEqual(len(results), 2, "Should return filtered objects in reverse order")
        self.assertEqual(results[0].value, 3, "First object value should be 3")
        self.assertEqual(results[1].value, 2, "Second object value should be 2")

    async def test_reverse_invalid_field(self):
        with self.assertRaises(FieldError):
            [obj async for obj in TestModel.async_object.order_by("nonexistent_field").reverse()]

    async def test_reverse_with_slice_error(self):
        with self.assertRaises(TypeError):
            [obj async for obj in TestModel.async_object[:1].reverse()]
