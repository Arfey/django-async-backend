from django.core.exceptions import FieldDoesNotExist
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAinBulk(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj1 = await TestModel.async_object.acreate(name="Test1", value=1)
        self.obj2 = await TestModel.async_object.acreate(name="Test2", value=2)

    async def test_ain_bulk_with_ids(self):
        results = await TestModel.async_object.ain_bulk(
            [self.obj1.id, self.obj2.id]
        )

        self.assertEqual(len(results), 2, "Should return 2 objects")
        self.assertIn(self.obj1.id, results, "Results should include obj1")
        self.assertIn(self.obj2.id, results, "Results should include obj2")
        self.assertEqual(
            results[self.obj1.id].name, "Test1", "Obj1 name should be 'Test1'"
        )
        self.assertEqual(
            results[self.obj2.id].name, "Test2", "Obj2 name should be 'Test2'"
        )

    async def test_ain_bulk_no_ids(self):
        results = await TestModel.async_object.ain_bulk([])
        self.assertEqual(len(results), 0, "Should return an empty dictionary")

    async def test_ain_bulk_with_field_name(self):
        results = await TestModel.async_object.ain_bulk(
            ["Test1", "Test2"], field_name="name"
        )

        self.assertEqual(len(results), 2, "Should return 2 objects")
        self.assertIn("Test1", results, "Results should include 'Test1'")
        self.assertIn("Test2", results, "Results should include 'Test2'")
        self.assertEqual(
            results["Test1"].value, 1, "Value for 'Test1' should be 1"
        )
        self.assertEqual(
            results["Test2"].value, 2, "Value for 'Test2' should be 2"
        )

    async def test_ain_bulk_nonexistent_field(self):
        with self.assertRaises(FieldDoesNotExist):
            await TestModel.async_object.ain_bulk(
                ["Test1"], field_name="nonexistent_field"
            )

    async def test_ain_bulk_invalid_field(self):
        with self.assertRaises(ValueError):
            await TestModel.async_object.ain_bulk(
                ["Test1"], field_name="value"
            )

    async def test_ain_bulk_with_slicing(self):
        sliced_queryset = TestModel.async_object.all()[:1]
        with self.assertRaises(TypeError):
            await sliced_queryset.ain_bulk([self.obj1.id])

    async def test_ain_bulk_with_non_model_iterable(self):
        queryset = TestModel.async_object.values("name")
        with self.assertRaises(TypeError):
            await queryset.ain_bulk([self.obj1.id])
