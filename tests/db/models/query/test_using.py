from django.utils.connection import ConnectionDoesNotExist
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestUsing(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()

    async def test_using_default_database(self):
        results = [
            obj async for obj in TestModel.async_objects.using("default")
        ]

        self.assertEqual(
            len(results),
            2,
            "Should return all objects from the default database",
        )
        self.assertEqual(
            results[0].name, "Test1", "First object name should be 'Test1'"
        )
        self.assertEqual(
            results[1].name, "Test2", "Second object name should be 'Test2'"
        )

    async def test_using_invalid_database(self):
        with self.assertRaises(ConnectionDoesNotExist):
            [obj async for obj in TestModel.async_objects.using("invalid_db")]

    async def test_using_with_filter(self):
        results = [
            obj
            async for obj in TestModel.async_objects.using("default").filter(
                name="Test1"
            )
        ]

        self.assertEqual(
            len(results),
            1,
            "Should return filtered object from the default database",
        )
        self.assertEqual(
            results[0].name, "Test1", "Filtered object name should be 'Test1'"
        )
