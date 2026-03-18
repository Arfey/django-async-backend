from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestValuesList(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=1)
        await TestModel.async_object.acreate(name="Test2", value=2)

    async def test_values_list(self):
        results = [
            obj
            async for obj in TestModel.async_object.values_list(
                "name", "value"
            )
        ]

        self.assertEqual(len(results), 2, "Should return 2 tuples")
        self.assertEqual(
            results[0], ("Test1", 1), "First tuple should be ('Test1', 1)"
        )
        self.assertEqual(
            results[1], ("Test2", 2), "Second tuple should be ('Test2', 2)"
        )

    async def test_values_list_no_objects(self):
        results = [
            obj
            async for obj in TestModel.async_object.filter(id=10).values_list(
                "name", "value"
            )
        ]
        self.assertEqual(
            len(results), 0, "Should return 0 tuples when none exist"
        )

    async def test_values_list_with_filter(self):
        results = [
            obj
            async for obj in TestModel.async_object.filter(
                name="Test1"
            ).values_list("name", "value")
        ]

        self.assertEqual(len(results), 1, "Should return 1 tuple")
        self.assertEqual(
            results[0], ("Test1", 1), "First tuple should be ('Test1', 1)"
        )

    async def test_values_list_invalid_field(self):
        with self.assertRaises(FieldError):
            [
                obj
                async for obj in TestModel.async_object.values_list(
                    "nonexistent_field"
                )
            ]

    async def test_values_list_flat(self):
        results = [
            obj
            async for obj in TestModel.async_object.values_list(
                "name", flat=True
            )
        ]

        self.assertEqual(
            len(results), 2, "Should return 2 values in flat mode"
        )
        self.assertEqual(results[0], "Test1", "First value should be 'Test1'")
        self.assertEqual(results[1], "Test2", "Second value should be 'Test2'")

    async def test_values_list_flat_all_fields(self):
        results = [
            obj async for obj in TestModel.async_object.values_list(flat=True)
        ]

        self.assertEqual(
            len(results), 2, "Should return 2 values in flat mode"
        )

    async def test_values_list_flat_multiple_fields(self):
        with self.assertRaises(TypeError):
            [
                obj
                async for obj in TestModel.async_object.values_list(
                    "name", "value", flat=True
                )
            ]

    async def test_values_list_named(self):
        results = [
            obj
            async for obj in TestModel.async_object.values_list(
                "name", "value", named=True
            )
        ]

        self.assertEqual(len(results), 2, "Should return 2 named tuples")
        self.assertEqual(
            results[0].name,
            "Test1",
            "First named tuple name should be 'Test1'",
        )
        self.assertEqual(
            results[0].value, 1, "First named tuple value should be 1"
        )
        self.assertEqual(
            results[1].name,
            "Test2",
            "Second named tuple name should be 'Test2'",
        )
        self.assertEqual(
            results[1].value, 2, "Second named tuple value should be 2"
        )

    async def test_values_list_named_all_fields(self):
        results = [
            obj async for obj in TestModel.async_object.values_list(named=True)
        ]

        self.assertEqual(len(results), 2, "Should return 2 named tuples")
        self.assertEqual(
            results[0].name,
            "Test1",
            "First named tuple name should be 'Test1'",
        )
        self.assertEqual(
            results[0].value, 1, "First named tuple value should be 1"
        )
        self.assertEqual(
            results[1].name,
            "Test2",
            "Second named tuple name should be 'Test2'",
        )
        self.assertEqual(
            results[1].value, 2, "Second named tuple value should be 2"
        )

    async def test_values_list_flat_and_named(self):
        with self.assertRaises(TypeError):
            [
                obj
                async for obj in TestModel.async_object.values_list(
                    "name", "value", flat=True, named=True
                )
            ]
