from unittest import mock

from django.core.exceptions import FieldDoesNotExist
from django.db import (
    DEFAULT_DB_ALIAS,
    connections,
)
from test_app.models import TestModel

from django_async_backend.db.models.query import BaseIterable
from django_async_backend.test import AsyncioTestCase


class TestAinBulk(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj1 = TestModel(name="Test1", value=1)
        await self.obj1.async_save()
        self.obj2 = TestModel(name="Test2", value=2)
        await self.obj2.async_save()

    async def test_ain_bulk_with_ids(self):
        results = await TestModel.async_objects.ain_bulk(
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
        results = await TestModel.async_objects.ain_bulk([])
        self.assertEqual(len(results), 0, "Should return an empty dictionary")

    async def test_ain_bulk_all(self):
        results = await TestModel.async_objects.ain_bulk()

        self.assertEqual(
            results,
            {self.obj1.id: self.obj1, self.obj2.id: self.obj2},
        )

    async def test_ain_bulk_with_field_name(self):
        results = await TestModel.async_objects.ain_bulk(
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
            await TestModel.async_objects.ain_bulk(
                ["Test1"], field_name="nonexistent_field"
            )

    async def test_ain_bulk_invalid_field(self):
        with self.assertRaises(ValueError):
            await TestModel.async_objects.ain_bulk(
                ["Test1"], field_name="value"
            )

    async def test_ain_bulk_with_slicing(self):
        sliced_queryset = TestModel.async_objects.all()[:1]
        with self.assertRaises(TypeError):
            await sliced_queryset.ain_bulk([self.obj1.id])

    async def test_ain_bulk_batched(self):
        # With a batch size below the number of ids, ain_bulk() fetches the
        # objects one batch at a time and merges the results.
        connection = connections[DEFAULT_DB_ALIAS]

        with mock.patch.object(
            connection.ops, "bulk_batch_size", return_value=1
        ):
            results = await TestModel.async_objects.ain_bulk(
                [self.obj1.id, self.obj2.id]
            )

        self.assertEqual(
            results, {self.obj1.id: self.obj1, self.obj2.id: self.obj2}
        )

    async def test_ain_bulk_batched_values_list(self):
        # The batched path must query the reshaped queryset, so that
        # values_list() still applies to every batch.
        connection = connections[DEFAULT_DB_ALIAS]

        with mock.patch.object(
            connection.ops, "bulk_batch_size", return_value=1
        ):
            results = await TestModel.async_objects.values_list(
                "name"
            ).ain_bulk([self.obj1.id, self.obj2.id])

        self.assertEqual(
            results,
            {self.obj1.id: ("Test1",), self.obj2.id: ("Test2",)},
        )

    async def test_ain_bulk_unsupported_iterable_class(self):
        class CustomIterable(BaseIterable):
            pass

        queryset = TestModel.async_objects.all()
        queryset._iterable_class = CustomIterable

        with self.assertRaises(TypeError) as ctx:
            await queryset.ain_bulk([self.obj1.id])

        self.assertEqual(
            str(ctx.exception),
            "in_bulk() cannot be used with CustomIterable.",
        )

    # --- values() -----------------------------------------------------

    async def test_ain_bulk_values_all_fields(self):
        results = await TestModel.async_objects.values().ain_bulk(
            [self.obj1.id]
        )

        self.assertEqual(
            results,
            {
                self.obj1.id: {
                    "id": self.obj1.id,
                    "name": "Test1",
                    "value": 1,
                    "relative_id": None,
                }
            },
        )

    async def test_ain_bulk_values_fields(self):
        # field_name ("pk") is not in values("name"), so it is added to the
        # query to build the keys and stripped back out of the values.
        results = await TestModel.async_objects.values("name").ain_bulk(
            [self.obj1.id]
        )

        self.assertEqual(results, {self.obj1.id: {"name": "Test1"}})

    async def test_ain_bulk_values_fields_including_pk(self):
        results = await TestModel.async_objects.values("pk", "name").ain_bulk(
            [self.obj1.id]
        )

        self.assertEqual(
            results,
            {self.obj1.id: {"pk": self.obj1.id, "name": "Test1"}},
        )

    async def test_ain_bulk_values_alternative_field_name(self):
        results = await TestModel.async_objects.values("value").ain_bulk(
            ["Test1"], field_name="name"
        )

        self.assertEqual(results, {"Test1": {"value": 1}})

    # --- values_list() ------------------------------------------------

    async def test_ain_bulk_values_list_empty(self):
        results = await TestModel.async_objects.values_list().ain_bulk([])
        self.assertEqual(results, {})

    async def test_ain_bulk_values_list_all(self):
        results = await TestModel.async_objects.values_list().ain_bulk()

        self.assertEqual(
            results,
            {
                self.obj1.id: (self.obj1.id, "Test1", 1, None),
                self.obj2.id: (self.obj2.id, "Test2", 2, None),
            },
        )

    async def test_ain_bulk_values_list_fields(self):
        # field_name is missing from values_select, so it is prepended to the
        # query and then sliced off the returned rows.
        results = await TestModel.async_objects.values_list("name").ain_bulk(
            [self.obj1.id, self.obj2.id]
        )

        self.assertEqual(
            results,
            {self.obj1.id: ("Test1",), self.obj2.id: ("Test2",)},
        )

    async def test_ain_bulk_values_list_fields_including_pk(self):
        results = await TestModel.async_objects.values_list(
            "pk", "name"
        ).ain_bulk([self.obj1.id, self.obj2.id])

        self.assertEqual(
            results,
            {
                self.obj1.id: (self.obj1.id, "Test1"),
                self.obj2.id: (self.obj2.id, "Test2"),
            },
        )

    async def test_ain_bulk_values_list_named(self):
        results = await TestModel.async_objects.values_list(
            named=True
        ).ain_bulk([self.obj1.id, self.obj2.id])

        self.assertEqual(len(results), 2)
        row = results[self.obj1.id]
        # "pk" is missing from values_select, so it is prepended to the row.
        self.assertEqual(
            row._fields, ("pk", "id", "name", "value", "relative_id")
        )
        self.assertEqual(row.pk, self.obj1.id)
        self.assertEqual(row.name, "Test1")

    async def test_ain_bulk_values_list_named_fields(self):
        results = await TestModel.async_objects.values_list(
            "pk", "name", named=True
        ).ain_bulk([self.obj1.id, self.obj2.id])

        self.assertEqual(len(results), 2)
        row = results[self.obj1.id]
        self.assertEqual(row._fields, ("pk", "name"))
        self.assertEqual(row.pk, self.obj1.id)
        self.assertEqual(row.name, "Test1")

    async def test_ain_bulk_values_list_named_alternative_field(self):
        # field_name is added to the namedtuple, so it leads _fields.
        results = await TestModel.async_objects.values_list(
            "value", named=True
        ).ain_bulk(["Test1"], field_name="name")

        row = results["Test1"]
        self.assertEqual(row._fields, ("name", "value"))
        self.assertEqual(row.name, "Test1")
        self.assertEqual(row.value, 1)

    # --- values_list(flat=True) ---------------------------------------

    async def test_ain_bulk_values_list_flat_field_pk(self):
        # values_select is exactly field_name, so each key maps to itself.
        results = await TestModel.async_objects.values_list(
            "pk", flat=True
        ).ain_bulk([self.obj1.id, self.obj2.id])

        self.assertEqual(
            results,
            {self.obj1.id: self.obj1.id, self.obj2.id: self.obj2.id},
        )

    async def test_ain_bulk_values_list_flat_field(self):
        # A flat list of a different field is turned back into a non-flat
        # values_list() so the key and the value can both be read.
        results = await TestModel.async_objects.values_list(
            "name", flat=True
        ).ain_bulk([self.obj1.id, self.obj2.id])

        self.assertEqual(
            results,
            {self.obj1.id: "Test1", self.obj2.id: "Test2"},
        )

    async def test_ain_bulk_values_list_flat_alternative_field_name(self):
        results = await TestModel.async_objects.values_list(
            "value", flat=True
        ).ain_bulk(["Test1", "Test2"], field_name="name")

        self.assertEqual(results, {"Test1": 1, "Test2": 2})
