import re

from django.core.exceptions import FieldError
from django.db.models import (
    Count,
    Value,
)
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestValues(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()

    async def test_values(self):
        results = [
            obj
            async for obj in TestModel.async_objects.values("name", "value")
        ]

        self.assertEqual(len(results), 2, "Should return 2 value dictionaries")
        self.assertEqual(
            results[0]["name"], "Test1", "First object name should be 'Test1'"
        )
        self.assertEqual(
            results[0]["value"], 1, "First object value should be 1"
        )
        self.assertEqual(
            results[1]["name"], "Test2", "Second object name should be 'Test2'"
        )
        self.assertEqual(
            results[1]["value"], 2, "Second object value should be 2"
        )

    async def test_values_no_objects(self):
        results = [
            obj
            async for obj in TestModel.async_objects.filter(id=10).values(
                "name", "value"
            )
        ]
        self.assertEqual(
            len(results),
            0,
            "Should return 0 value dictionaries when none exist",
        )

    async def test_values_with_filter(self):
        results = [
            obj
            async for obj in TestModel.async_objects.filter(
                name="Test1"
            ).values("name", "value")
        ]

        self.assertEqual(len(results), 1, "Should return 1 value dictionary")
        self.assertEqual(
            results[0]["name"], "Test1", "First object name should be 'Test1'"
        )
        self.assertEqual(
            results[0]["value"], 1, "First object value should be 1"
        )

    async def test_values_invalid_field(self):
        with self.assertRaises(FieldError):
            [
                obj
                async for obj in TestModel.async_objects.values(
                    "nonexistent_field"
                )
            ]

    async def test_values_annotate(self):
        results = [
            obj
            async for obj in TestModel.async_objects.annotate(
                count=Count("name")
            )
            .order_by("name")
            .values("name", "count")
        ]

        self.assertEqual(
            results,
            [{"name": "Test1", "count": 1}, {"name": "Test2", "count": 1}],
            "Results should be ordered by 'name' and include correct counts",
        )

    async def test_values_alias_requires_annotate(self):
        """alias() keeps the expression out of annotation_select, so selecting
        it by values() asks the user to promote it with annotate().
        """
        with self.assertRaisesRegex(
            FieldError,
            re.escape(
                "Cannot select the 'total' alias. Use annotate() to "
                "promote it."
            ),
        ):
            TestModel.async_objects.alias(total=Count("name")).values("total")

    async def test_values_masked_annotation_reports_previous_call(self):
        """When a previous values() call masked the annotation out, but other
        annotations are still selected, the error points at that call.
        """
        queryset = TestModel.async_objects.annotate(
            total=Count("name"), one=Value(1)
        ).values("one")

        with self.assertRaisesRegex(
            FieldError,
            re.escape(
                "Cannot select the 'total' alias. It was excluded by a "
                "previous values() or values_list() call. Include 'total' "
                "in that call to select it."
            ),
        ):
            queryset.values("total")

    async def test_values_list_masked_annotation_reports_previous_call(self):
        queryset = TestModel.async_objects.annotate(
            total=Count("name"), one=Value(1)
        ).values_list("one")

        with self.assertRaisesRegex(
            FieldError,
            re.escape(
                "Cannot select the 'total' alias. It was excluded by a "
                "previous values() or values_list() call. Include 'total' "
                "in that call to select it."
            ),
        ):
            queryset.values_list("total")

    async def test_values_selects_extra(self):
        """An extra() select is picked up by values() as a RawSQL column."""
        results = [
            obj
            async for obj in TestModel.async_objects.extra(
                select={"double_value": "value * 2"}
            )
            .order_by("name")
            .values("name", "double_value")
        ]

        self.assertEqual(
            results,
            [
                {"name": "Test1", "double_value": 2},
                {"name": "Test2", "double_value": 4},
            ],
            "values() should select the extra() column",
        )

    async def test_values_field_alongside_annotation(self):
        """A plain field selected while an annotation is in the mask goes
        through names_to_path() before being added to the field names.
        """
        results = [
            obj
            async for obj in TestModel.async_objects.annotate(
                total=Count("name")
            )
            .order_by("name")
            .values("total", "value")
        ]

        self.assertEqual(
            results,
            [{"total": 1, "value": 1}, {"total": 1, "value": 2}],
            "values() should select both the annotation and the field",
        )

    async def test_values_unresolvable_field_alongside_annotation(self):
        queryset = TestModel.async_objects.annotate(total=Count("name"))

        with self.assertRaises(FieldError):
            queryset.values("total", "nonexistent_field")

    async def test_values_no_fields(self):
        results = [obj async for obj in TestModel.async_objects.values()]

        self.assertEqual(len(results), 2, "Should return 2 value dictionaries")
        self.assertIn(
            "id", results[0], "Result should include 'id' field by default"
        )
        self.assertIn(
            "name", results[0], "Result should include 'name' field by default"
        )
        self.assertIn(
            "value",
            results[0],
            "Result should include 'value' field by default",
        )
        self.assertEqual(
            results[0]["name"], "Test1", "First object name should be 'Test1'"
        )
        self.assertEqual(
            results[1]["name"], "Test2", "Second object name should be 'Test2'"
        )
