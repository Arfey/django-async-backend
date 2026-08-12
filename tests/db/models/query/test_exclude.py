from django.core.exceptions import FieldError
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestExclude(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()

    async def test_exclude_by_name(self):
        results = [
            obj async for obj in TestModel.async_objects.exclude(name="Test1")
        ]

        self.assertEqual(len(results), 1, "Should exclude 1 object")
        self.assertEqual(
            results[0].name, "Test2", "Remaining object name should be 'Test2'"
        )

    async def test_exclude_no_results(self):
        results = [
            obj
            async for obj in TestModel.async_objects.exclude(
                name="Nonexistent"
            )
        ]

        self.assertEqual(
            len(results),
            2,
            "Should return all objects when none match the exclude condition",
        )

    async def test_exclude_invalid_field(self):
        with self.assertRaises(FieldError):
            [
                obj
                async for obj in TestModel.async_objects.exclude(
                    nonexistent_field="value"
                )
            ]

    async def test_exclude_multiple_conditions(self):
        results = [
            obj
            async for obj in TestModel.async_objects.exclude(
                name="Test1", value=1
            )
        ]

        self.assertEqual(
            len(results), 1, "Should exclude 1 object matching both conditions"
        )
        self.assertEqual(
            results[0].name, "Test2", "Remaining object name should be 'Test2'"
        )


class TestExcludeInWithNull(AsyncioTestCase):
    """`exclude(field__in=[...])` on a nullable column.

    Django strips None from the RHS of an `in` lookup, so the NULL rows have
    to be handled by an explicit IS NULL clause. Which connector joins that
    clause depends on whether None was in the original RHS:

        None present -> NOT (col IN (...) OR col IS NULL)   NULLs excluded
        None absent  -> NOT (col IN (...) AND col IS NOT NULL)  NULLs kept
    """

    async def asyncSetUp(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()
        await TestModel(name="TestNull", value=None).async_save()

    async def test_exclude_in_with_none_excludes_null_rows(self):
        queryset = TestModel.async_objects.exclude(value__in=[1, None])
        results = [obj async for obj in queryset.order_by("name")]

        self.assertEqual(
            [obj.name for obj in results],
            ["Test2"],
            "None in the RHS should exclude NULL rows as well as value=1",
        )
        self.assertIn(
            'IN (1) OR "test_model"."value" IS NULL',
            str(queryset.query),
            "IS NULL should be joined with OR when None is in the RHS",
        )

    async def test_exclude_in_without_none_keeps_null_rows(self):
        queryset = TestModel.async_objects.exclude(value__in=[1])
        results = [obj async for obj in queryset.order_by("name")]

        self.assertEqual(
            [obj.name for obj in results],
            ["Test2", "TestNull"],
            "Without None in the RHS, NULL rows must be preserved",
        )
        self.assertIn(
            'IN (1) AND "test_model"."value" IS NOT NULL',
            str(queryset.query),
            "IS NOT NULL should be joined with AND when None is absent",
        )

    async def test_exclude_in_with_none_accepts_non_list_iterable(self):
        # The branch checks for Iterable, not list, so a tuple takes the
        # same OR path.
        queryset = TestModel.async_objects.exclude(value__in=(1, None))
        results = [obj async for obj in queryset.order_by("name")]

        self.assertEqual(
            [obj.name for obj in results],
            ["Test2"],
            "A tuple RHS containing None should behave like a list",
        )
        self.assertIn(
            'IN (1) OR "test_model"."value" IS NULL',
            str(queryset.query),
            "IS NULL should be joined with OR for any non-str iterable",
        )

    async def test_exclude_in_with_only_none(self):
        # Stripping None empties the IN list entirely, leaving just IS NULL.
        queryset = TestModel.async_objects.exclude(value__in=[None])
        results = [obj async for obj in queryset.order_by("name")]

        self.assertEqual(
            [obj.name for obj in results],
            ["Test1", "Test2"],
            "Excluding [None] should drop only the NULL row",
        )

    async def test_exclude_in_with_str_rhs_is_not_treated_as_container(self):
        # A str RHS is iterable but must not be scanned for None; it is
        # expanded per-character by the `in` lookup instead.
        queryset = TestModel.async_objects.exclude(name__in="Test1")
        results = [obj async for obj in queryset.order_by("name")]

        # "Test1" expands to the chars T, e, s, t, 1 -- no row's full name
        # equals a single char, so nothing is excluded.
        self.assertEqual(
            [obj.name for obj in results],
            ["Test1", "Test2", "TestNull"],
            "A str RHS should be iterated per character, not as a container",
        )
        self.assertNotIn(
            "IS NULL",
            str(queryset.query),
            "A str RHS must never take the None-handling branch",
        )
