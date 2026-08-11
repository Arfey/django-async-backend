import re

from django.db.models import (
    Count,
    FilteredRelation,
    Q,
)
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAnnotate(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()
        await TestModel(name="Test3", value=3).async_save()

    async def test_annotate_count(self):
        results = [
            obj
            async for obj in TestModel.async_objects.values("value")
            .annotate(count=Count("value"))
            .order_by("value")
        ]

        self.assertEqual(
            len(results), 3, "Should return 3 distinct value groups"
        )
        self.assertEqual(
            results[0]["value"], 1, "First group value should be 1"
        )
        self.assertEqual(
            results[0]["count"], 1, "First group count should be 1"
        )
        self.assertEqual(
            results[1]["value"], 2, "Second group value should be 2"
        )
        self.assertEqual(
            results[1]["count"], 1, "Second group count should be 1"
        )
        self.assertEqual(
            results[2]["value"], 3, "Third group value should be 3"
        )
        self.assertEqual(
            results[2]["count"], 1, "Third group count should be 1"
        )

    async def test_annotate_with_filter(self):
        results = [
            obj
            async for obj in TestModel.async_objects.filter(value=1)
            .values("value")
            .annotate(count=Count("value"))
        ]

        self.assertEqual(
            len(results), 1, "Should return 1 group for filtered value"
        )
        self.assertEqual(results[0]["value"], 1, "Group value should be 1")
        self.assertEqual(results[0]["count"], 1, "Group count should be 1")

    async def test_annotate_filtered_relation_period_forbidden(self):
        """A period in the alias would be ambiguous with a qualified column
        name, so add_filtered_relation() rejects it before anything else.
        """
        msg = (
            "FilteredRelation doesn't support aliases with periods "
            "(got 'relatives.test1')."
        )

        with self.assertRaisesRegex(ValueError, re.escape(msg)):
            TestModel.async_objects.annotate(
                **{
                    "relatives.test1": FilteredRelation(
                        "relatives",
                        condition=Q(relatives__name__iexact="test1"),
                    )
                }
            )

    async def test_annotate_filtered_relation_without_period(self):
        """An alias without a period passes the check and builds the join."""
        parent = await TestModel.async_objects.aget(name="Test1")
        child = TestModel(name="Child", value=4, relative=parent)
        await child.async_save()

        results = [
            obj
            async for obj in TestModel.async_objects.annotate(
                matched=FilteredRelation(
                    "relatives",
                    condition=Q(relatives__name__iexact="child"),
                )
            )
            .filter(matched__isnull=False)
            .order_by("name")
        ]

        self.assertEqual(
            [obj.name for obj in results],
            ["Test1"],
            "Only the parent of the matched relative should be returned",
        )

    async def test_annotate_no_results(self):
        results = [
            obj
            async for obj in TestModel.async_objects.filter(value=99)
            .values("value")
            .annotate(count=Count("value"))
        ]

        self.assertEqual(
            len(results),
            0,
            "Should return no groups when no objects match the filter",
        )
