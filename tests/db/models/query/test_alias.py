from django.db import NotSupportedError
from django.db.models import (
    Count,
    F,
    Value,
)
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAlias(AsyncioTestCase):
    async def test_alias_used_in_filter(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()

        queryset = (
            TestModel.async_objects.alias(doubled=F("value") * 2)
            .filter(doubled=4)
            .values_list("name", flat=True)
        )

        self.assertEqual(
            [i async for i in queryset],
            ["Test2"],
            "alias should be usable in filter()",
        )

    async def test_alias_not_selected(self):
        """An alias is not added to the query's selected fields."""
        await TestModel(name="Test1", value=1).async_save()

        queryset = TestModel.async_objects.alias(doubled=F("value") * 2)

        obj = [i async for i in queryset][0]
        self.assertFalse(
            hasattr(obj, "doubled"),
            "alias should not be selected onto the result objects",
        )

    async def test_alias_with_aggregate(self):
        parent = TestModel(name="Parent", value=1)
        await parent.async_save()
        await TestModel(name="Child1", value=2, relative=parent).async_save()
        await TestModel(name="Child2", value=3, relative=parent).async_save()

        queryset = (
            TestModel.async_objects.alias(total=Count("relatives"))
            .filter(total=2)
            .values_list("name", flat=True)
        )

        self.assertEqual(
            [i async for i in queryset],
            ["Parent"],
            "alias should support aggregates in filter()",
        )

    async def test_alias_positional_expression(self):
        await TestModel(name="Test1", value=1).async_save()

        queryset = TestModel.async_objects.alias(Count("relatives")).filter(
            relatives__count=0
        )

        self.assertEqual(
            [i.name async for i in queryset],
            ["Test1"],
            "alias should accept a positional expression with a default alias",
        )

    async def test_complex_annotation_requires_alias(self):
        with self.assertRaisesRegex(
            TypeError, r"Complex annotations require an alias"
        ):
            TestModel.async_objects.alias(F("value") * 2)

    async def test_conflicting_default_alias(self):
        with self.assertRaisesRegex(
            ValueError,
            r"The named annotation 'relatives__count' conflicts with the "
            r"default name for another annotation\.",
        ):
            TestModel.async_objects.alias(
                Count("relatives"), relatives__count=Count("relatives")
            )

    async def test_non_expression(self):
        # _annotate() hardcodes method_name="annotate", so alias() reports
        # annotate() here too. This matches Django.
        with self.assertRaisesRegex(
            TypeError,
            r"QuerySet\.annotate\(\) received non-expression\(s\): 1\.",
        ):
            TestModel.async_objects.alias(total=1)

    async def test_conflicts_with_model_field(self):
        with self.assertRaisesRegex(
            ValueError,
            r"The annotation 'name' conflicts with a field on the model\.",
        ):
            TestModel.async_objects.alias(name=F("value"))

    async def test_after_union(self):
        queryset = TestModel.async_objects.filter(name="Test1").union(
            TestModel.async_objects.filter(name="Test2")
        )

        with self.assertRaisesRegex(
            NotSupportedError,
            r"Calling QuerySet\.alias\(\) after union\(\) is not supported\.",
        ):
            queryset.alias(doubled=F("value") * 2)

    async def test_alias_with_value(self):
        await TestModel(name="Test1", value=1).async_save()

        queryset = TestModel.async_objects.alias(label=Value("x")).filter(
            label="x"
        )

        self.assertEqual(
            [i.name async for i in queryset],
            ["Test1"],
            "alias should support Value() expressions",
        )
