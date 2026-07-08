from django.core.exceptions import FieldError
from django.db.models import (
    F,
    Sum,
)
from test_app.models import (
    ChildModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestAUpdate(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()
        await TestModel(name="Item3", value=3).async_save()

    async def test_returns_row_count(self):
        rows = await TestModel.async_objects.all().aupdate(value=42)
        self.assertEqual(rows, 3)
        values = sorted(
            [obj.value async for obj in TestModel.async_objects.all()]
        )
        self.assertEqual(values, [42, 42, 42])

    async def test_filtered_update(self):
        rows = await TestModel.async_objects.filter(name="Item1").aupdate(
            value=99
        )
        self.assertEqual(rows, 1)
        item = await TestModel.async_objects.aget(name="Item1")
        self.assertEqual(item.value, 99)

    async def test_f_expression(self):
        await TestModel.async_objects.all().aupdate(value=F("value") + 10)
        values = sorted(
            [obj.value async for obj in TestModel.async_objects.all()]
        )
        self.assertEqual(values, [11, 12, 13])

    async def test_no_match_returns_zero(self):
        rows = await TestModel.async_objects.filter(name="Nope").aupdate(
            value=0
        )
        self.assertEqual(rows, 0)

    async def test_sliced_raises(self):
        qs = TestModel.async_objects.all()[:1]
        with self.assertRaises(TypeError):
            await qs.aupdate(value=1)


class TestAUpdateOrderBy(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()
        await TestModel(name="Item3", value=3).async_save()

    async def test_order_by_field(self):
        rows = await TestModel.async_objects.order_by("value").aupdate(value=7)
        self.assertEqual(rows, 3)
        values = sorted(
            [obj.value async for obj in TestModel.async_objects.all()]
        )
        self.assertEqual(values, [7, 7, 7])

    async def test_order_by_descending_field(self):
        rows = await TestModel.async_objects.order_by("-value").aupdate(
            value=7
        )
        self.assertEqual(rows, 3)

    async def test_order_by_annotation_ascending(self):
        # Ordering by an annotation alias inlines the annotation expression.
        rows = await (
            TestModel.async_objects.annotate(double=F("value") * 2)
            .order_by("double")
            .aupdate(value=8)
        )
        self.assertEqual(rows, 3)
        values = sorted(
            [obj.value async for obj in TestModel.async_objects.all()]
        )
        self.assertEqual(values, [8, 8, 8])

    async def test_order_by_annotation_descending(self):
        # The "-" prefix inlines the annotation wrapped in .desc().
        rows = await (
            TestModel.async_objects.annotate(double=F("value") * 2)
            .order_by("-double")
            .aupdate(value=9)
        )
        self.assertEqual(rows, 3)
        values = sorted(
            [obj.value async for obj in TestModel.async_objects.all()]
        )
        self.assertEqual(values, [9, 9, 9])

    async def test_order_by_aggregate_annotation_raises(self):
        # Ordering by an aggregate annotation cannot be inlined into an
        # UPDATE and must raise FieldError.
        qs = TestModel.async_objects.annotate(total=Sum("value")).order_by(
            "total"
        )
        with self.assertRaises(FieldError):
            await qs.aupdate(value=1)


class TestAUpdateRelated(AsyncioTestCase):
    async def test_parent_field_update_raises_not_implemented(self):
        # Updating a field that lives on a parent model (multi-table
        # inheritance) populates query.related_updates, which would force
        # SQLUpdateCompiler.pre_sql_setup() to run a pre-select through
        # Django's sync connection registry -- outside async_connections.
        # The async SQLUpdateCompiler rejects this instead of silently
        # using the wrong connection.
        with self.assertRaises(NotImplementedError):
            await ChildModel.async_objects.all().aupdate(parent_value=5)
