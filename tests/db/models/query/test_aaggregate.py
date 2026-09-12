import re

from django.db.models import (
    Avg,
    Count,
    F,
    Max,
    Min,
    Sum,
)
from django.db.models.functions import Upper
from test_app.models import (
    TestModel,
    TotalOrderingCompositePkModel,
)

from django_async_backend.test import AsyncioTestCase


class TestAAggregate(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Test1", value=1).async_save()
        await TestModel(name="Test2", value=2).async_save()
        await TestModel(name="Test3", value=3).async_save()

    async def test_aaggregate_with_kwargs(self):
        """A named aggregate is returned under the alias it was given."""
        result = await TestModel.async_objects.aaggregate(
            total=Sum("value"),
        )

        self.assertEqual(
            result, {"total": 6}, "Sum should be returned under its alias"
        )

    async def test_aaggregate_several_kwargs(self):
        """Several aggregates resolve in a single query."""
        result = await TestModel.async_objects.aaggregate(
            total=Sum("value"),
            lowest=Min("value"),
            highest=Max("value"),
            rows=Count("id"),
        )

        self.assertEqual(
            result,
            {"total": 6, "lowest": 1, "highest": 3, "rows": 3},
            "Every alias should be present in the result",
        )

    async def test_aaggregate_positional_arg_uses_default_alias(self):
        """A positional aggregate is stored under its default_alias."""
        result = await TestModel.async_objects.aaggregate(Sum("value"))

        self.assertEqual(
            result,
            {"value__sum": 6},
            "Positional aggregate should use its default alias",
        )

    async def test_aaggregate_positional_and_keyword_args(self):
        """Positional and named aggregates can be mixed."""
        result = await TestModel.async_objects.aaggregate(
            Max("value"),
            lowest=Min("value"),
        )

        self.assertEqual(
            result,
            {"value__max": 3, "lowest": 1},
            "Both the default alias and the explicit alias should be present",
        )

    async def test_aaggregate_complex_positional_requires_alias(self):
        """An expression without a default_alias has to be named explicitly."""
        msg = "Complex aggregates require an alias"

        with self.assertRaisesRegex(TypeError, re.escape(msg)):
            await TestModel.async_objects.aaggregate(Sum("value") / 2)

    async def test_aaggregate_non_expression_rejected(self):
        """Plain values are rejected before the query is built."""
        msg = "QuerySet.aggregate() received non-expression(s): 1."

        with self.assertRaisesRegex(TypeError, re.escape(msg)):
            await TestModel.async_objects.aaggregate(total=1)

    async def test_aaggregate_distinct_fields_not_implemented(self):
        """distinct(*fields) cannot be combined with aggregation."""
        msg = "aggregate() + distinct(fields) not implemented."

        with self.assertRaisesRegex(NotImplementedError, re.escape(msg)):
            await TestModel.async_objects.distinct("name").aaggregate(
                total=Sum("value")
            )

    async def test_aaggregate_no_aggregates(self):
        """Aggregating nothing produces an empty mapping."""
        result = await TestModel.async_objects.aaggregate()

        self.assertEqual(
            result, {}, "No aggregates should yield an empty dict"
        )

    async def test_aaggregate_with_filter(self):
        """The aggregate is computed over the filtered rows only."""
        result = await TestModel.async_objects.filter(value__gt=1).aaggregate(
            total=Sum("value"),
        )

        self.assertEqual(
            result, {"total": 5}, "Only matching rows should be summed"
        )

    async def test_aaggregate_no_matching_rows(self):
        """Sum over an empty result set is None, while Count is 0."""
        result = await TestModel.async_objects.filter(value=99).aaggregate(
            total=Sum("value"),
            rows=Count("id"),
        )

        self.assertEqual(
            result,
            {"total": None, "rows": 0},
            "Sum should be None and Count 0 when nothing matches",
        )

    async def test_aaggregate_over_annotation(self):
        """An aggregate can reference an annotation added earlier."""
        result = await (
            TestModel.async_objects.annotate(doubled=Sum("value") * 2)
            .values("doubled")
            .aaggregate(total=Sum("doubled"))
        )

        self.assertEqual(
            result,
            {"total": 12},
            "The aggregate should be computed over the annotated values",
        )

    async def test_aaggregate_avg(self):
        """Avg returns a float mean over the selected rows."""
        result = await TestModel.async_objects.aaggregate(Avg("value"))

        self.assertEqual(
            result, {"value__avg": 2.0}, "Average of 1, 2 and 3 should be 2.0"
        )

    async def test_aaggregate_on_sliced_queryset(self):
        """A slice is aggregated through a subquery, not after the fact."""
        result = await TestModel.async_objects.order_by("value")[
            :2
        ].aaggregate(total=Sum("value"))

        self.assertEqual(
            result,
            {"total": 3},
            "Only the first two rows should be summed",
        )

    async def test_aaggregate_non_aggregate_expression_rejected(self):
        """An expression that resolves but does not aggregate is rejected.

        This is a later check than the non-expression one: F() has
        resolve_expression(), so it only fails once it turns out not to
        contain an aggregate.
        """
        msg = "total is not an aggregate expression"

        with self.assertRaisesRegex(TypeError, re.escape(msg)):
            await TestModel.async_objects.aaggregate(total=F("value"))

    async def test_aaggregate_over_existing_aggregate_annotation(self):
        """An existing aggregate annotation forces the inner query to be
        grouped by the primary key, since the default columns are selected.
        """
        result = await TestModel.async_objects.annotate(
            relative_count=Count("relatives")
        ).aaggregate(total=Sum("value"))

        self.assertEqual(
            result,
            {"total": 6},
            "The outer aggregate should sum each row exactly once",
        )

    async def test_aaggregate_masks_grouping_annotation(self):
        """A non-aggregate annotation that contributes group by columns is
        kept in the inner query's annotation mask rather than elided.
        """
        result = await (
            TestModel.async_objects.annotate(upper_name=Upper("name"))
            .order_by("value")[:2]
            .aaggregate(total=Sum("value"))
        )

        self.assertEqual(
            result,
            {"total": 3},
            "Only the two sliced rows should be summed",
        )

    async def test_aaggregate_with_non_aggregate_annotation(self):
        """A plain annotation is inlined into the aggregate instead of
        creating a subquery, because it is not itself an aggregate.
        """
        result = await TestModel.async_objects.annotate(
            upper_name=Upper("name")
        ).aaggregate(total=Sum("value"))

        self.assertEqual(
            result, {"total": 6}, "The annotation should not affect the sum"
        )

    async def test_aaggregate_on_empty_result_set(self):
        """An impossible filter short circuits before SQL runs, so the
        aggregate falls back to its empty result value.
        """
        result = await TestModel.async_objects.filter(pk__in=[]).aaggregate(
            total=Sum("value"),
        )

        self.assertEqual(
            result,
            {"total": None},
            "Sum over an empty result set should be None",
        )

    async def test_acount_on_sliced_queryset(self):
        """Counting a slice selects the primary key in the inner query, since
        a slice needs a subquery but selects no field of its own.
        """
        count = await TestModel.async_objects.all()[:2].acount()

        self.assertEqual(count, 2, "The slice should limit the count to 2")

    async def test_aaggregate_composite_pk(self):
        """Aggregating a model with a composite primary key works as long as
        the aggregate itself does not wrap the composite field, which Django
        rejects up front.
        """
        await TotalOrderingCompositePkModel(
            tenant_id=1, code=1, label="a"
        ).async_save()
        await TotalOrderingCompositePkModel(
            tenant_id=1, code=2, label="b"
        ).async_save()

        result = await TotalOrderingCompositePkModel.async_objects.annotate(
            composite=F("pk")
        ).aaggregate(rows=Count("*"))

        self.assertEqual(
            result, {"rows": 2}, "Both composite pk rows should be counted"
        )
