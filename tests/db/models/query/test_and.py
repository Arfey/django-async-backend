from test_app.models import TestModel

from django_async_backend.db.models.query import QuerySet as AsyncQuerySet
from django_async_backend.test import AsyncioTestCase


class TestAnd(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()
        await TestModel(name="Item3", value=3).async_save()

    async def test_and_with_results(self):
        queryset1 = TestModel.async_objects.filter(value__lte=2)
        queryset2 = TestModel.async_objects.filter(value__gte=2)
        and_queryset = queryset1 & queryset2

        results = [item async for item in and_queryset.order_by("value")]
        self.assertEqual(
            [item.name for item in results],
            ["Item2"],
            "& should return only items matching both querysets",
        )

    async def test_and_no_results(self):
        queryset1 = TestModel.async_objects.filter(value=1)
        queryset2 = TestModel.async_objects.filter(value=3)
        and_queryset = queryset1 & queryset2

        results = [item async for item in and_queryset]
        self.assertEqual(
            len(results),
            0,
            "& should return no items when the querysets do not overlap",
        )

    async def test_and_with_identical_querysets(self):
        queryset1 = TestModel.async_objects.filter(value__gte=2)
        queryset2 = TestModel.async_objects.filter(value__gte=2)
        and_queryset = queryset1 & queryset2

        results = [item async for item in and_queryset.order_by("value")]
        self.assertEqual(
            [item.name for item in results],
            ["Item2", "Item3"],
            "& should not duplicate items matched by both querysets",
        )

    async def test_and_with_empty_right_queryset(self):
        queryset1 = TestModel.async_objects.filter(value=2)
        queryset2 = TestModel.async_objects.none()
        and_queryset = queryset1 & queryset2

        self.assertIs(
            and_queryset,
            queryset2,
            "& with an empty queryset should return the empty queryset",
        )

        results = [item async for item in and_queryset]
        self.assertEqual(
            len(results), 0, "& with an empty queryset should match nothing"
        )

    async def test_and_with_empty_left_queryset(self):
        queryset1 = TestModel.async_objects.none()
        queryset2 = TestModel.async_objects.filter(value=2)
        and_queryset = queryset1 & queryset2

        self.assertIs(
            and_queryset,
            queryset1,
            "& on an empty queryset should return the empty queryset",
        )

        results = [item async for item in and_queryset]
        self.assertEqual(
            len(results), 0, "& on an empty queryset should match nothing"
        )

    async def test_and_with_both_empty_querysets(self):
        queryset1 = TestModel.async_objects.none()
        queryset2 = TestModel.async_objects.none()
        and_queryset = queryset1 & queryset2

        self.assertIs(
            and_queryset,
            queryset2,
            "& should short-circuit on the right queryset first",
        )

    async def test_and_with_sliced_queryset(self):
        queryset1 = TestModel.async_objects.order_by("value")[:2]
        queryset2 = TestModel.async_objects.filter(value=2)

        # Unlike |, & does not wrap a sliced queryset into a pk subquery, so
        # combining onto a sliced query is rejected by Query.combine().
        with self.assertRaisesRegex(
            TypeError, "Cannot combine queries once a slice has been taken."
        ):
            queryset1 & queryset2

    async def test_and_stays_async(self):
        queryset1 = TestModel.async_objects.filter(value__lte=2)
        queryset2 = TestModel.async_objects.filter(value__gte=2)

        and_queryset = queryset1 & queryset2

        self.assertIsInstance(
            and_queryset,
            AsyncQuerySet,
            "& should stay on the async queryset",
        )

    async def test_and_returns_new_queryset(self):
        queryset1 = TestModel.async_objects.filter(value__lte=2)
        queryset2 = TestModel.async_objects.filter(value__gte=2)

        and_queryset = queryset1 & queryset2

        self.assertIsNot(and_queryset, queryset1)
        self.assertIsNot(and_queryset, queryset2)
        self.assertIsNone(
            queryset1._result_cache, "& should not evaluate the operands"
        )

    async def test_and_merges_known_related_objects(self):
        # There is no async related manager to populate _known_related_objects,
        # so it is seeded the way a related manager would.
        parent = await TestModel.async_objects.aget(name="Item1")
        relative = TestModel._meta.get_field("relative")

        queryset1 = TestModel.async_objects.filter(value=2)
        queryset2 = TestModel.async_objects.filter(value=3)
        queryset2._known_related_objects = {relative: {parent.pk: parent}}

        and_queryset = queryset1 & queryset2

        self.assertEqual(
            and_queryset._known_related_objects,
            {relative: {parent.pk: parent}},
            "& should carry known related objects over from the right queryset",
        )

    async def test_and_with_combined_queryset(self):
        combined = TestModel.async_objects.filter(value=1).union(
            TestModel.async_objects.filter(value=3)
        )

        with self.assertRaises(TypeError):
            combined & TestModel.async_objects.filter(value=2)

        with self.assertRaises(TypeError):
            TestModel.async_objects.filter(value=2) & combined

    async def test_and_with_sync_queryset(self):
        with self.assertRaises(TypeError):
            TestModel.async_objects.filter(value=1) & TestModel.objects.filter(
                value=3
            )

    async def test_and_with_empty_right_and_sync_queryset(self):
        # The EmptyQuerySet shortcut returns `other` as is, so an unchecked
        # sync queryset would escape the async ORM entirely.
        with self.assertRaises(TypeError):
            TestModel.async_objects.filter(value=1) & TestModel.objects.none()

    async def test_and_with_empty_left_and_sync_queryset(self):
        # InstanceCheckMeta only recognises async querysets, so a sync
        # queryset is not treated as an EmptyQuerySet either.
        with self.assertRaises(TypeError):
            TestModel.async_objects.none() & TestModel.objects.filter(value=3)

    async def test_and_with_mismatched_values(self):
        queryset1 = TestModel.async_objects.values("name")
        queryset2 = TestModel.async_objects.values("value")

        with self.assertRaises(TypeError):
            queryset1 & queryset2
