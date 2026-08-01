from test_app.models import TestModel

from django_async_backend.db.models.query import QuerySet as AsyncQuerySet
from django_async_backend.test import AsyncioTestCase


class TestOr(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()
        await TestModel(name="Item3", value=3).async_save()

    async def test_or_with_results(self):
        queryset1 = TestModel.async_objects.filter(value=1)
        queryset2 = TestModel.async_objects.filter(value=3)
        or_queryset = queryset1 | queryset2

        results = [item async for item in or_queryset.order_by("value")]
        self.assertEqual(
            [item.name for item in results],
            ["Item1", "Item3"],
            "| should return items matching either queryset",
        )

    async def test_or_with_overlapping_querysets(self):
        queryset1 = TestModel.async_objects.filter(value__lte=2)
        queryset2 = TestModel.async_objects.filter(value__gte=2)
        or_queryset = queryset1 | queryset2

        results = [item async for item in or_queryset.order_by("value")]
        self.assertEqual(
            [item.name for item in results],
            ["Item1", "Item2", "Item3"],
            "| should not duplicate items matched by both querysets",
        )

    async def test_or_no_results(self):
        queryset1 = TestModel.async_objects.filter(value=0)
        queryset2 = TestModel.async_objects.filter(value=4)
        or_queryset = queryset1 | queryset2

        results = [item async for item in or_queryset]
        self.assertEqual(
            len(results),
            0,
            "| should return no items if both querysets are empty",
        )

    async def test_or_with_empty_left_queryset(self):
        queryset1 = TestModel.async_objects.none()
        queryset2 = TestModel.async_objects.filter(value=2)
        or_queryset = queryset1 | queryset2

        self.assertIs(
            or_queryset,
            queryset2,
            "| on an empty queryset should return the right queryset",
        )

        results = [item async for item in or_queryset]
        self.assertEqual(
            [item.name for item in results],
            ["Item2"],
            "| should include only 'Item2'",
        )

    async def test_or_with_empty_right_queryset(self):
        queryset1 = TestModel.async_objects.filter(value=2)
        queryset2 = TestModel.async_objects.none()
        or_queryset = queryset1 | queryset2

        self.assertIs(
            or_queryset,
            queryset1,
            "| with an empty queryset should return the left queryset",
        )

        results = [item async for item in or_queryset]
        self.assertEqual(
            [item.name for item in results],
            ["Item2"],
            "| should include only 'Item2'",
        )

    async def test_or_with_sliced_left_queryset(self):
        queryset1 = TestModel.async_objects.order_by("value")[:1]
        queryset2 = TestModel.async_objects.filter(value=3)
        or_queryset = queryset1 | queryset2

        self.assertIsInstance(
            or_queryset,
            AsyncQuerySet,
            "| on a sliced queryset should stay on the async queryset",
        )

        results = [item async for item in or_queryset.order_by("value")]
        self.assertEqual(
            [item.name for item in results],
            ["Item1", "Item3"],
            "| should wrap the sliced queryset into a pk subquery",
        )

    async def test_or_with_sliced_right_queryset(self):
        queryset1 = TestModel.async_objects.filter(value=3)
        queryset2 = TestModel.async_objects.order_by("value")[:1]
        or_queryset = queryset1 | queryset2

        self.assertIsInstance(
            or_queryset,
            AsyncQuerySet,
            "| with a sliced queryset should stay on the async queryset",
        )

        results = [item async for item in or_queryset.order_by("value")]
        self.assertEqual(
            [item.name for item in results],
            ["Item1", "Item3"],
            "| should wrap the sliced queryset into a pk subquery",
        )

    async def test_or_merges_known_related_objects(self):
        # There is no async related manager to populate _known_related_objects,
        # so it is seeded the way a related manager would.
        parent = await TestModel.async_objects.aget(name="Item1")
        relative = TestModel._meta.get_field("relative")

        queryset1 = TestModel.async_objects.filter(value=2)
        queryset2 = TestModel.async_objects.filter(value=3)
        queryset2._known_related_objects = {relative: {parent.pk: parent}}

        or_queryset = queryset1 | queryset2

        self.assertEqual(
            or_queryset._known_related_objects,
            {relative: {parent.pk: parent}},
            "| should carry known related objects over from the right queryset",
        )

    async def test_or_with_combined_queryset(self):
        combined = TestModel.async_objects.filter(value=1).union(
            TestModel.async_objects.filter(value=3)
        )

        with self.assertRaises(TypeError):
            combined | TestModel.async_objects.filter(value=2)

        with self.assertRaises(TypeError):
            TestModel.async_objects.filter(value=2) | combined

    async def test_or_with_sync_queryset(self):
        with self.assertRaises(TypeError):
            TestModel.async_objects.filter(value=1) | TestModel.objects.filter(
                value=3
            )

    async def test_or_with_empty_left_and_sync_queryset(self):
        # The EmptyQuerySet shortcut returns `other` as is, so an unchecked
        # sync queryset would escape the async ORM entirely.
        with self.assertRaises(TypeError):
            TestModel.async_objects.none() | TestModel.objects.filter(value=3)

    async def test_or_with_empty_sync_queryset(self):
        # InstanceCheckMeta only recognises async querysets, so a sync none()
        # is not treated as an EmptyQuerySet either.
        with self.assertRaises(TypeError):
            TestModel.async_objects.filter(value=1) | TestModel.objects.none()

    async def test_or_with_mismatched_values(self):
        queryset1 = TestModel.async_objects.values("name")
        queryset2 = TestModel.async_objects.values("value")

        with self.assertRaises(TypeError):
            queryset1 | queryset2
