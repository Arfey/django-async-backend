from django.test import TestCase
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestMock(TestCase):
    def test_mock(self):
        pass


class TestSyncIterationGuard(AsyncioTestCase):

    async def test_iter_raises(self):
        qs = TestModel.async_objects.all()

        with self.assertRaisesRegex(
            TypeError, "QuerySet is not iterable synchronously"
        ):
            iter(qs)

    async def test_list_raises(self):
        qs = TestModel.async_objects.all()

        with self.assertRaisesRegex(
            TypeError, "QuerySet is not iterable synchronously"
        ):
            list(qs)

    async def test_sync_comprehension_raises(self):
        qs = TestModel.async_objects.all()

        with self.assertRaisesRegex(
            TypeError, "QuerySet is not iterable synchronously"
        ):
            [obj for obj in qs]

    async def test_extend_raises(self):
        """The Collector.delete() regression: list.extend() used to hang."""
        qs = TestModel.async_objects.all()
        objs = []

        with self.assertRaisesRegex(
            TypeError, "QuerySet is not iterable synchronously"
        ):
            objs.extend(qs)

    async def test_contains_raises(self):
        """`in` falls back to __iter__, so it fails fast too."""
        obj = TestModel(name="Item1")
        await obj.async_save()

        qs = TestModel.async_objects.all()

        with self.assertRaises(TypeError):
            obj in qs

    async def test_truthiness_raises(self):
        """QuerySet has no __bool__, so truth-testing falls back to __len__."""
        qs = TestModel.async_objects.all()

        self.assertNotIn(
            "__bool__",
            type(qs).__dict__,
            "A __bool__ of its own would shadow the __len__ guard",
        )

        with self.assertRaisesRegex(
            TypeError, r"len\(\) is not supported on QuerySet"
        ):
            bool(qs)

        with self.assertRaisesRegex(
            TypeError, r"len\(\) is not supported on QuerySet"
        ):
            if qs:
                pass

    async def test_len_raises(self):
        qs = TestModel.async_objects.all()

        with self.assertRaisesRegex(
            TypeError, r"len\(\) is not supported on QuerySet"
        ):
            len(qs)

    async def test_guard_applies_with_populated_cache(self):
        """A filled result cache is what the freeze needed, not what saved it.

        collect() evaluates its sub-querysets, so the ones reaching
        Collector.delete() carry a populated _result_cache. That is the exact
        state in which __getitem__ hands back coroutines forever.
        """
        await TestModel(name="Item1").async_save()

        qs = TestModel.async_objects.all()
        [obj async for obj in qs]

        self.assertIsNotNone(qs._result_cache, "Cache should be populated")

        with self.assertRaisesRegex(
            TypeError, "QuerySet is not iterable synchronously"
        ):
            list(qs)

        with self.assertRaisesRegex(
            TypeError, r"len\(\) is not supported on QuerySet"
        ):
            len(qs)

    async def test_error_message_points_at_the_async_alternative(self):
        qs = TestModel.async_objects.all()

        with self.assertRaises(TypeError) as ctx:
            list(qs)

        self.assertIn("async for obj in qs", str(ctx.exception))

        with self.assertRaises(TypeError) as ctx:
            len(qs)

        self.assertIn("await qs.acount()", str(ctx.exception))

    async def test_async_iteration_still_works(self):
        await TestModel(name="Item1").async_save()
        await TestModel(name="Item2").async_save()

        qs = TestModel.async_objects.all()
        results = [obj async for obj in qs]

        self.assertEqual(len(results), 2, "Should iterate over 2 objects")
        self.assertEqual(await qs.acount(), 2)
