from django.db import DEFAULT_DB_ALIAS
from django.test import TestCase
from test_app.models import TestModel

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)


class TestMock(TestCase):
    def test_mock(self):
        pass


class TestGetItem(AsyncioTestCase):

    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1")
        await TestModel.async_object.acreate(name="Item2")
        await TestModel.async_object.acreate(name="Item3")
        await TestModel.async_object.acreate(name="Item4")

    async def test_get_single_item(self):
        qs = TestModel.async_object.all()

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            item = await qs[1]
            self.assertEqual(item.name, "Item2")
            self.assertIsNone(qs._result_cache, "Cache should be empty")

            item = await qs[1]
            self.assertEqual(
                item.name, "Item2", "Should fetch the second item"
            )
            self.assertEqual(len(ctx.captured_queries), 2)
            self.assertIsNone(qs._result_cache, "Cache should be empty")

        # populate cache
        [i async for i in qs]

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            item = await qs[0]
            await qs[1]
            await qs[2]

            self.assertEqual(item.name, "Item1")
            self.assertEqual(len(ctx.captured_queries), 0)
            self.assertEqual(len(qs._result_cache), await qs.acount())

    async def test_get_slice(self):
        qs = TestModel.async_object.all()

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            items = [i async for i in qs[1:3]]

            self.assertEqual(len(ctx.captured_queries), 1)
            self.assertEqual(len(items), 2, "Should fetch two items")
            self.assertEqual(
                items[0].name, "Item2", "First item in slice should be 'Item2'"
            )
            self.assertEqual(
                items[1].name,
                "Item3",
                "Second item in slice should be 'Item3'",
            )

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            # populate cache
            [i async for i in qs]
            self.assertEqual(len(ctx.captured_queries), 1)

            items = [i async for i in qs[1:3]]
            self.assertEqual(len(ctx.captured_queries), 1)
            self.assertEqual(len(items), 2, "Should fetch two items")
            self.assertEqual(
                items[0].name, "Item2", "First item in slice should be 'Item2'"
            )
            self.assertEqual(
                items[1].name,
                "Item3",
                "Second item in slice should be 'Item3'",
            )

    async def test_get_slice_with_step(self):
        qs = TestModel.async_object.all()
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            items = [i async for i in qs[0:3:2]]
            self.assertEqual(len(ctx.captured_queries), 1)

        self.assertEqual(
            len(items), 2, "Should fetch two items with a step of 2"
        )
        self.assertEqual(
            items[0].name, "Item1", "First item in slice should be 'Item1'"
        )
        self.assertEqual(
            items[1].name, "Item3", "Second item in slice should be 'Item3'"
        )

        # populate cache
        [i async for i in qs]

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            items = [i async for i in qs[0:3:2]]
            self.assertEqual(len(ctx.captured_queries), 0)

    async def test_invalid_index(self):
        with self.assertRaises(TypeError):
            await TestModel.async_object.all()["invalid"]

    async def test_negative_index(self):
        with self.assertRaises(ValueError):
            await TestModel.async_object.all()[-1]

    async def test_out_of_range(self):
        with self.assertRaises(IndexError):
            await TestModel.async_object.all()[100]
