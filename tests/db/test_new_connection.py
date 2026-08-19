import asyncio

from django.db import DEFAULT_DB_ALIAS
from test_app.models import TestModel

from django_async_backend.db import (
    async_connections,
    async_new_connection,
)
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import AsyncioTransactionTestCase


async def names():
    return [obj.name async for obj in TestModel.async_objects.order_by("name")]


class AsyncNewConnectionTests(AsyncioTransactionTestCase):
    async def asyncTearDown(self):
        await TestModel.async_objects.all().adelete()

    async def test_plain_await(self):
        """Wrapping a coroutine and awaiting it directly."""

        async def writer():
            await TestModel.async_objects.acreate(name="plain")
            return await names()

        self.assertEqual(await async_new_connection(writer()), ["plain"])

    async def test_gather(self):
        """Each gather'd call gets its own connection."""

        async def writer(i):
            await TestModel.async_objects.acreate(name=f"g{i}")

        await asyncio.gather(
            *(async_new_connection(writer(i)) for i in range(5))
        )

        self.assertEqual(await names(), ["g0", "g1", "g2", "g3", "g4"])

    async def test_task_group(self):
        """Each TaskGroup child gets its own connection."""

        async def writer(i):
            await TestModel.async_objects.acreate(name=f"t{i}")

        async with asyncio.TaskGroup() as tg:
            for i in range(5):
                tg.create_task(async_new_connection(writer(i)))

        self.assertEqual(await names(), ["t0", "t1", "t2", "t3", "t4"])

    async def test_decorator(self):
        """Usable as a decorator on an async function."""

        @async_new_connection
        async def writer(i):
            await TestModel.async_objects.acreate(name=f"d{i}")
            return i

        results = await asyncio.gather(*(writer(i) for i in range(3)))

        self.assertEqual(results, [0, 1, 2])
        self.assertEqual(await names(), ["d0", "d1", "d2"])

    async def test_context_manager(self):
        """Usable as an async context manager."""

        async with async_new_connection():
            await TestModel.async_objects.acreate(name="cm")

        self.assertEqual(await names(), ["cm"])

    async def test_own_transaction_per_task(self):
        """Each task can own a transaction on its own connection."""

        async def writer(i):
            async with async_atomic():
                await TestModel.async_objects.acreate(name=f"x{i}")

        await asyncio.gather(
            *(async_new_connection(writer(i)) for i in range(5))
        )

        self.assertEqual(len(await names()), 5)

    async def test_parent_connection_untouched(self):
        """The caller's connection is not swapped or closed."""
        parent = async_connections[DEFAULT_DB_ALIAS]
        await parent.ensure_connection()

        async def writer():
            self.assertIsNot(async_connections[DEFAULT_DB_ALIAS], parent)

        await async_new_connection(writer())

        self.assertIs(async_connections[DEFAULT_DB_ALIAS], parent)
        self.assertIsNotNone(parent.connection)

    async def test_connection_closed_on_exception(self):
        """A failure inside still closes the new connection."""
        seen = {}

        async def writer():
            seen["conn"] = async_connections[DEFAULT_DB_ALIAS]
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            await async_new_connection(writer())

        self.assertIsNone(seen["conn"].connection)

    async def test_rollback_isolated_from_parent(self):
        """A rolled-back task does not undo the parent's work."""
        await TestModel.async_objects.acreate(name="kept")

        async def writer():
            async with async_atomic():
                await TestModel.async_objects.acreate(name="dropped")
                raise ValueError("boom")

        with self.assertRaises(ValueError):
            await async_new_connection(writer())

        self.assertEqual(await names(), ["kept"])

    async def test_caller_connection_reusable_after(self):
        """The caller keeps using its own connection after the block.

        Detaching aliases from the caller's context would orphan the
        connection it was already using, so the next query would silently
        open a second one.
        """
        before = async_connections[DEFAULT_DB_ALIAS]
        await before.ensure_connection()

        async def writer():
            await TestModel.async_objects.acreate(name="inner")

        await async_new_connection(writer())

        after = async_connections[DEFAULT_DB_ALIAS]
        self.assertIs(after, before)
        self.assertIsNotNone(after.connection)
        self.assertEqual(await names(), ["inner"])
