import asyncio
import contextvars

from django.db import DEFAULT_DB_ALIAS
from django.utils.connection import ConnectionDoesNotExist
from test_app.models import TestModel

from django_async_backend.db import (
    async_acquire,
    async_connections,
)
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import AsyncioTransactionTestCase


async def names():
    return [obj.name async for obj in TestModel.async_objects.order_by("name")]


def initialized(alias):
    return hasattr(async_connections._connections, alias)


class AsyncAcquireTests(AsyncioTransactionTestCase):
    async def asyncTearDown(self):
        await TestModel.async_objects.all().adelete()

    async def test_yields_connection_for_alias(self):
        """The yielded connection is the one the ORM uses for the alias."""
        async with async_acquire("other") as conn:
            self.assertEqual(conn.alias, "other")
            self.assertIs(conn, async_connections["other"])

    async def test_defaults_to_default_alias(self):
        async with async_acquire() as conn:
            self.assertEqual(conn.alias, DEFAULT_DB_ALIAS)

    async def test_reuses_connection_owned_by_current_task(self):
        """In the owning task the existing connection is reused, not closed."""
        parent = async_connections[DEFAULT_DB_ALIAS]
        await parent.ensure_connection()

        async with async_acquire() as conn:
            self.assertIs(conn, parent)

        self.assertIs(async_connections[DEFAULT_DB_ALIAS], parent)
        self.assertIsNotNone(parent.connection)

    async def test_nested_reuses_same_connection(self):
        async with async_acquire() as outer:
            async with async_acquire() as inner:
                self.assertIs(inner, outer)

    async def test_gather_gets_own_connections(self):
        """A helper using async_acquire() is safe to run under gather()."""
        parent = async_connections[DEFAULT_DB_ALIAS]
        await parent.ensure_connection()
        seen = []

        async def writer(i):
            async with async_acquire() as conn:
                seen.append(conn)
                await TestModel.async_objects.acreate(name=f"g{i}")

        await asyncio.gather(*(writer(i) for i in range(3)))

        self.assertEqual(await names(), ["g0", "g1", "g2"])
        self.assertEqual(len({id(c) for c in seen}), 3)
        for conn in seen:
            self.assertIsNot(conn, parent)
            self.assertIsNone(conn.connection)
        self.assertIs(async_connections[DEFAULT_DB_ALIAS], parent)
        self.assertIsNotNone(parent.connection)

    async def test_task_group_gets_own_connections(self):
        async def writer(i):
            async with async_acquire():
                await TestModel.async_objects.acreate(name=f"t{i}")

        async with asyncio.TaskGroup() as tg:
            for i in range(3):
                tg.create_task(writer(i))

        self.assertEqual(await names(), ["t0", "t1", "t2"])

    async def test_reuses_unused_connection_created_by_current_task(self):
        """A connection created but never used in this task (as left by the
        close_async_connections middleware) is still owned by it and gets
        reused, while child tasks get their own.
        """
        seen = {}

        async def request():
            parent = async_connections[DEFAULT_DB_ALIAS]
            seen["parent"] = parent

            async with async_acquire() as conn:
                seen["reused"] = conn

            async def writer(i):
                async with async_acquire() as conn:
                    seen.setdefault("children", []).append(conn)
                    await TestModel.async_objects.acreate(name=f"u{i}")

            await asyncio.gather(*(writer(i) for i in range(2)))
            seen["names"] = await names()
            await parent.close()

        await asyncio.get_running_loop().create_task(
            request(), context=contextvars.Context()
        )

        self.assertIs(seen["reused"], seen["parent"])
        for conn in seen["children"]:
            self.assertIsNot(conn, seen["parent"])
        self.assertEqual(seen["names"], ["u0", "u1"])

    async def test_nested_in_child_reuses_new_connection(self):
        seen = []

        async def writer():
            async with async_acquire() as outer:
                async with async_acquire() as inner:
                    seen.extend([outer, inner])

        await asyncio.gather(writer())

        outer, inner = seen
        self.assertIs(inner, outer)
        self.assertIsNot(outer, async_connections[DEFAULT_DB_ALIAS])

    async def test_creates_and_closes_when_not_initialized(self):
        """In a fresh context the connection is opened and closed by us."""
        seen = {}

        async def writer():
            self.assertFalse(initialized(DEFAULT_DB_ALIAS))
            async with async_acquire() as conn:
                seen["conn"] = conn
                await TestModel.async_objects.acreate(name="fresh")
            seen["initialized_after"] = initialized(DEFAULT_DB_ALIAS)

        await asyncio.get_running_loop().create_task(
            writer(), context=contextvars.Context()
        )

        self.assertIsNone(seen["conn"].connection)
        self.assertFalse(seen["initialized_after"])
        self.assertEqual(await names(), ["fresh"])

    async def test_new_connection_closed_on_exception(self):
        parent = async_connections[DEFAULT_DB_ALIAS]
        seen = {}

        async def writer():
            async with async_acquire() as conn:
                seen["conn"] = conn
                await conn.ensure_connection()
                raise ValueError("boom")

        with self.assertRaises(ValueError):
            await asyncio.create_task(writer())

        self.assertIsNone(seen["conn"].connection)
        self.assertIs(async_connections[DEFAULT_DB_ALIAS], parent)

    async def test_refuses_to_escape_parent_transaction(self):
        """Forking off a connection inside the parent's atomic block would
        silently run outside its transaction, so it raises instead."""

        async def writer():
            async with async_acquire():
                pass

        async with async_atomic():
            with self.assertRaisesRegex(RuntimeError, "outside the parent"):
                await asyncio.create_task(writer())

    async def test_allow_outside_atomic(self):
        """Opting out runs the child on its own connection and transaction,
        so its work survives the parent's rollback."""

        async def writer():
            async with async_acquire(allow_outside_atomic=True) as conn:
                self.assertFalse(conn.in_atomic_block)
                await TestModel.async_objects.acreate(name="independent")

        with self.assertRaises(ValueError):
            async with async_atomic():
                await TestModel.async_objects.acreate(name="rolled back")
                await asyncio.create_task(writer())
                raise ValueError("boom")

        self.assertEqual(await names(), ["independent"])

    async def test_on_commit(self):
        called = []

        async with async_acquire() as conn:
            async with async_atomic(using=conn.alias):
                await TestModel.async_objects.acreate(name="c")
                await conn.on_commit(lambda: called.append("done"))
                self.assertEqual(called, [])

        self.assertEqual(called, ["done"])

    async def test_on_commit_in_child_task(self):
        called = []

        async def writer():
            async with async_acquire() as conn:
                async with async_atomic(using=conn.alias):
                    await TestModel.async_objects.acreate(name="c")
                    await conn.on_commit(lambda: called.append("done"))

        await asyncio.gather(writer())

        self.assertEqual(called, ["done"])

    async def test_unknown_alias(self):
        with self.assertRaises(ConnectionDoesNotExist):
            async with async_acquire("nope"):
                pass
