"""
Tests for concurrent async task safety.

Demonstrates the INTRANS bug: child tasks sharing a connection via
ContextVar inheritance each open their own async_atomic(), creating
overlapping savepoints that corrupt transaction state.

    ProgrammingError: can't change 'autocommit' now:
        connection in transaction status INTRANS

Correct patterns:
  1. Single parent async_atomic() wrapping asyncio.gather()
  2. _independent_connection() for per-task isolation

See: https://github.com/Arfey/django-async-backend/issues/11
"""

import asyncio

from django.db import DEFAULT_DB_ALIAS

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import AsyncioTransactionTestCase

N_TASKS = 10


async def create_table():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            "DROP TABLE IF EXISTS concurrency_test;"
        )
        await cursor.execute(
            """
            CREATE TABLE concurrency_test (
                id SERIAL PRIMARY KEY,
                task_id INTEGER NOT NULL
            );
            """
        )


async def drop_table():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS concurrency_test;")

    await async_connections[DEFAULT_DB_ALIAS].close()


async def count_rows():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        res = await cursor.execute(
            "SELECT COUNT(*) FROM concurrency_test;"
        )
        row = await res.fetchone()
        return row[0]


class PerTaskAtomicBugTests(AsyncioTransactionTestCase):
    """
    Proves the INTRANS bug: concurrent child tasks each opening their
    own async_atomic() on a shared connection corrupt transaction state.

    This is the pattern from issue #11. Child tasks inherit the parent's
    connection via ContextVar. When each child calls async_atomic(),
    they create overlapping savepoints on the same connection, leading to:

        ProgrammingError: can't change 'autocommit' now:
            connection in transaction status INTRANS
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_per_task_atomic_causes_intrans(self):
        """Each child task opening async_atomic() triggers INTRANS.

        Run multiple rounds to defeat timing flakiness — the bug
        depends on task interleaving so a single round can
        occasionally succeed by luck.
        """
        rounds = 5
        all_errors = []

        for _ in range(rounds):
            barrier = asyncio.Barrier(N_TASKS)
            errors = []

            async def writer(task_id):
                try:
                    await barrier.wait()
                    async with async_atomic():
                        async with await async_connections[
                            DEFAULT_DB_ALIAS
                        ].cursor() as cursor:
                            await cursor.execute(
                                "INSERT INTO concurrency_test "
                                "(task_id) VALUES (%s);",
                                [task_id],
                            )
                        await asyncio.sleep(0)
                except Exception as e:
                    errors.append((task_id, e))

            await asyncio.gather(
                *(writer(i) for i in range(N_TASKS))
            )
            all_errors.extend(errors)
            if errors:
                break  # Bug triggered, no need for more rounds

        self.assertTrue(
            len(all_errors) > 0,
            f"Expected INTRANS errors from overlapping per-task "
            f"async_atomic() on shared connection, but got none "
            f"after {rounds} rounds. "
            f"If this fails, the bug may be fixed upstream.",
        )


class ParentAtomicPatternTests(AsyncioTransactionTestCase):
    """
    Correct pattern: single parent async_atomic() wrapping
    asyncio.gather(). Child tasks write on the parent's transaction.
    No per-task savepoints, no INTRANS.
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_parent_atomic_with_child_writes(self):
        """Parent opens transaction, child tasks write. All succeed."""
        barrier = asyncio.Barrier(N_TASKS)
        errors = []

        async def writer(task_id):
            try:
                await barrier.wait()
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO concurrency_test (task_id) "
                        "VALUES (%s);",
                        [task_id],
                    )
                await asyncio.sleep(0)
            except Exception as e:
                errors.append((task_id, e))

        async with async_atomic():
            await asyncio.gather(*(writer(i) for i in range(N_TASKS)))

        self.assertEqual(
            errors,
            [],
            f"{len(errors)}/{N_TASKS} tasks failed: "
            + "; ".join(f"task {tid}: {e}" for tid, e in errors),
        )

        row_count = await count_rows()
        self.assertEqual(row_count, N_TASKS)


class IndependentConnectionTests(AsyncioTransactionTestCase):
    """
    Correct pattern: _independent_connection() gives each child task
    its own connection, so per-task async_atomic() works safely.
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_independent_connections_with_per_task_atomic(self):
        """Each task gets its own connection via _independent_connection."""
        barrier = asyncio.Barrier(N_TASKS)
        errors = []

        async def writer(task_id):
            try:
                await barrier.wait()
                async with async_connections._independent_connection():
                    async with async_atomic():
                        async with await async_connections[
                            DEFAULT_DB_ALIAS
                        ].cursor() as cursor:
                            await cursor.execute(
                                "INSERT INTO concurrency_test "
                                "(task_id) VALUES (%s);",
                                [task_id],
                            )
                        await asyncio.sleep(0)
            except Exception as e:
                errors.append((task_id, e))

        await asyncio.gather(*(writer(i) for i in range(N_TASKS)))

        self.assertEqual(
            errors,
            [],
            f"{len(errors)}/{N_TASKS} tasks failed: "
            + "; ".join(f"task {tid}: {e}" for tid, e in errors),
        )

        row_count = await count_rows()
        self.assertEqual(row_count, N_TASKS)

    async def test_independent_connection_is_different_object(self):
        """_independent_connection provides a distinct connection."""
        parent_conn = async_connections[DEFAULT_DB_ALIAS]
        await parent_conn.ensure_connection()
        parent_id = id(parent_conn)

        child_ids = []

        async def child():
            async with async_connections._independent_connection():
                conn = async_connections[DEFAULT_DB_ALIAS]
                await conn.ensure_connection()
                child_ids.append(id(conn))

        task = asyncio.create_task(child())
        await task

        self.assertEqual(len(child_ids), 1)
        self.assertNotEqual(
            parent_id,
            child_ids[0],
            "_independent_connection should provide a "
            "different connection",
        )
