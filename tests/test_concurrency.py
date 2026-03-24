"""
Tests for concurrent async task safety.

These tests verify that async_connections and async_atomic work correctly
when multiple asyncio tasks run concurrently on the same event loop thread.

Root cause under test: AsyncConnectionHandler uses thread_critical=True,
making connections thread-local. Since all async tasks share one event loop
thread, they share one connection object. Transaction state (in_atomic_block,
savepoint_ids, needs_rollback) gets corrupted when tasks overlap.

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


class ConcurrentAtomicWriteTests(AsyncioTransactionTestCase):
    """
    Multiple async tasks each open their own async_atomic() block and
    INSERT a row. With proper task isolation, all N tasks should succeed
    independently. With shared connections, savepoint IDs collide and
    transactions abort.
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_concurrent_atomic_writes(self):
        """N concurrent async_atomic writes should all succeed."""
        barrier = asyncio.Barrier(N_TASKS)
        errors = []

        async def writer(task_id):
            try:
                # Barrier ensures all tasks enter async_atomic before
                # any proceeds, maximizing interleaving.
                await barrier.wait()
                async with async_atomic():
                    async with await async_connections[
                        DEFAULT_DB_ALIAS
                    ].cursor() as cursor:
                        await cursor.execute(
                            "INSERT INTO concurrency_test (task_id) "
                            "VALUES (%s);",
                            [task_id],
                        )
                    # Yield to other tasks inside the atomic block.
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


class ConcurrentReadTests(AsyncioTransactionTestCase):
    """
    Multiple async tasks each run a SELECT COUNT concurrently.
    With proper task isolation, all should return the correct count.
    With shared connections, reads can see corrupted transaction state.
    """

    async def asyncSetUp(self):
        await create_table()
        # Seed rows for reading.
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            for i in range(5):
                await cursor.execute(
                    "INSERT INTO concurrency_test (task_id) VALUES (%s);",
                    [i],
                )

    async def asyncTearDown(self):
        await drop_table()

    async def test_concurrent_reads(self):
        """N concurrent reads should all return the correct count."""
        barrier = asyncio.Barrier(N_TASKS)
        results = []
        errors = []

        async def reader(task_id):
            try:
                await barrier.wait()
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as cursor:
                    res = await cursor.execute(
                        "SELECT COUNT(*) FROM concurrency_test;"
                    )
                    row = await res.fetchone()
                    results.append((task_id, row[0]))
                    await asyncio.sleep(0)
            except Exception as e:
                errors.append((task_id, e))

        await asyncio.gather(*(reader(i) for i in range(N_TASKS)))

        self.assertEqual(
            errors,
            [],
            f"{len(errors)}/{N_TASKS} readers failed: "
            + "; ".join(f"task {tid}: {e}" for tid, e in errors),
        )

        self.assertEqual(len(results), N_TASKS)
        for task_id, count in results:
            self.assertEqual(
                count,
                5,
                f"Task {task_id} got count={count}, expected 5",
            )


class ConcurrentMixedReadWriteTests(AsyncioTransactionTestCase):
    """
    Concurrent mix of writers (async_atomic + INSERT) and readers (SELECT).
    Writers should all succeed and readers should get consistent results.
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_concurrent_mixed(self):
        """Concurrent readers and writers should not interfere."""
        n_writers = 10
        n_readers = 10
        write_barrier = asyncio.Barrier(n_writers)
        read_barrier = asyncio.Barrier(n_readers)
        write_errors = []
        read_errors = []

        async def writer(task_id):
            try:
                await write_barrier.wait()
                async with async_atomic():
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
                write_errors.append((task_id, e))

        async def reader(task_id):
            try:
                await read_barrier.wait()
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as cursor:
                    res = await cursor.execute(
                        "SELECT COUNT(*) FROM concurrency_test;"
                    )
                    await res.fetchone()
                    await asyncio.sleep(0)
            except Exception as e:
                read_errors.append((task_id, e))

        tasks = [writer(i) for i in range(n_writers)]
        tasks += [reader(i) for i in range(n_readers)]
        await asyncio.gather(*tasks)

        self.assertEqual(
            write_errors,
            [],
            f"{len(write_errors)}/{n_writers} writers failed: "
            + "; ".join(f"task {tid}: {e}" for tid, e in write_errors),
        )
        self.assertEqual(
            read_errors,
            [],
            f"{len(read_errors)}/{n_readers} readers failed: "
            + "; ".join(f"task {tid}: {e}" for tid, e in read_errors),
        )

        row_count = await count_rows()
        self.assertEqual(row_count, n_writers)


class TaskIsolationTests(AsyncioTransactionTestCase):
    """
    Verify that parent and child tasks get separate connections.
    This directly exercises the _TaskAwareLocal identity check that
    detects inherited ContextVar values from asyncio.create_task().
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_parent_child_task_isolation(self):
        """Child task via create_task gets its own connection."""
        parent_conn = async_connections[DEFAULT_DB_ALIAS]
        await parent_conn.ensure_connection()
        parent_id = id(parent_conn)

        child_ids = []

        async def child():
            conn = async_connections[DEFAULT_DB_ALIAS]
            await conn.ensure_connection()
            child_ids.append(id(conn))

        task = asyncio.create_task(child())
        await task

        self.assertEqual(len(child_ids), 1)
        self.assertNotEqual(
            parent_id,
            child_ids[0],
            "Child task got the same connection as parent — "
            "task isolation is broken",
        )
