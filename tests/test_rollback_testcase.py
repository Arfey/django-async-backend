"""
Tests for AsyncioRollbackTestCase.

Covers four shapes:
  1. async_connections write inside a plain Django TestCase leaks past rollback
     (the gap that motivated the new class).
  2. async_connections write inside AsyncioRollbackTestCase is rolled back.
  3. Sync ORM write + async_connections write in the same test both roll back
     and see each other's uncommitted data.
  4. async_atomic creates a savepoint and rolls back independently of the
     outer test transaction.
"""
import asyncio

from django.db import DEFAULT_DB_ALIAS, connection
from django.test import TestCase

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import AsyncioRollbackTestCase


class StandardTestCaseLeaksAsyncWrites(TestCase):
    """A plain Django TestCase does NOT cover async_connections writes.

    This is the gap. The test inserts a row via async_connections inside a
    TestCase, the TestCase rolls back at end-of-test, and the row is still
    in the database afterward. tearDownClass cleans it up so we don't
    poison other tests.
    """

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        # Run after the class transaction is closed; touches the real table.
        with connection.cursor() as c:
            c.execute("DELETE FROM test_model WHERE name LIKE 'leak-demo-%'")
        # Tear down any async connections opened by this test so the test
        # DB can be dropped at suite teardown.
        async def _close_async():
            for alias in async_connections.settings.keys():
                await async_connections[alias].close()

        asyncio.run(_close_async())

    def test_async_write_leaks_past_testcase_rollback(self):
        async def _insert():
            async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
                await c.execute(
                    "INSERT INTO test_model (name) VALUES ('leak-demo-1')"
                )
            # Async pool defaults to autocommit=True, so the row commits.
            await async_connections[DEFAULT_DB_ALIAS].close()

        asyncio.run(_insert())

        # Verify the row is committed-and-visible from a fresh async session.
        # If async_connections shared a transaction with the sync conn, the
        # row would NOT have committed and a fresh session would see 0.
        async def _count():
            try:
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as c:
                    await c.execute(
                        "SELECT count(*) FROM test_model "
                        "WHERE name = 'leak-demo-1'"
                    )
                    row = await c.fetchone()
                    return row[0]
            finally:
                await async_connections[DEFAULT_DB_ALIAS].close()

        self.assertEqual(asyncio.run(_count()), 1)


class RollbackCoversAsyncWrites(AsyncioRollbackTestCase):
    """AsyncioRollbackTestCase routes async writes through the sync conn,
    so the TestCase rollback covers them."""

    async def test_async_write_visible_then_rolled_back(self):
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "INSERT INTO test_model (name) VALUES ('rollback-demo-1')"
            )
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'rollback-demo-1'"
            )
            row = await c.fetchone()
            self.assertEqual(row[0], 1)


class RollbackCoversNothingPersists(AsyncioRollbackTestCase):
    """Run after RollbackCoversAsyncWrites; verifies the prior test's row
    did not leak into this test's view."""

    async def test_no_rows_present(self):
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'rollback-demo-1'"
            )
            row = await c.fetchone()
            self.assertEqual(row[0], 0)


class MixedSyncAndAsyncWrites(AsyncioRollbackTestCase):
    """Sync write via Django connection + async raw SQL share the test
    transaction. Each side sees the other's uncommitted writes; both
    roll back together."""

    async def test_sync_then_async_visible_to_each_other(self):
        from asgiref.sync import sync_to_async

        def _sync_write():
            with connection.cursor() as cur:
                cur.execute(
                    "INSERT INTO test_model (name) VALUES ('from-sync')"
                )

        await sync_to_async(_sync_write)()

        # Async raw insert (goes to the same sync conn via the bridge).
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "INSERT INTO test_model (name) VALUES ('from-async')"
            )

        # Both rows visible from the async side.
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name IN ('from-sync', 'from-async')"
            )
            row = await c.fetchone()
            self.assertEqual(row[0], 2)

        # And from the sync side.
        def _sync_count():
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM test_model "
                    "WHERE name IN ('from-sync', 'from-async')"
                )
                return cur.fetchone()[0]

        self.assertEqual(await sync_to_async(_sync_count)(), 2)


class AsyncAtomicSavepointBehavior(AsyncioRollbackTestCase):
    """async_atomic should create a savepoint inside the TestCase
    transaction and roll back independently."""

    async def test_async_atomic_rollback_only_affects_inner_block(self):
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "INSERT INTO test_model (name) VALUES ('outer-row')"
            )

        try:
            async with async_atomic():
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as c:
                    await c.execute(
                        "INSERT INTO test_model (name) "
                        "VALUES ('inner-row')"
                    )
                raise RuntimeError("boom")
        except RuntimeError:
            pass

        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT name FROM test_model "
                "WHERE name IN ('outer-row', 'inner-row') "
                "ORDER BY name"
            )
            rows = await c.fetchall()
            self.assertEqual([r[0] for r in rows], ["outer-row"])

    async def test_async_atomic_commit_keeps_inner_writes(self):
        async with async_atomic():
            async with await async_connections[
                DEFAULT_DB_ALIAS
            ].cursor() as c:
                await c.execute(
                    "INSERT INTO test_model (name) VALUES ('committed-inner')"
                )

        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'committed-inner'"
            )
            row = await c.fetchone()
            self.assertEqual(row[0], 1)


class SetUpTestDataIsVisibleFromAsyncSide(AsyncioRollbackTestCase):
    """setUpTestData runs in TestCase's outer transaction; async reads via
    the bridge should see those rows."""

    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cur:
            cur.execute(
                "INSERT INTO test_model (name) VALUES ('seeded-by-setup')"
            )

    async def test_seeded_row_visible_from_async(self):
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'seeded-by-setup'"
            )
            row = await c.fetchone()
            self.assertEqual(row[0], 1)
