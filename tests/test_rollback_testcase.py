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


class CrossPoolAtomicSemanticsDivergence(AsyncioRollbackTestCase):
    """REGRESSION (currently fails): the bridge gives different
    cross-pool transaction semantics than production.

    In production:
    - ``async_atomic()`` operates on the async psycopg connection.
    - ``sync_to_async(transaction.atomic)`` operates on Django's sync
      psycopg connection.
    - Two physical connections, two independent transactions. A sync
      atomic block that commits its savepoint and a surrounding async
      atomic that rolls back: sync write **persists**.

    Under the bridge:
    - ``async_connections[alias]`` is a proxy onto the sync conn.
    - Both atomics write SAVEPOINT/RELEASE/ROLLBACK to the same
      backend, but each tracks its own savepoint counter.
    - The inner sync atomic's RELEASE doesn't tell the proxy
      anything; the outer async atomic rolls back to its own
      savepoint, which sits *under* the inner one in PG's stack —
      undoing the inner sync write too.

    This is the gap Arfey called out: the bridge asks tests to assert
    on a behaviour the framework can't actually replicate, because two
    state machines now share one connection. Document the limitation
    here as a failing test until the bridge either grows
    cross-state-machine coordination or the framework explicitly
    forbids the mix.
    """

    async def test_inner_sync_atomic_persists_through_outer_async_rollback(self):
        from asgiref.sync import sync_to_async
        from django.db import transaction

        def _sync_atomic_insert():
            with transaction.atomic():
                with connection.cursor() as cur:
                    cur.execute(
                        "INSERT INTO test_model (name) "
                        "VALUES ('mixed-atomic-sync')"
                    )

        try:
            async with async_atomic():
                await sync_to_async(_sync_atomic_insert)()
                raise RuntimeError("trigger outer rollback")
        except RuntimeError:
            pass

        # Production semantics (separate physical conns): the inner
        # sync atomic committed its own savepoint on the sync conn, so
        # the row persists despite the outer async_atomic rollback.
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'mixed-atomic-sync'"
            )
            row = await c.fetchone()
            self.assertEqual(
                row[0],
                1,
                "Bridge collapses two physical connections into one, so the "
                "outer async_atomic ROLLBACK undoes a sync atomic block that "
                "had already RELEASEd its own savepoint. Production has "
                "separate conns and the sync write persists.",
            )


class BridgeFlattersDoesNotMatchProduction(AsyncioRollbackTestCase):
    """REGRESSION (currently passes — and that's the problem).

    The inverse of ``CrossPoolAtomicSemanticsDivergence``: a test that
    asserts the **bridge's** answer (rather than production's) gets a
    green light, even though shipping the same code to a real
    deployment would behave differently.

    Concretely: outer ``async_atomic`` wraps an inner sync atomic
    insert via ``sync_to_async``, then the outer block raises.

    - Under the bridge: both atomics share one psycopg conn; the
      outer ROLLBACK reverts the inner write. ``count == 0``.
    - In production: async and sync are separate physical
      connections; the inner sync atomic committed its savepoint
      before the outer raised. ``count == 1``.

    A test author who didn't know about the bridge could write
    ``assertEqual(count, 0)`` here, watch CI go green, and ship a
    code path that silently leaks rows in production. That's the
    structural risk Arfey flagged: the bridge isn't just "covers
    fewer scenarios", it actively models a different semantics.

    The framework doesn't know which answer the test author meant —
    it can't tell apart "I'm testing rollback covers everything"
    from "I'm testing cross-pool isolation". So this passing-but-wrong
    case has to be addressed at the framework contract level, not by
    individual test authors being careful.
    """

    async def test_bridge_says_inner_sync_write_was_rolled_back(self):
        from asgiref.sync import sync_to_async
        from django.db import transaction

        def _sync_atomic_insert():
            with transaction.atomic():
                with connection.cursor() as cur:
                    cur.execute(
                        "INSERT INTO test_model (name) "
                        "VALUES ('flatter-demo')"
                    )

        try:
            async with async_atomic():
                await sync_to_async(_sync_atomic_insert)()
                raise RuntimeError("trigger outer rollback")
        except RuntimeError:
            pass

        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'flatter-demo'"
            )
            row = await c.fetchone()
            # Asserts the **bridge**'s answer. This is GREEN under
            # AsyncioRollbackTestCase. Same code shipped to prod would
            # observe ``row[0] == 1`` because the inner sync atomic
            # commits independently on the sync connection.
            self.assertEqual(row[0], 0)


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
