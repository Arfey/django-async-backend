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
    """REGRESSION (currently fails on purpose).

    Demonstrates that ``AsyncioRollbackTestCase`` cannot model
    cross-pool transaction semantics — the answers it gives for tests
    that mix ``async_atomic`` and ``transaction.atomic`` differ from
    what a real deployment of django-async-backend would observe.

    What the bridge actually does
    -----------------------------

    ``AsyncioRollbackTestCase`` replaces ``async_connections[alias]``
    with a proxy that owns no real psycopg async connection. Every
    cursor / savepoint call the proxy receives is dispatched, via
    ``sync_to_async``, onto the same ``psycopg.Connection`` that
    Django's ``TestCase`` opened a transaction on. The bridge also
    swaps Django's ``connections._connections`` storage to a
    thread-blind container pointing at that same wrapper. End result:
    code that *thinks* it's on two separate connections is actually
    sharing one TCP socket, one server-side backend, and one
    transaction state.

    What a real deployment looks like
    ---------------------------------

    Outside of a test, ``async_connections[alias]`` opens (or pulls
    from a pool) its own ``psycopg.AsyncConnection``. ``connections[alias]``
    opens a separate ``psycopg.Connection``. They are two distinct
    connections to Postgres with two distinct backends and two
    independent transaction stacks. A SAVEPOINT issued from
    ``async_atomic()`` lives on connection A; a SAVEPOINT issued from
    ``transaction.atomic()`` lives on connection B. Neither sees the
    other's savepoint stack.

    The shape this test exercises
    -----------------------------

    Outer ``async with async_atomic():`` wraps an inner
    ``sync_to_async(_sync_atomic_insert)``, where the inner block
    runs ``with transaction.atomic(): cursor.execute(INSERT)`` and
    cleanly exits. The outer block then raises and triggers a
    rollback.

    In a real deployment, the inner sync atomic opens a savepoint on
    the **sync** connection, runs the INSERT, and releases the
    savepoint — which durably commits the row to the sync
    connection's transaction (autocommit by default for sync code
    outside an explicit ``transaction.atomic`` chain at the request
    level). The outer ``async_atomic()``'s rollback then issues
    ROLLBACK TO SAVEPOINT on the **async** connection, which has zero
    effect on the sync connection. A subsequent SELECT — from any
    connection — sees the row. That is the answer this test asserts:
    ``count == 1``.

    Under the bridge, the two atomics share one connection. The PG
    savepoint stack mid-test ends up roughly:

        [TestCase_class_savepoint,
         async_atomic_savepoint,
         transaction.atomic_savepoint,
         INSERT,
         RELEASE transaction.atomic_savepoint]

    Then the outer block raises, the bridge issues
    ``ROLLBACK TO SAVEPOINT async_atomic_savepoint``, and the INSERT
    — which sits underneath that savepoint in PG's stack — is
    reverted along with it. Bridge result: ``count == 0``.

    The test asserts ``1`` (the deployed-app answer) and gets ``0``
    (the bridge's), so it fails. The failure is not a bug in the
    bridge implementation; it's a property of collapsing two
    connections into one. No bookkeeping change inside the proxy can
    fix it because Postgres still only sees one transaction stack.
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

        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
            await c.execute(
                "SELECT count(*) FROM test_model "
                "WHERE name = 'mixed-atomic-sync'"
            )
            row = await c.fetchone()
            self.assertEqual(
                row[0],
                1,
                "Bridge collapses two physical connections into one, so "
                "the outer async_atomic ROLLBACK undoes a sync atomic "
                "block that had already RELEASEd its own savepoint. A "
                "real deployment has separate connections, the inner "
                "sync atomic commits independently, and the row persists.",
            )


class BridgeFlattersDoesNotMatchDeployedApp(AsyncioRollbackTestCase):
    """REGRESSION (currently passes — and that's the problem).

    The inverse shape of ``CrossPoolAtomicSemanticsDivergence``: a
    test that asserts the **bridge's** answer (rather than the
    deployed-app answer) is GREEN under ``AsyncioRollbackTestCase``,
    even though identical code shipped to a real deployment of
    django-async-backend would observe the opposite outcome.

    Same scenario as the previous test — outer ``async_atomic``
    wrapping an inner sync ``transaction.atomic`` insert via
    ``sync_to_async``, with the outer block raising — but the
    assertion encodes ``count == 0`` (what the bridge reports)
    instead of ``count == 1`` (what a real deployment reports).

    A test author who has never read the bridge's implementation
    notes can reasonably look at this code, run it under
    ``AsyncioRollbackTestCase``, see ``row[0] == 0``, codify that as
    the expected behaviour, and watch CI go green. The same code in
    production, however, has the inner sync atomic on a separate
    physical connection — committing the row before the outer
    rollback runs — so the production observation is ``row[0] == 1``.

    The framework cannot tell apart these two intents:

      * "I'm testing that my application correctly relies on
        ``async_atomic`` rolling back work that happened underneath
        it." (bridge gives the right answer if everything underneath
        was async)
      * "I'm testing that my application correctly handles cross-pool
        isolation, where sync writes persist past async rollbacks."
        (bridge gives the wrong answer; deployed app behaviour is
        opposite)

    Both are legitimate uses of an integration test. The bridge's
    implementation silently picks the first interpretation —
    everything looks rolled-back because everything ran on one
    connection — and that interpretation can be wrong without any
    visible signal at test-write time.

    This test is the artifact of that ambiguity: green CI on a code
    path whose deployed behaviour the framework hasn't actually
    verified. Worth deciding at the framework-contract level whether
    to (a) detect mixed-style atomics on a single proxied connection
    and raise, (b) ship a companion test base that uses real async +
    sync connections for code paths whose semantics depend on the
    two-pool distinction, or (c) document the constraint and rely on
    adopters to never mix the styles.
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
            # Asserts the **bridge**'s answer. Currently green under
            # AsyncioRollbackTestCase. The same code shipped to a real
            # deployment of django-async-backend would observe
            # ``row[0] == 1`` because the inner sync atomic commits
            # independently on the sync connection.
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
