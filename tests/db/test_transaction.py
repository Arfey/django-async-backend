import asyncio
import contextlib
import sys

from django.db import (
    DEFAULT_DB_ALIAS,
    DatabaseError,
    Error,
    IntegrityError,
    transaction,
)

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import (
    async_atomic,
    async_mark_for_rollback_on_error,
)
from django_async_backend.test import AsyncioTransactionTestCase


async def create_table():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE reporter_table_tmp (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            );
            """
        )


async def drop_table():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("DROP TABLE reporter_table_tmp;")

    await async_connections[DEFAULT_DB_ALIAS].close()


async def create_instance(id):
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO reporter_table_tmp (name) VALUES ('{id}');"
        )

        return str(id)


async def get_all():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        res = await cursor.execute(
            "SELECT name FROM reporter_table_tmp order by name;"
        )

        return [i[0] for i in await res.fetchall()]


class AsyncAtomicDurableTests(AsyncioTransactionTestCase):

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_durable_atomic_commit(self):
        async with async_atomic(durable=True):
            await create_instance(1)

        self.assertEqual(await get_all(), ["1"])

    async def test_durable_rollback(self):
        try:
            async with async_atomic(durable=True):
                await create_instance(1)
                raise Exception
        except Exception:
            pass

        self.assertEqual(await get_all(), [])

    async def test_durable_nested_outer(self):
        async with async_atomic(durable=True):
            await create_instance(1)

            async with async_atomic():
                await create_instance(2)

        self.assertEqual(await get_all(), ["1", "2"])

    async def test_durable_nested_both(self):
        msg = (
            "A durable atomic block cannot be nested within another "
            "atomic block."
        )

        async with async_atomic(durable=True):
            with self.assertRaisesRegex(RuntimeError, msg):
                async with async_atomic(durable=True):
                    pass

    async def test_durable_nested_inner(self):
        msg = (
            "A durable atomic block cannot be nested within another "
            "atomic block."
        )

        async with async_atomic():
            with self.assertRaisesRegex(RuntimeError, msg):
                async with async_atomic(durable=True):
                    pass

    async def test_durable_sequence(self):
        async with async_atomic(durable=True):
            await create_instance(1)

        self.assertEqual(await get_all(), ["1"])

        async with async_atomic(durable=True):
            await create_instance(2)

        self.assertEqual(await get_all(), ["1", "2"])


class AsyncAtomicTests(AsyncioTransactionTestCase):

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_decorator_syntax_commit(self):
        @async_atomic
        async def make_reporter():
            return await create_instance(1)

        reporter = await make_reporter()
        self.assertSequenceEqual(await get_all(), [reporter])

    async def test_decorator_syntax_rollback(self):
        @async_atomic
        async def make_reporter():
            await create_instance(1)
            raise Exception("Oops, that's his last name")

        with self.assertRaisesRegex(Exception, "Oops"):
            await make_reporter()
        self.assertSequenceEqual(await get_all(), [])

    async def test_alternate_decorator_syntax_commit(self):
        @async_atomic()
        async def make_reporter():
            return await create_instance(1)

        reporter = await make_reporter()
        self.assertSequenceEqual(await get_all(), [reporter])

    async def test_alternate_decorator_syntax_rollback(self):
        @async_atomic()
        async def make_reporter():
            await create_instance(1)
            raise Exception("Oops, that's his last name")

        with self.assertRaisesRegex(Exception, "Oops"):
            await make_reporter()

        self.assertSequenceEqual(await get_all(), [])

    async def test_commit(self):
        async with async_atomic():
            reporter = await create_instance(1)
        self.assertSequenceEqual(await get_all(), [reporter])

    async def test_rollback(self):
        with self.assertRaisesRegex(Exception, "Oops"):
            async with async_atomic():
                await create_instance(1)
                raise Exception("Oops, that's his last name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_nested_commit_commit(self):
        async with async_atomic():
            reporter1 = await create_instance(1)
            async with async_atomic():
                reporter2 = await create_instance(2)
        self.assertSequenceEqual(await get_all(), [reporter1, reporter2])

    async def test_nested_commit_rollback(self):
        async with async_atomic():
            reporter = await create_instance(1)
            with self.assertRaisesRegex(Exception, "Oops"):
                async with async_atomic():
                    await create_instance(2)
                    raise Exception("Oops, that's his last name")
        self.assertSequenceEqual(await get_all(), [reporter])

    async def test_nested_rollback_commit(self):
        with self.assertRaisesRegex(Exception, "Oops"):
            async with async_atomic():
                await create_instance(1)
                async with async_atomic():
                    await create_instance(2)
                raise Exception("Oops, that's his first name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_nested_rollback_rollback(self):
        with self.assertRaisesRegex(Exception, "Oops"):
            async with async_atomic():
                await create_instance(1)
                with self.assertRaisesRegex(Exception, "Oops"):
                    async with async_atomic():
                        await create_instance(2)
                    raise Exception("Oops, that's his last name")
                raise Exception("Oops, that's his first name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_merged_commit_commit(self):
        async with async_atomic():
            reporter1 = await create_instance(1)
            async with async_atomic(savepoint=False):
                reporter2 = await create_instance(2)
        self.assertSequenceEqual(await get_all(), [reporter1, reporter2])

    async def test_merged_commit_rollback(self):
        async with async_atomic():
            await create_instance(1)
            with self.assertRaisesRegex(Exception, "Oops"):
                async with async_atomic(savepoint=False):
                    await create_instance(2)
                    raise Exception("Oops, that's his last name")
        # Writes in the outer block are rolled back too.
        self.assertSequenceEqual(await get_all(), [])

    async def test_merged_rollback_commit(self):
        with self.assertRaisesRegex(Exception, "Oops"):
            async with async_atomic():
                await create_instance(1)
                async with async_atomic(savepoint=False):
                    await create_instance(2)
                raise Exception("Oops, that's his first name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_merged_rollback_rollback(self):
        with self.assertRaisesRegex(Exception, "Oops"):
            async with async_atomic():
                await create_instance(1)
                with self.assertRaisesRegex(Exception, "Oops"):
                    async with async_atomic(savepoint=False):
                        await create_instance(2)
                    raise Exception("Oops, that's his last name")
                raise Exception("Oops, that's his first name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_reuse_commit_commit(self):
        atomic = async_atomic()
        async with atomic:
            reporter1 = await create_instance(1)
            async with atomic:
                reporter2 = await create_instance(2)
        self.assertSequenceEqual(await get_all(), [reporter1, reporter2])

    async def test_reuse_commit_rollback(self):
        atomic = async_atomic()
        async with atomic:
            reporter = await create_instance(1)
            with self.assertRaisesRegex(Exception, "Oops"):
                async with atomic:
                    await create_instance(2)
                    raise Exception("Oops, that's his last name")
        self.assertSequenceEqual(await get_all(), [reporter])

    async def test_reuse_rollback_commit(self):
        atomic = async_atomic()
        with self.assertRaisesRegex(Exception, "Oops"):
            async with atomic:
                await create_instance(1)
                async with atomic:
                    await create_instance(2)
                raise Exception("Oops, that's his first name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_reuse_rollback_rollback(self):
        atomic = async_atomic()
        with self.assertRaisesRegex(Exception, "Oops"):
            async with atomic:
                await create_instance(1)
                with self.assertRaisesRegex(Exception, "Oops"):
                    async with atomic:
                        await create_instance(2)
                    raise Exception("Oops, that's his last name")
                raise Exception("Oops, that's his first name")
        self.assertSequenceEqual(await get_all(), [])

    async def test_force_rollback(self):
        async with async_atomic():
            await create_instance(1)
            # atomic block shouldn't rollback, but force it.
            self.assertFalse(
                async_connections[DEFAULT_DB_ALIAS].get_rollback()
            )
            async_connections[DEFAULT_DB_ALIAS].set_rollback(True)
        self.assertSequenceEqual(await get_all(), [])

    async def test_prevent_rollback(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        async with async_atomic():
            reporter = await create_instance(1)
            sid = await connection.savepoint()
            # trigger a database error inside an inner atomic without
            # savepoint
            with self.assertRaises(DatabaseError):
                async with async_atomic(savepoint=False):
                    async with await connection.cursor() as cursor:
                        await cursor.execute(
                            "SELECT no_such_col FROM reporter_table_tmp"
                        )
            # prevent atomic from rolling back since we're recovering
            # manually
            self.assertTrue(connection.get_rollback())
            connection.set_rollback(False)
            await connection.savepoint_rollback(sid)
        self.assertSequenceEqual(await get_all(), [reporter])

    async def test_failure_on_exit_transaction(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with async_atomic():
            try:
                with self.assertRaises(DatabaseError):
                    async with async_atomic():
                        await create_instance(1)
                        self.assertEqual(len(await get_all()), 1)
                        # Incorrect savepoint id to provoke a database
                        # error.
                        connection.savepoint_ids.append("12")

                with self.assertRaises(transaction.TransactionManagementError):
                    await get_all()
                self.assertIs(connection.needs_rollback, True)
            finally:
                if connection.savepoint_ids:
                    connection.savepoint_ids.pop()
        self.assertSequenceEqual(await get_all(), [])


class AsyncAtomicInsideTransactionTests(AsyncAtomicTests):
    """
    All basic tests for atomic should also pass within an existing
    transaction.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.atomic = async_atomic()
        await self.atomic.__aenter__()

    async def asyncTearDown(self):
        await self.atomic.__aexit__(*sys.exc_info())
        await super().asyncTearDown()


class AsyncAtomicWithoutAutocommitTests(AsyncAtomicTests):
    """
    All basic tests for atomic should also pass when autocommit is turned
    off.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()

        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.set_autocommit(False)

    async def asyncTearDown(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.rollback()
        await connection.set_autocommit(True)
        await super().asyncTearDown()


class AsyncAtomicMergeTests(AsyncioTransactionTestCase):
    """Test merging transactions with savepoint=False."""

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_merged_outer_rollback(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with async_atomic():
            await create_instance(1)
            async with async_atomic(savepoint=False):
                await create_instance(2)
                with self.assertRaisesRegex(Exception, "Oops"):
                    async with async_atomic(savepoint=False):
                        await create_instance(3)
                        raise Exception("Oops, that's his last name")
                # The third insert couldn't be roll back. Temporarily mark the
                # connection as not needing rollback to check it.
                self.assertTrue(connection.get_rollback())
                connection.set_rollback(False)
                self.assertEqual(len(await get_all()), 3)
                connection.set_rollback(True)
            # The second insert couldn't be roll back. Temporarily mark the
            # connection as not needing rollback to check it.
            self.assertTrue(connection.get_rollback())
            connection.set_rollback(False)
            self.assertEqual(len(await get_all()), 3)
            connection.set_rollback(True)
        # The first block has a savepoint and must roll back.
        self.assertSequenceEqual(await get_all(), [])

    async def test_merged_inner_savepoint_rollback(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with async_atomic():
            reporter = await create_instance(1)
            async with async_atomic():
                await create_instance(2)
                with self.assertRaisesRegex(Exception, "Oops"):
                    async with async_atomic(savepoint=False):
                        await create_instance(3)
                        raise Exception("Oops, that's his last name")
                # The third insert couldn't be roll back. Temporarily mark the
                # connection as not needing rollback to check it.
                self.assertTrue(connection.get_rollback())
                connection.set_rollback(False)
                self.assertEqual(len(await get_all()), 3)
                connection.set_rollback(True)
            # The second block has a savepoint and must roll back.
            self.assertEqual(len(await get_all()), 1)
        self.assertSequenceEqual(await get_all(), [reporter])


class AsyncAtomicErrorsTests(AsyncioTransactionTestCase):
    forbidden_atomic_msg = (
        "This is forbidden when an 'atomic' block is active."
    )

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_atomic_prevents_setting_autocommit(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        autocommit = await connection.get_autocommit()
        async with async_atomic():
            with self.assertRaisesRegex(
                transaction.TransactionManagementError,
                self.forbidden_atomic_msg,
            ):
                await connection.set_autocommit(not autocommit)

        # Make sure autocommit wasn't changed.
        self.assertEqual(await connection.get_autocommit(), autocommit)

    async def test_atomic_prevents_calling_transaction_methods(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with async_atomic():
            with self.assertRaisesRegex(
                transaction.TransactionManagementError,
                self.forbidden_atomic_msg,
            ):
                await connection.commit()
            with self.assertRaisesRegex(
                transaction.TransactionManagementError,
                self.forbidden_atomic_msg,
            ):
                await connection.rollback()

    async def test_atomic_prevents_queries_in_broken_transaction(self):
        await create_instance(1)

        async with async_atomic():
            with self.assertRaises(IntegrityError):
                async with async_mark_for_rollback_on_error(DEFAULT_DB_ALIAS):
                    await create_instance(1)

            # The transaction is marked as needing rollback.
            msg = (
                "An error occurred in the current transaction. You can't "
                "execute queries until the end of the 'atomic' block."
            )
            with self.assertRaisesRegex(
                transaction.TransactionManagementError, msg
            ) as cm:
                await create_instance(2)

            self.assertIsInstance(cm.exception.__cause__, IntegrityError)
        self.assertEqual(len(await get_all()), 1)

    async def test_atomic_prevents_queries_in_broken_transaction_after_client_close(  # noqa
        self,
    ):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with async_atomic():
            await create_instance(1)
            await connection.close()
            # The connection is closed and the transaction is marked as
            # needing rollback. This will raise an InterfaceError on databases
            # that refuse to create cursors on closed connections (PostgreSQL)
            # and a TransactionManagementError on other databases.
            with self.assertRaises(Error):
                await create_instance(2)
        # The connection is usable again .
        self.assertEqual(len(await get_all()), 0)


class NonAsyncAutocommitTests(AsyncioTransactionTestCase):
    async def asyncSetUp(self):
        await create_table()

        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.set_autocommit(False)

    async def asyncTearDown(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.rollback()
        await connection.set_autocommit(True)

        await drop_table()

    async def test_orm_query_after_error_and_rollback(self):
        """
        ORM queries are allowed after an error and a rollback in non-autocommit
        mode (#27504).
        """
        connection = async_connections[DEFAULT_DB_ALIAS]

        await create_instance(1)
        with self.assertRaises(IntegrityError):
            await create_instance(1)

        await connection.rollback()
        await get_all()

    async def test_orm_query_without_autocommit(self):
        """
        #24921 -- ORM queries must be possible after set_autocommit(False).
        """
        await create_instance(1)


class IndependentConnectionTransaction(AsyncioTransactionTestCase):
    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_nested_independent_connection(self):
        async with async_connections._independent_connection():
            await create_instance(1)

            self.assertEqual(len(await get_all()), 1)

            async with async_connections._independent_connection():
                await create_instance(2)

                self.assertEqual(len(await get_all()), 2)

    async def test_nested_independent_connection_with_transaction(self):
        async with async_connections._independent_connection():
            async with async_atomic():
                await create_instance(1)

                self.assertEqual(len(await get_all()), 1)

                async with async_connections._independent_connection():
                    await create_instance(2)

                    self.assertEqual(len(await get_all()), 1)

    async def test_nested_independent_connection_with_nested_transaction(self):
        async with async_connections._independent_connection():
            async with async_atomic():
                await create_instance(1)

                self.assertEqual(len(await get_all()), 1)

                async with async_connections._independent_connection():
                    async with async_atomic():
                        await create_instance(2)

                        self.assertEqual(len(await get_all()), 1)


class ConcurrentAsyncAtomicTests(AsyncioTransactionTestCase):
    """
    Child tasks that inherit a parent's connection via ContextVar can
    corrupt transaction state or silently lose writes by sharing the
    same psycopg async connection. validate_task_sharing on every
    cursor/commit/rollback rejects the sharing with DatabaseError
    (unless explicitly allowed via inc_task_sharing).

    Correct patterns:
      - Use _independent_connection() per task
      - inc_task_sharing() around code that deliberately shares
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    @contextlib.asynccontextmanager
    async def _strict_task_sharing(self, conn):
        """Temporarily disable task sharing the test framework enabled.

        AsyncioTransactionTestCase inc_task_sharing for the whole test
        lifecycle so setUp and test methods (which run in different
        tasks) can share one connection. To exercise validate_task_sharing
        itself we need to turn that off for the scope of the assertion.
        """
        conn.dec_task_sharing()
        try:
            yield
        finally:
            conn.inc_task_sharing()

    async def test_child_task_cursor_raises(self):
        """A child task using the parent's connection raises."""
        conn = async_connections[DEFAULT_DB_ALIAS]
        async with self._strict_task_sharing(conn):
            async def writer():
                await create_instance("should_fail")

            with self.assertRaisesRegex(DatabaseError, "same task"):
                await asyncio.create_task(writer())
        self.assertEqual(await get_all(), [])

    async def test_cross_task_transaction_inheritance_raises(self):
        """Cross-task connection use with one task in atomic raises.

        Reproduces the bug pattern: main task issues raw SQL while an
        audit task has opened an async_atomic. Without per-query
        validation this is silent data loss; with validation the
        interloping cursor use raises DatabaseError and — crucially —
        nothing is written to the database.
        """
        conn = async_connections[DEFAULT_DB_ALIAS]
        async with self._strict_task_sharing(conn):
            async def main_task():
                await create_instance("main")

            async def audit_task():
                try:
                    async with async_atomic():
                        await create_instance("audit")
                        raise RuntimeError("rollback")
                except RuntimeError:
                    pass

            results = await asyncio.gather(
                main_task(), audit_task(), return_exceptions=True
            )
            errors = [r for r in results if isinstance(r, DatabaseError)]
            self.assertTrue(
                len(errors) > 0,
                "Expected DatabaseError from cross-task connection use",
            )
        # The real regression guard: nothing got committed.
        self.assertEqual(await get_all(), [])

    async def test_cross_task_set_autocommit_raises(self):
        """Cross-task set_autocommit raises (class C: session state)."""
        conn = async_connections[DEFAULT_DB_ALIAS]
        async with self._strict_task_sharing(conn):
            async def toggle():
                await conn.set_autocommit(False)

            with self.assertRaisesRegex(DatabaseError, "same task"):
                await asyncio.create_task(toggle())

    async def test_inc_task_sharing_allows_cross_task_use(self):
        """inc_task_sharing is the documented escape hatch."""
        conn = async_connections[DEFAULT_DB_ALIAS]
        # Test framework already has it inc'd; verify cross-task use
        # actually works rather than relying on implicit state.

        async def writer():
            await create_instance("shared")

        await asyncio.create_task(writer())
        self.assertEqual(await get_all(), ["shared"])

    async def test_dec_task_sharing_below_zero_raises(self):
        """Refcount cannot be decremented below zero."""
        conn = async_connections[DEFAULT_DB_ALIAS]
        async with self._strict_task_sharing(conn):
            # Sharing count is now 0; another dec should raise.
            with self.assertRaisesRegex(RuntimeError, "below zero"):
                conn.dec_task_sharing()

    async def test_nested_savepoint_same_task_works(self):
        """Nested async_atomic() in the same task is a savepoint."""
        async with async_atomic():
            await create_instance("outer")
            async with async_atomic():
                await create_instance("inner")

        self.assertEqual(len(await get_all()), 2)

    async def test_sequential_atomic_same_task_works(self):
        """Sequential async_atomic() calls in the same task work."""
        async with async_atomic():
            await create_instance("first")
        async with async_atomic():
            await create_instance("second")

        self.assertEqual(len(await get_all()), 2)

    async def test_parent_atomic_avoids_corruption(self):
        """Single parent async_atomic wrapping gather() works."""
        barrier = asyncio.Barrier(10)

        async def writer(task_id):
            await barrier.wait()
            await create_instance(f"p{task_id}")
            await asyncio.sleep(0)

        async with async_atomic():
            results = await asyncio.gather(
                *(writer(i) for i in range(10)), return_exceptions=True
            )

        self.assertEqual([r for r in results if isinstance(r, Exception)], [])
        self.assertEqual(len(await get_all()), 10)

    async def test_independent_connection_avoids_corruption(self):
        """_independent_connection() per task avoids corruption."""
        barrier = asyncio.Barrier(10)

        async def writer(task_id):
            await barrier.wait()
            async with async_connections._independent_connection():
                async with async_atomic():
                    await create_instance(f"i{task_id}")
                    await asyncio.sleep(0)

        results = await asyncio.gather(
            *(writer(i) for i in range(10)), return_exceptions=True
        )

        self.assertEqual([r for r in results if isinstance(r, Exception)], [])
        self.assertEqual(len(await get_all()), 10)
