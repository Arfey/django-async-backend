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
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE reporter_table_tmp (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            );
            """
        )


async def drop_table():
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("DROP TABLE reporter_table_tmp;")

    await async_connections[DEFAULT_DB_ALIAS].close()


async def create_instance(id):
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO reporter_table_tmp (name) VALUES ('{id}');"
        )

        return str(id)


async def get_all():
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
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
                    async with connection.cursor() as cursor:
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
