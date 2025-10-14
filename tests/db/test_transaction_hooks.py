from django.db import (
    DEFAULT_DB_ALIAS,
    transaction,
)

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import AsyncioTransactionTestCase


class ForcedError(Exception):
    pass


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

        return int(id)


async def get_all():
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        res = await cursor.execute(
            "SELECT name FROM reporter_table_tmp order by name;"
        )

        return [int(i[0]) for i in await res.fetchall()]


class TestConnectionOnCommit(AsyncioTransactionTestCase):
    """
    Tests for connection.on_commit().

    Creation/checking of database objects in parallel with
    callback tracking is to verify that the behavior of the
    two match in all tested cases.
    """

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    def setUp(self):
        self.notified = []

    def notify(self, id_):
        if id_ == "error":
            raise ForcedError()
        self.notified.append(id_)

    async def do(self, num):
        """Create a Thing instance and notify about it."""
        await create_instance(num)
        await async_connections[DEFAULT_DB_ALIAS].on_commit(
            lambda: self.notify(num)
        )

    async def assertDone(self, nums):
        self.assertNotified(nums)
        self.assertEqual(sorted(await get_all()), sorted(nums))

    def assertNotified(self, nums):
        self.assertEqual(self.notified, nums)

    async def test_executes_immediately_if_no_transaction(self):
        await self.do(1)
        await self.assertDone([1])

    async def test_robust_if_no_transaction(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        def robust_callback():
            raise ForcedError("robust callback")

        with self.assertLogs(
            "django_async_backend.db.backends", "ERROR"
        ) as cm:
            await connection.on_commit(robust_callback, robust=True)
            await self.do(1)

        await self.assertDone([1])
        log_record = cm.records[0]
        self.assertEqual(
            log_record.getMessage(),
            "Error calling TestConnectionOnCommit."
            "test_robust_if_no_transaction."
            "<locals>.robust_callback in on_commit() (robust callback).",
        )
        self.assertIsNotNone(log_record.exc_info)
        raised_exception = log_record.exc_info[1]
        self.assertIsInstance(raised_exception, ForcedError)
        self.assertEqual(str(raised_exception), "robust callback")

    async def test_robust_transaction(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        def robust_callback():
            raise ForcedError("robust callback")

        with self.assertLogs(
            "django_async_backend.db.backends", "ERROR"
        ) as cm:
            async with async_atomic():
                await connection.on_commit(robust_callback, robust=True)
                await self.do(1)

        await self.assertDone([1])
        log_record = cm.records[0]
        self.assertEqual(
            log_record.getMessage(),
            "Error calling TestConnectionOnCommit."
            "test_robust_transaction.<locals>."
            "robust_callback in on_commit() during"
            " transaction (robust callback).",
        )
        self.assertIsNotNone(log_record.exc_info)
        raised_exception = log_record.exc_info[1]
        self.assertIsInstance(raised_exception, ForcedError)
        self.assertEqual(str(raised_exception), "robust callback")

    async def test_delays_execution_until_after_transaction_commit(self):
        async with async_atomic():
            await self.do(1)
            self.assertNotified([])
        await self.assertDone([1])

    async def test_does_not_execute_if_transaction_rolled_back(self):
        try:
            async with async_atomic():
                await self.do(1)
                raise ForcedError()
        except ForcedError:
            pass

        await self.assertDone([])

    async def test_executes_only_after_final_transaction_committed(self):
        async with async_atomic():
            async with async_atomic():
                await self.do(1)
                self.assertNotified([])
            self.assertNotified([])
        await self.assertDone([1])

    async def test_discards_hooks_from_rolled_back_savepoint(self):
        async with async_atomic():
            # one successful savepoint
            async with async_atomic():
                await self.do(1)
            # one failed savepoint
            try:
                async with async_atomic():
                    await self.do(2)
                    raise ForcedError()
            except ForcedError:
                pass
            # another successful savepoint
            async with async_atomic():
                await self.do(3)

        # only hooks registered during successful savepoints execute
        await self.assertDone([1, 3])

    async def test_no_hooks_run_from_failed_transaction(self):
        """If outer transaction fails, no hooks from within it run."""
        try:
            async with async_atomic():
                async with async_atomic():
                    await self.do(1)
                raise ForcedError()
        except ForcedError:
            pass

        await self.assertDone([])

    async def test_inner_savepoint_rolled_back_with_outer(self):
        async with async_atomic():
            try:
                async with async_atomic():
                    async with async_atomic():
                        await self.do(1)
                    raise ForcedError()
            except ForcedError:
                pass
            await self.do(2)

        await self.assertDone([2])

    async def test_no_savepoints_atomic_merged_with_outer(self):

        async with async_atomic():
            async with async_atomic():
                await self.do(1)
                try:
                    async with async_atomic(savepoint=False):
                        raise ForcedError()
                except ForcedError:
                    pass

        await self.assertDone([])

    async def test_inner_savepoint_does_not_affect_outer(self):
        async with async_atomic():
            async with async_atomic():
                await self.do(1)
                try:
                    async with async_atomic():
                        raise ForcedError()
                except ForcedError:
                    pass

        await self.assertDone([1])

    async def test_runs_hooks_in_order_registered(self):
        async with async_atomic():
            await self.do(1)
            async with async_atomic():
                await self.do(2)
            await self.do(3)

        await self.assertDone([1, 2, 3])

    async def test_hooks_cleared_after_successful_commit(self):
        async with async_atomic():
            await self.do(1)
        async with async_atomic():
            await self.do(2)

        await self.assertDone([1, 2])  # not [1, 1, 2]

    async def test_hooks_cleared_after_rollback(self):
        try:
            async with async_atomic():
                await self.do(1)
                raise ForcedError()
        except ForcedError:
            pass

        async with async_atomic():
            await self.do(2)

        await self.assertDone([2])

    async def test_hooks_cleared_on_reconnect(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with async_atomic():
            await self.do(1)
            await connection.close()

        await connection.connect()

        async with async_atomic():
            await self.do(2)

        await self.assertDone([2])

    async def test_error_in_hook_does_not_prevent_clearing_hooks(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        try:
            async with async_atomic():
                await connection.on_commit(lambda: self.notify("error"))
        except ForcedError:
            pass

        async with async_atomic():
            await self.do(1)

        await self.assertDone([1])

    async def test_db_query_in_hook(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async def commit():
            return [self.notify(t) for t in await get_all()]

        async with async_atomic():
            await create_instance(1)
            await connection.on_commit(commit)

        await self.assertDone([1])

    async def test_transaction_in_hook(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async def on_commit():
            async with async_atomic():
                t = await create_instance(1)
                self.notify(t)

        async with async_atomic():
            await connection.on_commit(on_commit)

        await self.assertDone([1])

    async def test_hook_in_hook(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async def on_commit(i, add_hook):
            async with async_atomic():
                if add_hook:
                    await connection.on_commit(
                        lambda: on_commit(i + 10, False)
                    )
                t = await create_instance(i)
                self.notify(t)

        async with async_atomic():
            await connection.on_commit(lambda: on_commit(1, True))
            await connection.on_commit(lambda: on_commit(2, True))

        await self.assertDone([1, 11, 2, 12])

    async def test_raises_exception_non_autocommit_mode(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        def should_never_be_called():
            raise AssertionError("this function should never be called")

        try:
            await connection.set_autocommit(False)
            msg = "cannot be used in manual transaction management"
            with self.assertRaisesRegex(
                transaction.TransactionManagementError, msg
            ):
                await connection.on_commit(should_never_be_called)
        finally:
            await connection.set_autocommit(True)

    async def test_raises_exception_non_callable(self):
        msg = "callback must be a callable"
        connection = async_connections[DEFAULT_DB_ALIAS]

        with self.assertRaisesRegex(TypeError, msg):
            await connection.on_commit(None)
