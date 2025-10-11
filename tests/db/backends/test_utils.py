from unittest.mock import (
    MagicMock,
    patch,
)

from django.db import DEFAULT_DB_ALIAS
from django.db.transaction import TransactionManagementError
from django.test import override_settings

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import AsyncioTestCase


class AsyncCursorTest(AsyncioTestCase):

    async def create_table(self):
        async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            await cursor.execute(
                "CREATE TABLE test_table_tmp (name VARCHAR(255) NOT NULL);"
            )

    async def test_execute(self):
        await self.create_table()

        async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO test_table_tmp (name) VALUES ('1');
            """
            )
            res = await cursor.execute("""SELECT name FROM test_table_tmp;""")

            self.assertEqual(await res.fetchone(), ("1",))

    async def test_execute_broken_transaction(self):
        async with async_atomic(DEFAULT_DB_ALIAS):
            connection = async_connections[DEFAULT_DB_ALIAS]
            async with connection.cursor() as cursor:
                connection.set_rollback(True)

                with self.assertRaises(TransactionManagementError):
                    await cursor.executemany("""SELECT %s;""", [(1,)])

    async def test_executemany(self):
        await self.create_table()

        async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO test_table_tmp (name) VALUES (%s)", [(1,), (2,)]
            )
            res = await cursor.execute("""SELECT name FROM test_table_tmp;""")

            self.assertEqual(await res.fetchall(), [("1",), ("2",)])

    async def test_executemany_broken_transaction(self):
        async with async_atomic(DEFAULT_DB_ALIAS):
            connection = async_connections[DEFAULT_DB_ALIAS]
            async with connection.cursor() as cursor:
                connection.set_rollback(True)

                with self.assertRaises(TransactionManagementError):
                    await cursor.execute("""SELECT 1;""")

    async def test_iterator(self):
        await self.create_table()

        async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO test_table_tmp (name) VALUES ('1'), ('2'), ('3');
            """
            )
            await cursor.execute("""SELECT name FROM test_table_tmp;""")

            self.assertEqual(
                [i async for i in cursor], [("1",), ("2",), ("3",)]
            )

    async def test_execution_wrapper(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        wrapper_spy = MagicMock()

        def test_wrapper(execute, sql, params, many, context):
            wrapper_spy(sql=sql, params=params, many=many)
            return execute(sql, params, many, context)

        with connection.execute_wrapper(test_wrapper):
            async with connection.cursor() as cursor:
                await cursor.execute("""select 1""")

        wrapper_spy.assert_called_once_with(
            sql="select 1", params=None, many=False
        )


class AsyncCursorDebugWrapperTest(AsyncioTestCase):
    @override_settings(DEBUG=True)
    @patch("django_async_backend.db.backends.utils.logger.debug")
    async def test_execute(self, debug_mock):
        connection = async_connections[DEFAULT_DB_ALIAS]
        async with connection.cursor() as cursor:
            res = await cursor.execute("""select 1;""")
            self.assertEqual(await res.fetchone(), (1,))

        self.assertEqual(len(connection.queries_log), 1)
        self.assertEqual(connection.queries_log[0]["sql"], "select 1;")
        debug_mock.assert_called()

    @override_settings(DEBUG=True)
    @patch("django_async_backend.db.backends.utils.logger.debug")
    async def test_executemany(self, debug_mock):
        connection = async_connections[DEFAULT_DB_ALIAS]
        async with connection.cursor() as cursor:
            await cursor.executemany("""SELECT %s;""", [(1,)])

        self.assertEqual(len(connection.queries_log), 1)
        self.assertEqual(
            connection.queries_log[0]["sql"], "1 times: SELECT %s;"
        )
        debug_mock.assert_called()
