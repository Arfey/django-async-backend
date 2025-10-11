from contextlib import asynccontextmanager
from unittest.mock import MagicMock

from django.db import DEFAULT_DB_ALIAS
from django.test import TestCase

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic


@asynccontextmanager
async def atomic_test(name=DEFAULT_DB_ALIAS):
    async with async_atomic(name):
        try:
            yield
        finally:
            async_connections[name].set_rollback(True)


class AsyncCursorTest(TestCase):

    async def create_table(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with connection.cursor() as cursor:
            await cursor.execute(
                "CREATE TABLE test_table_tmp (name VARCHAR(255) NOT NULL);"
            )

    @atomic_test()
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

    @atomic_test()
    async def test_executemany(self):
        await self.create_table()

        async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO test_table_tmp (name) VALUES (%s)", [(1,), (2,)]
            )
            res = await cursor.execute("""SELECT name FROM test_table_tmp;""")

            self.assertEqual(await res.fetchall(), [("1",), ("2",)])

    @atomic_test()
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

    @atomic_test()
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
