from unittest.mock import MagicMock, patch

import pytest
from django.db import DEFAULT_DB_ALIAS
from django.db.transaction import TransactionManagementError
from django.test import override_settings

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic


@pytest.fixture
async def cursor_table(async_db):
    """Create a temporary table for cursor tests."""
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("CREATE TABLE test_table_tmp (name VARCHAR(255) NOT NULL);")


async def test_execute(cursor_table):
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("INSERT INTO test_table_tmp (name) VALUES ('1');")
        res = await cursor.execute("SELECT name FROM test_table_tmp;")
        assert await res.fetchone() == ("1",)


async def test_execute_broken_transaction(async_db):
    async with async_atomic(DEFAULT_DB_ALIAS):
        connection = async_connections[DEFAULT_DB_ALIAS]
        async with await connection.cursor() as cursor:
            connection.set_rollback(True)
            with pytest.raises(TransactionManagementError):
                await cursor.executemany("SELECT %s;", [(1,)])


async def test_executemany(cursor_table):
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.executemany("INSERT INTO test_table_tmp (name) VALUES (%s)", [(1,), (2,)])
        res = await cursor.execute("SELECT name FROM test_table_tmp;")
        assert await res.fetchall() == [("1",), ("2",)]


async def test_executemany_broken_transaction(async_db):
    async with async_atomic(DEFAULT_DB_ALIAS):
        connection = async_connections[DEFAULT_DB_ALIAS]
        async with await connection.cursor() as cursor:
            connection.set_rollback(True)
            with pytest.raises(TransactionManagementError):
                await cursor.execute("SELECT 1;")


async def test_iterator(cursor_table):
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("INSERT INTO test_table_tmp (name) VALUES ('1'), ('2'), ('3');")
        await cursor.execute("SELECT name FROM test_table_tmp;")
        assert [i async for i in cursor] == [("1",), ("2",), ("3",)]


async def test_execution_wrapper(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    wrapper_spy = MagicMock()

    def spy_wrapper(execute, sql, params, many, context):
        wrapper_spy(sql=sql, params=params, many=many)
        return execute(sql, params, many, context)

    with connection.execute_wrapper(spy_wrapper):
        async with await connection.cursor() as cursor:
            await cursor.execute("select 1")

    wrapper_spy.assert_called_once_with(sql="select 1", params=None, many=False)


@override_settings(DEBUG=True)
@patch("django_async_backend.db.backends.utils.logger.debug")
async def test_debug_wrapper_execute(debug_mock, async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with await connection.cursor() as cursor:
        res = await cursor.execute("select 1;")
        assert await res.fetchone() == (1,)

    assert len(connection.queries_log) == 1
    assert connection.queries_log[0]["sql"] == "select 1;"
    debug_mock.assert_called()


@override_settings(DEBUG=True)
@patch("django_async_backend.db.backends.utils.logger.debug")
async def test_debug_wrapper_executemany(debug_mock, async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with await connection.cursor() as cursor:
        await cursor.executemany("SELECT %s;", [(1,)])

    assert len(connection.queries_log) == 1
    assert connection.queries_log[0]["sql"] == "1 times: SELECT %s;"
    debug_mock.assert_called()
