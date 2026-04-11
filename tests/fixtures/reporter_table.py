"""Shared helpers for a temporary reporter table used by backend and
transaction tests."""

from django.db import DEFAULT_DB_ALIAS

from django_async_backend.db import async_connections


async def create_reporter_table():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE reporter_table_tmp (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            );
            """
        )


async def drop_reporter_table():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("DROP TABLE reporter_table_tmp;")
    await async_connections[DEFAULT_DB_ALIAS].close()


async def insert_reporter(id):
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(f"INSERT INTO reporter_table_tmp (name) VALUES ('{id}');")
        return str(id)


async def fetch_all_reporters():
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        res = await cursor.execute("SELECT name FROM reporter_table_tmp order by name;")
        return [i[0] for i in await res.fetchall()]
