from contextlib import asynccontextmanager

from django.db import DEFAULT_DB_ALIAS

from django_async_backend.db import async_connections


@asynccontextmanager
async def async_capture_queries(using=DEFAULT_DB_ALIAS):
    connection = async_connections[using]
    old = connection.force_debug_cursor
    connection.force_debug_cursor = True
    await connection.ensure_connection()
    initial = len(connection.queries_log)
    queries = []
    try:
        yield queries
    finally:
        connection.force_debug_cursor = old
        queries.extend(list(connection.queries_log)[initial:])


@asynccontextmanager
async def async_assert_num_queries(num, *, using=DEFAULT_DB_ALIAS):
    async with async_capture_queries(using) as queries:
        yield queries
    executed = len(queries)
    assert executed == num, f"{executed} queries executed, {num} expected\nCaptured queries were:\n" + "\n".join(
        f"{i}. {q['sql']}" for i, q in enumerate(queries, start=1)
    )
