"""Shared helpers around the Reporter model used by backend and transaction
tests that need to observe real BEGIN/COMMIT/ROLLBACK."""

from django.db import DEFAULT_DB_ALIAS
from shared.models import Reporter

from django_async_backend.db import async_connections


async def truncate_reporter_table():
    """Wipe the table outside of any transaction. Used by the
    reporter_table_transaction fixture for cleanup between tests."""
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("TRUNCATE TABLE reporter_table_tmp RESTART IDENTITY;")


async def insert_reporter(id):
    await Reporter.async_object.acreate(name=str(id))
    return str(id)


async def fetch_all_reporters():
    return [r.name async for r in Reporter.async_object.order_by("name")]
