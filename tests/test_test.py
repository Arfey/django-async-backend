from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import (
    AsyncioTestCase,
    AsyncioTransactionTestCase,
)


async def purge():
    """App-level helper that uses async_atomic internally."""
    async with async_atomic():
        async with await async_connections["default"].cursor() as c:
            await c.execute("DROP TABLE IF EXISTS demo")


class DownCallsHelperMixin:
    async def asyncSetUp(self):
        async with await async_connections["default"].cursor() as c:
            await c.execute("CREATE TABLE IF NOT EXISTS demo (id SERIAL)")

    async def asyncTearDown(self):
        # RuntimeError: "Using a transaction in a nested task is forbidden"
        await purge()

    async def test_thing(self):
        async with await async_connections["default"].cursor() as c:
            await c.execute("INSERT INTO demo DEFAULT VALUES")


class AsyncioTransactionTestCaseTearDownCallsHelper(
    DownCallsHelperMixin, AsyncioTransactionTestCase
):
    pass


class AsyncioTestCaseTearDownCallsHelper(
    DownCallsHelperMixin, AsyncioTestCase
):
    pass
