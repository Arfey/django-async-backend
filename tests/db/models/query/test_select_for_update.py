from django.db import DEFAULT_DB_ALIAS
from django.db.transaction import TransactionManagementError
from test_app.models import SaveModel

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
    AsyncioTransactionTestCase,
)


class TestSelectForUpdate(AsyncioTestCase):
    async def asyncSetUp(self):
        await SaveModel.async_objects.acreate(name="Item1", value=1)

    async def test_nowait_and_skip_locked_are_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            SaveModel.async_objects.select_for_update(
                nowait=True, skip_locked=True
            )

    async def test_sql_contains_for_update(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            items = [
                i
                async for i in SaveModel.async_objects.select_for_update()
            ]

        self.assertEqual(len(items), 1)
        self.assertIn("FOR UPDATE", ctx.captured_queries[-1]["sql"])

    async def _last_sql(self, **kwargs):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            [
                i
                async for i in SaveModel.async_objects.select_for_update(
                    **kwargs
                )
            ]
        return ctx.captured_queries[-1]["sql"]

    async def test_sql_contains_for_update_skip_locked(self):
        self.assertIn(
            "FOR UPDATE SKIP LOCKED", await self._last_sql(skip_locked=True)
        )

    async def test_sql_contains_for_update_nowait(self):
        self.assertIn("FOR UPDATE NOWAIT", await self._last_sql(nowait=True))

    async def test_sql_contains_for_update_of(self):
        self.assertIn("FOR UPDATE OF", await self._last_sql(of=("self",)))

    async def test_sql_contains_for_no_key_update(self):
        self.assertIn("FOR NO KEY UPDATE", await self._last_sql(no_key=True))


class TestSelectForUpdateOutsideTransaction(AsyncioTransactionTestCase):
    async def test_raises_outside_of_transaction(self):
        with self.assertRaises(TransactionManagementError):
            [
                i
                async for i in SaveModel.async_objects.select_for_update()
            ]

    async def test_raises_in_subquery_outside_of_transaction(self):
        # The FOR UPDATE flag sits on the inner query only; the check must
        # still fire even though the connection may not be established yet.
        inner = SaveModel.async_objects.select_for_update().values("pk")
        with self.assertRaises(TransactionManagementError):
            [
                i
                async for i in SaveModel.async_objects.filter(pk__in=inner)
            ]
