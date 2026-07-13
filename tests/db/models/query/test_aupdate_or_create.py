from django.db import DEFAULT_DB_ALIAS
from test_app.models import SaveModel

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
    AsyncioTransactionTestCase,
)


class TestAUpdateOrCreate(AsyncioTestCase):
    async def test_creates_when_missing(self):
        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Fresh", defaults={"value": 7}
        )

        self.assertTrue(created)
        self.assertIsNotNone(obj.pk)
        self.assertEqual(obj.value, 7)
        self.assertTrue(
            await SaveModel.async_objects.filter(
                name="Fresh", value=7
            ).aexists()
        )

    async def test_updates_existing_object(self):
        existing = await SaveModel.async_objects.acreate(
            name="Existing", value=1
        )

        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Existing", defaults={"value": 2}
        )

        self.assertFalse(created)
        self.assertEqual(obj.pk, existing.pk)
        self.assertEqual(obj.value, 2)
        existing = await SaveModel.async_objects.aget(pk=existing.pk)
        self.assertEqual(existing.value, 2)

    async def test_create_defaults_used_when_creating(self):
        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="New",
            defaults={"value": 10},
            create_defaults={"value": 5},
        )

        self.assertTrue(created)
        self.assertEqual(obj.value, 5)

    async def test_create_defaults_ignored_when_updating(self):
        await SaveModel.async_objects.acreate(name="Existing", value=1)

        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Existing",
            defaults={"value": 10},
            create_defaults={"value": 5},
        )

        self.assertFalse(created)
        self.assertEqual(obj.value, 10)

    async def test_defaults_callable_is_resolved(self):
        await SaveModel.async_objects.acreate(name="Callable", value=1)

        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Callable", defaults={"value": lambda: 42}
        )

        self.assertFalse(created)
        self.assertEqual(obj.value, 42)
        obj = await SaveModel.async_objects.aget(pk=obj.pk)
        self.assertEqual(obj.value, 42)

    async def test_no_defaults_issues_no_update(self):
        await SaveModel.async_objects.acreate(name="Existing", value=1)

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj, created = await SaveModel.async_objects.aupdate_or_create(
                name="Existing"
            )

        self.assertFalse(created)
        self.assertEqual(obj.value, 1)
        self.assertFalse(
            any(
                query["sql"].startswith("UPDATE")
                for query in ctx.captured_queries
            )
        )

    async def test_lookup_row_is_locked_for_update(self):
        await SaveModel.async_objects.acreate(name="Existing", value=1)

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            await SaveModel.async_objects.aupdate_or_create(
                name="Existing", defaults={"value": 2}
            )

        self.assertTrue(
            any(
                "FOR UPDATE" in query["sql"]
                for query in ctx.captured_queries
            )
        )


class TestAUpdateOrCreateOutsideTransaction(AsyncioTransactionTestCase):
    # AsyncioTransactionTestCase runs in autocommit (no outer transaction),
    # so the FOR UPDATE lookup only works if aupdate_or_create opens its
    # own atomic block.

    async def asyncTearDown(self):
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            await cursor.execute("DELETE FROM save_model;")

    async def test_opens_its_own_transaction(self):
        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Fresh", defaults={"value": 7}
        )
        self.assertTrue(created)

        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Fresh", defaults={"value": 8}
        )
        self.assertFalse(created)
        obj = await SaveModel.async_objects.aget(pk=obj.pk)
        self.assertEqual(obj.value, 8)
