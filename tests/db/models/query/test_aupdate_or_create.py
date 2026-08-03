from django.db import DEFAULT_DB_ALIAS
from test_app.models import (
    SaveModel,
    UpdateOrCreateModel,
)

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

    async def test_create_defaults_callable_is_resolved(self):
        obj, created = await SaveModel.async_objects.aupdate_or_create(
            name="Callable", create_defaults={"value": lambda: 5}
        )

        self.assertTrue(created)
        self.assertEqual(obj.value, 5)
        obj = await SaveModel.async_objects.aget(pk=obj.pk)
        self.assertEqual(obj.value, 5)

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


class TestAUpdateOrCreateUpdateFields(AsyncioTestCase):
    async def test_pre_save_fields_are_added_to_update_fields(self):
        existing = await UpdateOrCreateModel.async_objects.acreate(
            name="Existing", value=1
        )
        stale = existing.updated_at

        obj, created = (
            await UpdateOrCreateModel.async_objects.aupdate_or_create(
                name="Existing", defaults={"value": 2}
            )
        )

        self.assertFalse(created)
        # auto_now defines pre_save(), so it is refreshed even though it was
        # not listed in defaults.
        self.assertGreater(obj.updated_at, stale)
        obj = await UpdateOrCreateModel.async_objects.aget(pk=existing.pk)
        self.assertEqual(obj.value, 2)
        self.assertGreater(obj.updated_at, stale)

    async def test_related_field_attname_is_added_to_update_fields(self):
        related = await SaveModel.async_objects.acreate(name="Related")
        await UpdateOrCreateModel.async_objects.acreate(
            name="Existing", value=1, related=related
        )
        other = await SaveModel.async_objects.acreate(name="Other")

        obj, created = (
            await UpdateOrCreateModel.async_objects.aupdate_or_create(
                name="Existing", defaults={"value": 2}
            )
        )

        self.assertFalse(created)
        # `related` has a custom pre_save(), so both "related" and
        # "related_id" land in update_fields and the FK survives the save.
        self.assertEqual(obj.related_id, related.pk)
        obj = await UpdateOrCreateModel.async_objects.aget(pk=obj.pk)
        self.assertEqual(obj.related_id, related.pk)
        self.assertNotEqual(obj.related_id, other.pk)

    async def test_non_concrete_defaults_save_all_fields(self):
        existing = await UpdateOrCreateModel.async_objects.acreate(
            name="Existing", value=1
        )

        obj, created = (
            await UpdateOrCreateModel.async_objects.aupdate_or_create(
                name="Existing", defaults={"upper_name": "RENAMED"}
            )
        )

        self.assertFalse(created)
        self.assertEqual(obj.pk, existing.pk)
        # upper_name is a property, not a concrete field, so update_or_create
        # falls back to a full save() instead of passing update_fields.
        self.assertEqual(obj.name, "renamed")
        obj = await UpdateOrCreateModel.async_objects.aget(pk=existing.pk)
        self.assertEqual(obj.name, "renamed")


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
