from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import SynchronousOnlyOperation
from django.db import DEFAULT_DB_ALIAS
from django.test import override_settings
from test_app.models import (
    GenericFkModel,
    SaveChildModel,
    SaveModel,
    SaveParentModel,
    TestModel,
)

from django_async_backend.db import async_connections
from django_async_backend.db.models.base import AsyncModelMixin
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)
from django_async_backend.utils.contenttypes import aget_for_model


class InstanceHintRouter:
    """Routes reads to "other" only when it is given the instance hint."""

    def db_for_read(self, model, **hints):
        if isinstance(hints.get("instance"), TestModel):
            return "other"
        return None

    def db_for_write(self, model, **hints):
        return None

    def allow_relation(self, *args, **hints):
        return True

    def allow_migrate(self, *args, **hints):
        return True


class TestAsyncRefreshFromDb(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj = TestModel(name="Item1", value=1)
        await self.obj.async_save()

    async def test_reloads_values_changed_behind_the_instance(self):
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(value=99)

        await self.obj.async_refresh_from_db()

        self.assertEqual(
            self.obj.value, 99, "Stale value should be reloaded from the row"
        )

    async def test_returns_none(self):
        self.assertIsNone(await self.obj.async_refresh_from_db())

    async def test_fields_limits_which_columns_are_reloaded(self):
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(
            name="Renamed", value=99
        )

        await self.obj.async_refresh_from_db(fields=["value"])

        self.assertEqual(self.obj.value, 99, "Listed field should be reloaded")
        self.assertEqual(
            self.obj.name,
            "Item1",
            "Field outside fields= should keep its stale value",
        )

    async def test_empty_fields_does_not_query(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            await self.obj.async_refresh_from_db(fields=[])

        self.assertEqual(
            len(ctx), 0, "An empty fields= should short-circuit before the SQL"
        )

    async def test_related_lookup_in_fields_raises_value_error(self):
        with self.assertRaises(ValueError) as cm:
            await self.obj.async_refresh_from_db(fields=["relative__name"])

        self.assertEqual(
            str(cm.exception),
            'Found "__" in fields argument. Relations and transforms '
            "are not allowed in fields.",
        )

    async def test_using_selects_the_connection(self):
        """Each alias runs in its own uncommitted transaction, so this row is
        reachable from "other" only -- a refresh that ignored using= would
        look on "default" and find nothing.
        """
        other = TestModel(name="OnOther", value=1)
        await other.async_save(using="other")
        await TestModel.async_objects.using("other").filter(
            pk=other.pk
        ).aupdate(value=42)
        stale = TestModel(id=other.pk, name="OnOther", value=0)

        await stale.async_refresh_from_db(using="other")

        self.assertEqual(stale.value, 42)
        self.assertEqual(
            stale._state.db,
            "other",
            "_state.db should follow the database the row was read from",
        )

    async def test_from_queryset_is_used_to_reload(self):
        """The row is only reachable through the queryset we hand in, so a
        queryset that filters it out has to raise DoesNotExist.
        """
        with self.assertRaises(TestModel.DoesNotExist):
            await self.obj.async_refresh_from_db(
                from_queryset=TestModel.async_objects.filter(value__gt=1000)
            )

    async def test_deleted_row_raises_does_not_exist(self):
        await TestModel.async_objects.filter(pk=self.obj.pk).adelete()

        with self.assertRaises(TestModel.DoesNotExist):
            await self.obj.async_refresh_from_db()

    async def test_deferred_instance_reloads_only_loaded_fields(self):
        """An instance loaded through _only() must refresh without touching a
        deferred attribute -- reading one would fall back to Django's sync
        refresh_from_db and raise SynchronousOnlyOperation.
        """
        # _only() is private and defer() is not generated at all, so this is
        # the only way to build a deferred instance. It is setup, not subject.
        deferred = [
            o
            async for o in TestModel.async_objects.all()
            ._only("id", "name")
            .filter(pk=self.obj.pk)
        ][0]
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(
            name="Renamed", value=99
        )

        await deferred.async_refresh_from_db()

        self.assertEqual(
            deferred.name, "Renamed", "Loaded field should be reloaded"
        )
        self.assertEqual(
            deferred.get_deferred_fields(),
            {"value", "relative_id"},
            "Refresh should not un-defer the fields _only() left out",
        )
        with self.assertRaises(SynchronousOnlyOperation):
            deferred.value

    async def test_fields_loads_a_deferred_field(self):
        """fields= wins over the deferred set, so a field _only() left out is
        fetched and stops being deferred.
        """
        deferred = [
            o
            async for o in TestModel.async_objects.all()
            ._only("id", "name")
            .filter(pk=self.obj.pk)
        ][0]
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(value=99)

        await deferred.async_refresh_from_db(fields=["value"])

        self.assertEqual(deferred.value, 99)
        self.assertEqual(
            deferred.get_deferred_fields(),
            {"relative_id"},
            "Only the requested field should stop being deferred",
        )

    async def test_refreshes_inherited_parent_fields(self):
        """A multi-table child reloads the columns of its parent table too."""
        child = SaveChildModel(parent_value=1, child_value=2)
        await child.async_save()
        # aupdate() cannot span the two tables, so update each separately.
        await SaveParentModel.async_objects.filter(pk=child.pk).aupdate(
            parent_value=10
        )
        await SaveChildModel.async_objects.filter(pk=child.pk).aupdate(
            child_value=20
        )

        await child.async_refresh_from_db()

        self.assertEqual(child.parent_value, 10)
        self.assertEqual(child.child_value, 20)

    async def test_prefetched_objects_cache_is_cleared(self):
        self.obj._prefetched_objects_cache = {"relatives": []}

        await self.obj.async_refresh_from_db()

        self.assertEqual(
            self.obj._prefetched_objects_cache,
            {},
            "A full refresh should drop every prefetched result",
        )

    async def test_fields_drops_matching_prefetched_lookup(self):
        """A name in fields= that is a prefetch lookup is removed from the
        cache and from fields, rather than being sent to the database.
        """
        self.obj._prefetched_objects_cache = {"relatives": []}

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            await self.obj.async_refresh_from_db(fields=["relatives"])

        self.assertEqual(self.obj._prefetched_objects_cache, {})
        self.assertEqual(
            len(ctx), 0, "Nothing was left in fields=, so nothing to reload"
        )

    async def test_cached_foreign_key_is_copied_from_the_reloaded_row(self):
        parent = TestModel(name="Parent", value=0)
        await parent.async_save()
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(
            relative=parent
        )

        await self.obj.async_refresh_from_db(
            from_queryset=TestModel.async_objects.select_related("relative")
        )

        field = TestModel._meta.get_field("relative")
        self.assertTrue(
            field.is_cached(self.obj),
            "select_related() populated the cache on the reloaded row, so it "
            "should carry over",
        )
        self.assertEqual(field.get_cached_value(self.obj).pk, parent.pk)

    async def test_stale_cached_foreign_key_is_cleared(self):
        """The foreign key column itself is unchanged, so assigning it does
        not invalidate the cache -- the refresh has to drop it explicitly so
        the next access refetches.
        """
        parent = TestModel(name="Parent", value=0)
        await parent.async_save()
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(
            relative=parent
        )
        await self.obj.async_refresh_from_db()
        self.obj.relative = parent
        field = TestModel._meta.get_field("relative")
        self.assertTrue(field.is_cached(self.obj))

        await self.obj.async_refresh_from_db()

        self.assertFalse(
            field.is_cached(self.obj),
            "The reloaded row had no cached relative, so the stale cache "
            "should be dropped",
        )

    async def test_using_overrides_the_database_of_from_queryset(self):
        """using= is re-applied to the queryset we were handed, so the alias
        it was built with is discarded.
        """
        other = TestModel(name="OnOther", value=1)
        await other.async_save(using="other")
        await TestModel.async_objects.using("other").filter(
            pk=other.pk
        ).aupdate(value=7)
        stale = TestModel(id=other.pk, name="OnOther", value=0)

        await stale.async_refresh_from_db(
            from_queryset=TestModel.async_objects.using(DEFAULT_DB_ALIAS),
            using="other",
        )

        self.assertEqual(stale.value, 7)
        self.assertEqual(stale._state.db, "other")

    @override_settings(DATABASE_ROUTERS=[InstanceHintRouter()])
    async def test_router_is_given_the_instance_hint(self):
        """The router picks "other" only when it receives the instance hint,
        so a refresh that dropped the hint would read "default".
        """
        other = TestModel(name="OnOther", value=1)
        await other.async_save(using="other")
        stale = TestModel(id=other.pk, name="OnOther", value=0)

        await stale.async_refresh_from_db()

        self.assertEqual(stale.value, 1)
        self.assertEqual(stale._state.db, "other")

    async def test_sync_queryset_is_rejected(self):
        """A sync queryset has filter() and aget(), so without a guard it
        would read through Django's connection and a different transaction.
        """
        with self.assertRaises(TypeError) as cm:
            await self.obj.async_refresh_from_db(
                from_queryset=TestModel.objects.all()
            )

        self.assertEqual(
            str(cm.exception),
            "from_queryset must be an async queryset. "
            "Use Model.async_objects instead of Model.objects.",
        )

    async def test_sync_manager_is_rejected(self):
        with self.assertRaises(TypeError):
            await self.obj.async_refresh_from_db(
                from_queryset=TestModel.objects
            )

    async def test_async_manager_is_accepted(self):
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(value=5)

        await self.obj.async_refresh_from_db(
            from_queryset=TestModel.async_objects
        )

        self.assertEqual(self.obj.value, 5)

    async def test_cached_reverse_one_to_one_is_cleared(self):
        child = SaveChildModel(parent_value=1, child_value=2)
        await child.async_save()
        parent = await SaveParentModel.async_objects.aget(pk=child.pk)
        rel = SaveParentModel._meta.get_field("savechildmodel")
        rel.set_cached_value(parent, child)

        await parent.async_refresh_from_db()

        self.assertFalse(
            rel.is_cached(parent),
            "A full refresh should clear cached reverse relations",
        )

    async def test_fields_leaves_an_unlisted_cached_reverse_relation(self):
        """Only reverse relations named in fields= are dropped."""
        child = SaveChildModel(parent_value=1, child_value=2)
        await child.async_save()
        parent = await SaveParentModel.async_objects.aget(pk=child.pk)
        rel = SaveParentModel._meta.get_field("savechildmodel")
        rel.set_cached_value(parent, child)

        await parent.async_refresh_from_db(fields=["parent_value"])

        self.assertTrue(
            rel.is_cached(parent),
            "A reverse relation outside fields= should be left alone",
        )

    async def test_cached_generic_foreign_key_is_cleared(self):
        target = await SaveModel.async_objects.acreate(name="Target", value=1)
        # Assigning a generic FK resolves the content type through Django's
        # sync manager, so warm its cache first.
        await aget_for_model(SaveModel)
        obj = await GenericFkModel.async_objects.acreate(name="Holder")
        obj.content_object = target
        field = GenericFkModel._meta.get_field("content_object")
        self.assertTrue(field.is_cached(obj))

        await obj.async_refresh_from_db()

        self.assertFalse(
            field.is_cached(obj),
            "A full refresh should clear cached private (generic) relations",
        )

    async def test_fields_leaves_an_unlisted_cached_generic_foreign_key(self):
        """Only private relations named in fields= are dropped."""
        target = await SaveModel.async_objects.acreate(name="Target", value=1)
        await aget_for_model(SaveModel)
        obj = await GenericFkModel.async_objects.acreate(name="Holder")
        obj.content_object = target
        field = GenericFkModel._meta.get_field("content_object")

        await obj.async_refresh_from_db(fields=["name"])

        self.assertTrue(
            field.is_cached(obj),
            "A generic relation outside fields= should be left alone",
        )

    async def test_works_on_a_model_without_the_mixin(self):
        """patch.py copies the method onto Model, so third-party models get it
        too.
        """
        ct = await ContentType.async_objects.acreate(
            app_label="refresh_test", model="widget"
        )
        self.assertNotIsInstance(ct, AsyncModelMixin)
        await ContentType.async_objects.filter(pk=ct.pk).aupdate(
            model="gadget"
        )

        await ct.async_refresh_from_db()

        self.assertEqual(ct.model, "gadget")
