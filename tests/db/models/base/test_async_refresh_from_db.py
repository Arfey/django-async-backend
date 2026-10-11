from django.contrib.contenttypes.models import ContentType
from django.test import override_settings
from test_app.models import (
    GenericFkModel,
    SaveChildModel,
    SaveModel,
    SaveParentModel,
    TestModel,
)

from django_async_backend.db.models.base import AsyncModelMixin
from django_async_backend.test import AsyncioTestCase
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

    async def test_fields_is_not_supported(self):
        with self.assertRaises(NotImplementedError) as cm:
            await self.obj.async_refresh_from_db(fields=["value"])

        self.assertEqual(
            str(cm.exception),
            "async_refresh_from_db() always reloads every concrete field; "
            "'fields' is not supported.",
        )

    async def test_from_queryset_is_not_supported(self):
        with self.assertRaises(NotImplementedError) as cm:
            await self.obj.async_refresh_from_db(
                from_queryset=TestModel.async_objects.all()
            )

        self.assertEqual(
            str(cm.exception),
            "async_refresh_from_db() always reloads every concrete field; "
            "'from_queryset' is not supported.",
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

    async def test_deleted_row_raises_does_not_exist(self):
        await TestModel.async_objects.filter(pk=self.obj.pk).adelete()

        with self.assertRaises(TestModel.DoesNotExist):
            await self.obj.async_refresh_from_db()

    async def test_deferred_fields_are_loaded(self):
        """Reading a deferred field would fall back to Django's sync
        refresh_from_db and raise SynchronousOnlyOperation, so a refresh
        loads every concrete field, deferred ones included.
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

        self.assertEqual(deferred.get_deferred_fields(), set())
        self.assertEqual(deferred.name, "Renamed")
        self.assertEqual(deferred.value, 99)

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

        self.assertEqual(self.obj._prefetched_objects_cache, {})

    async def test_select_related_foreign_key_is_cleared(self):
        """Cleared even though the foreign key did not change: the cached
        object could hold stale values of its own.
        """
        parent = TestModel(name="Parent", value=0)
        await parent.async_save()
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(
            relative=parent
        )
        obj = await TestModel.async_objects.select_related("relative").aget(
            pk=self.obj.pk
        )
        field = TestModel._meta.get_field("relative")
        self.assertTrue(field.is_cached(obj))

        await obj.async_refresh_from_db()

        self.assertFalse(field.is_cached(obj))
        self.assertEqual(
            obj.relative_id,
            parent.pk,
            "The foreign key column itself is still reloaded",
        )

    async def test_cached_reverse_one_to_one_is_cleared(self):
        child = SaveChildModel(parent_value=1, child_value=2)
        await child.async_save()
        parent = await SaveParentModel.async_objects.aget(pk=child.pk)
        rel = SaveParentModel._meta.get_field("savechildmodel")
        rel.set_cached_value(parent, child)

        await parent.async_refresh_from_db()

        self.assertFalse(rel.is_cached(parent))

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

        self.assertFalse(field.is_cached(obj))

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
