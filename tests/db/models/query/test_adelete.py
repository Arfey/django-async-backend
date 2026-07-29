from django.db import NotSupportedError
from django.db.models import signals
from django.db.models.deletion import (
    ProtectedError,
    RestrictedError,
)
from test_app.models import (
    AsyncOnDeleteChildModel,
    AsyncOnDeleteParentModel,
    CascadeChildModel,
    DeleteModel,
    DoNothingChildModel,
    FastDeleteModel,
    ProtectChildModel,
    RestrictChildModel,
    SetCallableChildModel,
    SetChildModel,
    SetDefaultChildModel,
    SetNullChildModel,
    SyncOnDeleteParentModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestADelete(AsyncioTestCase):
    async def asyncSetUp(self):
        await FastDeleteModel(name="Item1").async_save()
        await FastDeleteModel(name="Item2").async_save()
        await FastDeleteModel(name="Item3").async_save()

    async def test_returns_count_and_per_model_counts(self):
        count, per_model = await FastDeleteModel.async_objects.all().adelete()

        self.assertEqual(count, 3)
        self.assertEqual(per_model, {"test_app.FastDeleteModel": 3})
        self.assertEqual(await FastDeleteModel.async_objects.acount(), 0)

    async def test_filtered_delete(self):
        count, per_model = await FastDeleteModel.async_objects.filter(
            name="Item1"
        ).adelete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.FastDeleteModel": 1})
        names = sorted(
            [obj.name async for obj in FastDeleteModel.async_objects.all()]
        )
        self.assertEqual(names, ["Item2", "Item3"])

    async def test_no_match_returns_zero(self):
        count, per_model = await FastDeleteModel.async_objects.filter(
            name="Nope"
        ).adelete()

        self.assertEqual(count, 0)
        self.assertEqual(per_model, {})
        self.assertEqual(await FastDeleteModel.async_objects.acount(), 3)

    async def test_clears_result_cache(self):
        qs = FastDeleteModel.async_objects.all()
        self.assertEqual(len([obj async for obj in qs]), 3)

        await qs.adelete()

        self.assertEqual(len([obj async for obj in qs]), 0)

    async def test_sliced_raises(self):
        qs = FastDeleteModel.async_objects.all()[:1]

        with self.assertRaises(TypeError):
            await qs.adelete()

    async def test_distinct_fields_raises(self):
        qs = FastDeleteModel.async_objects.distinct("name")

        with self.assertRaises(TypeError):
            await qs.adelete()

    async def test_values_raises(self):
        qs = FastDeleteModel.async_objects.values("name")

        with self.assertRaises(TypeError):
            await qs.adelete()

    async def test_combined_query_raises(self):
        qs = FastDeleteModel.async_objects.filter(name="Item1").union(
            FastDeleteModel.async_objects.filter(name="Item2")
        )

        with self.assertRaises(NotSupportedError):
            await qs.adelete()


class TestADeleteCascade(AsyncioTestCase):
    async def asyncSetUp(self):
        self.parent = DeleteModel(name="Parent", value=1)
        await self.parent.async_save()
        self.other = DeleteModel(name="Other", value=2)
        await self.other.async_save()

    async def test_no_match_returns_zero(self):
        """FastDeleteModel short-circuits into fast_deletes, so an empty
        queryset only reaches the collector on a cascading model.
        """
        count, per_model = await DeleteModel.async_objects.filter(
            name="Nope"
        ).adelete()

        self.assertEqual(count, 0)
        self.assertEqual(per_model, {})
        self.assertEqual(await DeleteModel.async_objects.acount(), 2)

    async def test_cascades_to_children(self):
        await CascadeChildModel(name="Child1", parent=self.parent).async_save()
        await CascadeChildModel(name="Child2", parent=self.parent).async_save()
        await CascadeChildModel(name="Child3", parent=self.other).async_save()

        count, per_model = await DeleteModel.async_objects.filter(
            name="Parent"
        ).adelete()

        self.assertEqual(count, 3)
        self.assertEqual(
            per_model,
            {
                "test_app.DeleteModel": 1,
                "test_app.CascadeChildModel": 2,
            },
        )
        names = sorted(
            [obj.name async for obj in CascadeChildModel.async_objects.all()]
        )
        self.assertEqual(names, ["Child3"])

    async def test_cascades_over_whole_queryset(self):
        await CascadeChildModel(name="Child1", parent=self.parent).async_save()
        await CascadeChildModel(name="Child2", parent=self.other).async_save()

        count, per_model = await DeleteModel.async_objects.all().adelete()

        self.assertEqual(count, 4)
        self.assertEqual(
            per_model,
            {
                "test_app.DeleteModel": 2,
                "test_app.CascadeChildModel": 2,
            },
        )

    async def test_cascades_through_self_referencing_fk(self):
        parent = TestModel(name="Root", value=1)
        await parent.async_save()
        child = TestModel(name="Leaf", value=2, relative=parent)
        await child.async_save()

        count, per_model = await TestModel.async_objects.filter(
            name="Root"
        ).adelete()

        self.assertEqual(count, 2)
        self.assertEqual(per_model, {"test_app.TestModel": 2})
        self.assertEqual(await TestModel.async_objects.acount(), 0)

    async def test_protect_raises(self):
        await ProtectChildModel(name="Child1", parent=self.parent).async_save()

        with self.assertRaises(ProtectedError) as ctx:
            await DeleteModel.async_objects.filter(name="Parent").adelete()

        self.assertEqual(
            [obj.name for obj in ctx.exception.protected_objects], ["Child1"]
        )
        self.assertEqual(await DeleteModel.async_objects.acount(), 2)

    async def test_restrict_raises(self):
        await RestrictChildModel(
            name="Child1", parent=self.parent
        ).async_save()

        with self.assertRaises(RestrictedError) as ctx:
            await DeleteModel.async_objects.filter(name="Parent").adelete()

        self.assertEqual(
            [obj.name for obj in ctx.exception.restricted_objects], ["Child1"]
        )
        self.assertEqual(await DeleteModel.async_objects.acount(), 2)

    async def test_restrict_allowed_when_also_cascaded(self):
        await RestrictChildModel(
            name="Child1", parent=self.parent, owner=self.parent
        ).async_save()

        count, per_model = await DeleteModel.async_objects.filter(
            name="Parent"
        ).adelete()

        self.assertEqual(
            per_model,
            {
                "test_app.DeleteModel": 1,
                "test_app.RestrictChildModel": 1,
            },
        )
        self.assertEqual(count, 2)
        self.assertEqual(await RestrictChildModel.async_objects.acount(), 0)

    async def test_set_null_nulls_the_fk(self):
        await SetNullChildModel(name="Child1", parent=self.parent).async_save()

        count, per_model = await DeleteModel.async_objects.filter(
            name="Parent"
        ).adelete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.DeleteModel": 1})
        child = await SetNullChildModel.async_objects.aget(name="Child1")
        self.assertIsNone(child.parent_id)

    async def test_set_default_resets_the_fk(self):
        await SetDefaultChildModel(
            name="Child1", parent=self.parent
        ).async_save()

        await DeleteModel.async_objects.filter(name="Parent").adelete()

        child = await SetDefaultChildModel.async_objects.aget(name="Child1")
        self.assertIsNone(child.parent_id)

    async def test_set_value_resets_the_fk(self):
        await SetChildModel(name="Child1", parent=self.parent).async_save()

        await DeleteModel.async_objects.filter(name="Parent").adelete()

        child = await SetChildModel.async_objects.aget(name="Child1")
        self.assertIsNone(child.parent_id)

    async def test_set_callable_resets_the_fk(self):
        await SetCallableChildModel(
            name="Child1", parent=self.parent
        ).async_save()

        await DeleteModel.async_objects.filter(name="Parent").adelete()

        child = await SetCallableChildModel.async_objects.aget(name="Child1")
        self.assertIsNone(child.parent_id)

    async def test_do_nothing_leaves_the_fk(self):
        await DoNothingChildModel(
            name="Child1", parent=self.parent
        ).async_save()

        count, per_model = await DeleteModel.async_objects.filter(
            name="Parent"
        ).adelete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.DeleteModel": 1})
        child = await DoNothingChildModel.async_objects.aget(name="Child1")
        self.assertEqual(child.parent_id, self.parent.pk)


class TestADeleteCustomOnDelete(AsyncioTestCase):
    async def test_async_handler_is_used(self):
        parent = AsyncOnDeleteParentModel(name="Parent")
        await parent.async_save()
        await AsyncOnDeleteChildModel(
            name="Child1", parent=parent
        ).async_save()

        count, per_model = await AsyncOnDeleteParentModel.async_objects.filter(
            name="Parent"
        ).adelete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.AsyncOnDeleteParentModel": 1})
        child = await AsyncOnDeleteChildModel.async_objects.aget(name="Child1")
        self.assertIsNone(child.parent_id)

    async def test_sync_handler_is_rejected(self):
        parent = SyncOnDeleteParentModel(name="Parent")
        await parent.async_save()

        with self.assertRaises(TypeError) as ctx:
            await SyncOnDeleteParentModel.async_objects.filter(
                name="Parent"
            ).adelete()

        self.assertIn(
            "async delete does not support the synchronous on_delete handler",
            str(ctx.exception),
        )


class TestADeleteSignals(AsyncioTestCase):
    def setUp(self):
        self.sent = []
        signals.pre_delete.connect(self.pre_delete, sender=DeleteModel)
        signals.post_delete.connect(self.post_delete, sender=DeleteModel)
        self.addCleanup(
            signals.pre_delete.disconnect, self.pre_delete, sender=DeleteModel
        )
        self.addCleanup(
            signals.post_delete.disconnect,
            self.post_delete,
            sender=DeleteModel,
        )

    def pre_delete(self, sender, instance, **kwargs):
        self.sent.append(("pre_delete", instance.name, instance.pk))

    def post_delete(self, sender, instance, **kwargs):
        self.sent.append(("post_delete", instance.name, instance.pk))

    async def test_sends_pre_and_post_delete(self):
        instance = DeleteModel(name="Parent", value=1)
        await instance.async_save()

        await DeleteModel.async_objects.filter(name="Parent").adelete()

        self.assertEqual(
            self.sent,
            [
                ("pre_delete", "Parent", instance.pk),
                ("post_delete", "Parent", instance.pk),
            ],
        )
