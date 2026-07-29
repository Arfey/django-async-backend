from django.contrib.contenttypes.models import ContentType
from django.db.models import signals
from django.db.models.deletion import (
    ProtectedError,
    RestrictedError,
)
from test_app.models import (
    CascadeChildModel,
    ChildModel,
    DeleteModel,
    FastDeleteModel,
    GenericChildModel,
    GenericRelationModel,
    ParentModel,
    ProtectChildModel,
    RestrictChildModel,
    SetNullChildModel,
)

from django_async_backend.test import AsyncioTestCase


class TestAsyncDelete(AsyncioTestCase):
    async def test_deletes_row(self):
        instance = FastDeleteModel(name="Item1")
        await instance.async_save()

        count, per_model = await instance.async_delete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.FastDeleteModel": 1})
        self.assertEqual(await FastDeleteModel.async_objects.acount(), 0)

    async def test_clears_pk(self):
        instance = FastDeleteModel(name="Item1")
        await instance.async_save()

        await instance.async_delete()

        self.assertIsNone(instance.pk)

    async def test_leaves_other_rows(self):
        instance = FastDeleteModel(name="Item1")
        await instance.async_save()
        await FastDeleteModel(name="Item2").async_save()

        await instance.async_delete()

        names = [obj.name async for obj in FastDeleteModel.async_objects.all()]
        self.assertEqual(names, ["Item2"])

    async def test_unsaved_instance_raises(self):
        instance = FastDeleteModel(name="Item1")

        with self.assertRaises(ValueError) as ctx:
            await instance.async_delete()

        self.assertIn("can't be deleted", str(ctx.exception))

    async def test_using_selects_the_database(self):
        instance = FastDeleteModel(name="Item1")
        await instance.async_save(using="other")

        count, _ = await instance.async_delete(using="other")

        self.assertEqual(count, 1)


class TestAsyncDeleteCascade(AsyncioTestCase):
    async def asyncSetUp(self):
        self.parent = DeleteModel(name="Parent", value=1)
        await self.parent.async_save()

    async def test_cascades_to_children(self):
        await CascadeChildModel(name="Child1", parent=self.parent).async_save()
        await CascadeChildModel(name="Child2", parent=self.parent).async_save()

        count, per_model = await self.parent.async_delete()

        self.assertEqual(count, 3)
        self.assertEqual(
            per_model,
            {
                "test_app.DeleteModel": 1,
                "test_app.CascadeChildModel": 2,
            },
        )
        self.assertEqual(await CascadeChildModel.async_objects.acount(), 0)

    async def test_set_null_nulls_the_fk(self):
        await SetNullChildModel(name="Child1", parent=self.parent).async_save()

        count, per_model = await self.parent.async_delete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.DeleteModel": 1})
        child = await SetNullChildModel.async_objects.aget(name="Child1")
        self.assertIsNone(child.parent_id)

    async def test_protect_raises(self):
        await ProtectChildModel(name="Child1", parent=self.parent).async_save()

        with self.assertRaises(ProtectedError):
            await self.parent.async_delete()

        self.assertEqual(await DeleteModel.async_objects.acount(), 1)

    async def test_restrict_raises(self):
        await RestrictChildModel(
            name="Child1", parent=self.parent
        ).async_save()

        with self.assertRaises(RestrictedError):
            await self.parent.async_delete()

        self.assertEqual(await DeleteModel.async_objects.acount(), 1)


class TestAsyncDeleteGenericRelation(AsyncioTestCase):
    async def asyncSetUp(self):
        # The contenttypes cache outlives a single test, so start cold to keep
        # these tests independent of execution order.
        ContentType.objects.clear_cache()
        self.addCleanup(ContentType.objects.clear_cache)
        self.parent = GenericRelationModel(name="Parent")
        await self.parent.async_save()
        self.content_type = await ContentType.async_objects.aget(
            app_label="test_app", model="genericrelationmodel"
        )

    async def add_child(self, name):
        await GenericChildModel(
            name=name,
            content_type=self.content_type,
            object_id=self.parent.pk,
        ).async_save()

    async def test_cascades_to_generic_children(self):
        await self.add_child("Child1")
        await self.add_child("Child2")

        count, per_model = await self.parent.async_delete()

        self.assertEqual(count, 3)
        self.assertEqual(
            per_model,
            {
                "test_app.GenericRelationModel": 1,
                "test_app.GenericChildModel": 2,
            },
        )
        self.assertEqual(await GenericChildModel.async_objects.acount(), 0)

    async def test_leaves_generic_children_of_other_rows(self):
        await self.add_child("Child1")
        other = GenericRelationModel(name="Other")
        await other.async_save()
        await GenericChildModel(
            name="OtherChild",
            content_type=self.content_type,
            object_id=other.pk,
        ).async_save()

        await self.parent.async_delete()

        names = [
            obj.name async for obj in GenericChildModel.async_objects.all()
        ]
        self.assertEqual(names, ["OtherChild"])

    async def test_recreates_missing_content_type(self):
        """Django's get_for_model() creates the row when it is absent, so the
        delete must succeed rather than raising DoesNotExist.
        """
        await ContentType.async_objects.filter(
            pk=self.content_type.pk
        ).adelete()

        count, per_model = await self.parent.async_delete()

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.GenericRelationModel": 1})
        self.assertTrue(
            await ContentType.async_objects.filter(
                app_label="test_app", model="genericrelationmodel"
            ).aexists()
        )

    async def test_reuses_cached_content_type(self):
        """A warm cache means no lookup at all -- proven by deleting the row
        and showing the next delete neither fails nor recreates it.
        """
        other = GenericRelationModel(name="Other")
        await other.async_save()
        await self.parent.async_delete()

        await ContentType.async_objects.filter(
            pk=self.content_type.pk
        ).adelete()

        await other.async_delete()

        self.assertFalse(
            await ContentType.async_objects.filter(
                app_label="test_app", model="genericrelationmodel"
            ).aexists()
        )


class TestAsyncDeleteInheritance(AsyncioTestCase):
    async def test_deletes_parent_row(self):
        child = ChildModel(parent_value=1, child_value=2)
        await child.async_save()

        count, per_model = await child.async_delete()

        self.assertEqual(count, 2)
        self.assertEqual(
            per_model,
            {
                "test_app.ChildModel": 1,
                "test_app.ParentModel": 1,
            },
        )
        self.assertEqual(await ParentModel.async_objects.acount(), 0)

    async def test_keep_parents_leaves_parent_row(self):
        child = ChildModel(parent_value=1, child_value=2)
        await child.async_save()

        count, per_model = await child.async_delete(keep_parents=True)

        self.assertEqual(count, 1)
        self.assertEqual(per_model, {"test_app.ChildModel": 1})
        self.assertEqual(await ParentModel.async_objects.acount(), 1)


class TestAsyncDeleteSignals(AsyncioTestCase):
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

    def pre_delete(self, sender, instance, origin, **kwargs):
        self.sent.append(("pre_delete", instance.name, origin))

    def post_delete(self, sender, instance, origin, **kwargs):
        self.sent.append(("post_delete", instance.name, origin))

    async def test_sends_pre_and_post_delete_with_origin(self):
        instance = DeleteModel(name="Parent", value=1)
        await instance.async_save()

        await instance.async_delete()

        self.assertEqual(
            self.sent,
            [
                ("pre_delete", "Parent", instance),
                ("post_delete", "Parent", instance),
            ],
        )
