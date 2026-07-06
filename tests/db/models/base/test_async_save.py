from unittest import mock

from django.db.models import Value
from test_app.models import (
    DbDefaultModel,
    RelatedSaveModel,
    SaveModel,
)

from django_async_backend.db import async_connections
from django_async_backend.test import AsyncioTestCase


class TestAsyncSave(AsyncioTestCase):
    async def test_insert_creates_row(self):
        instance = SaveModel(name="Item1", value=1)

        await instance.async_save()

        stored = await SaveModel.async_objects.aget(name="Item1")
        self.assertEqual(stored.pk, instance.pk)
        self.assertEqual(stored.value, 1)

    async def test_insert_assigns_pk(self):
        instance = SaveModel(name="Item2")

        self.assertIsNone(instance.pk)
        await instance.async_save()

        self.assertIsNotNone(instance.pk)
        self.assertTrue(
            await SaveModel.async_objects.filter(pk=instance.pk).aexists()
        )

    async def test_force_insert_with_force_update_raises(self):
        instance = SaveModel(name="Item3", value=1)

        with self.assertRaises(ValueError) as cm:
            await instance.async_save(force_insert=True, force_update=True)

        self.assertEqual(
            str(cm.exception),
            "Cannot force both insert and updating in model saving.",
        )

    async def test_force_insert_with_update_fields_raises(self):
        instance = SaveModel(name="Item4", value=1)

        with self.assertRaises(ValueError) as cm:
            await instance.async_save(
                force_insert=True, update_fields=["value"]
            )

        self.assertEqual(
            str(cm.exception),
            "Cannot force both insert and updating in model saving.",
        )

    async def test_empty_update_fields_is_noop(self):
        instance = SaveModel(name="Item5", value=1)

        await instance.async_save(update_fields=[])

        self.assertIsNone(instance.pk)
        self.assertFalse(
            await SaveModel.async_objects.filter(name="Item5").aexists()
        )

    async def test_unknown_update_fields_raise(self):
        instance = SaveModel(name="Item6", value=1)
        await instance.async_save()

        with self.assertRaises(ValueError):
            await instance.async_save(update_fields=["missing"])

    async def test_insert_with_expression_value_is_read_back(self):
        instance = SaveModel(name="Item7", value=Value(41) + Value(1))

        await instance.async_save()

        self.assertEqual(instance.value, 42)
        stored = await SaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.value, 42)

    async def test_insert_drops_db_returning_field_when_backend_cannot_return(
        self,
    ):
        features = async_connections["default"].features
        instance = DbDefaultModel(name="Item8", ts=99)
        insert_mock = mock.AsyncMock(return_value=[])

        with mock.patch.object(
            type(features), "can_return_columns_from_insert", False
        ), mock.patch.object(DbDefaultModel, "_async_do_insert", insert_mock):
            await instance.async_save()

        returning_fields = insert_mock.call_args.args[3]
        returning_names = {f.name for f in returning_fields}
        self.assertNotIn("ts", returning_names)
        self.assertIn("id", returning_names)


class TestAsyncPrepareRelatedFieldsForSave(AsyncioTestCase):
    async def test_unsaved_fk_raises(self):
        unsaved = SaveModel(name="unsaved-fk")
        instance = RelatedSaveModel(name="Rel1", fk=unsaved)

        with self.assertRaises(ValueError):
            await instance.async_save()

    async def test_unsaved_o2o_raises(self):
        unsaved = SaveModel(name="unsaved-o2o")
        instance = RelatedSaveModel(name="Rel3", o2o=unsaved)

        with self.assertRaises(ValueError):
            await instance.async_save()

    async def test_related_saved_after_assignment_sets_attname(self):
        related = SaveModel(name="backfill-parent")
        instance = RelatedSaveModel(name="Rel4", fk=related)
        self.assertIsNone(instance.fk_id)

        await related.async_save()
        await instance.async_save()

        stored = await RelatedSaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.fk_id, related.pk)

    async def test_saved_relation_saves_normally(self):
        related = SaveModel(name="saved-parent")
        await related.async_save()

        instance = RelatedSaveModel(name="Rel5", fk=related)
        await instance.async_save()

        stored = await RelatedSaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.fk_id, related.pk)

    async def test_null_relations_save_normally(self):
        instance = RelatedSaveModel(name="Rel6")

        await instance.async_save()

        self.assertTrue(
            await RelatedSaveModel.async_objects.filter(
                pk=instance.pk
            ).aexists()
        )

    async def test_cached_none_relation_is_skipped(self):
        instance = RelatedSaveModel(name="Rel7")
        instance.fk = None
        self.assertTrue(
            RelatedSaveModel._meta.get_field("fk").is_cached(instance)
        )

        await instance.async_save()

        self.assertTrue(
            await RelatedSaveModel.async_objects.filter(
                pk=instance.pk
            ).aexists()
        )

    async def test_changed_related_pk_clears_cached_relation(self):
        parent = SaveModel(name="cache-parent")
        await parent.async_save()
        original_pk = parent.pk

        instance = RelatedSaveModel(name="Rel8", fk=parent)
        parent.pk = original_pk + 1000
        fk_field = RelatedSaveModel._meta.get_field("fk")
        self.assertTrue(fk_field.is_cached(instance))

        await instance.async_save()

        self.assertFalse(fk_field.is_cached(instance))
        stored = await RelatedSaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.fk_id, original_pk)
