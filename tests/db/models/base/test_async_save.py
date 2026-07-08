from unittest import mock

from django.db.models import Value
from django.test import override_settings
from test_app.models import (
    DbDefaultModel,
    OrderItemModel,
    OrderParentModel,
    PkOnlyModel,
    RelatedSaveModel,
    SaveChildModel,
    SaveModel,
    SaveParentModel,
    SaveProxyModel,
    SelectOnSaveModel,
    UuidPkModel,
)

from django_async_backend.db import async_connections
from django_async_backend.db.models.query import QuerySet
from django_async_backend.test import AsyncioTestCase


class SaveModelToOtherRouter:

    def db_for_write(self, model, **hints):
        if model is SaveModel:
            return "other"
        return None

    def db_for_read(self, model, **hints):
        return None

    def allow_relation(self, *args, **hints):
        return True

    def allow_migrate(self, *args, **hints):
        return True


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

    @override_settings(DATABASE_ROUTERS=[SaveModelToOtherRouter()])
    async def test_uses_router_db_for_write_when_using_omitted(self):
        instance = SaveModel(name="ItemRouter", value=1)

        await instance.async_save()

        self.assertEqual(instance._state.db, "other")

    @override_settings(DATABASE_ROUTERS=[SaveModelToOtherRouter()])
    async def test_explicit_using_skips_router_db_for_write(self):
        instance = SaveModel(name="ItemRouter2", value=1)

        await instance.async_save(using="default")

        self.assertEqual(instance._state.db, "default")

    async def test_error_during_save_marks_transaction_for_rollback(self):
        connection = async_connections["default"]
        instance = SaveModel(name="RollbackItem", value=1)
        # accepted mock
        boom = mock.AsyncMock(side_effect=RuntimeError("boom"))

        self.assertTrue(connection.in_atomic_block)
        self.assertFalse(connection.needs_rollback)

        # accepted mock
        with mock.patch.object(SaveModel, "_async_save_table", boom):
            with self.assertRaises(RuntimeError):
                await instance.async_save()

        self.assertTrue(connection.needs_rollback)
        self.assertIsInstance(connection.rollback_exc, RuntimeError)

        connection.needs_rollback = False

    async def test_insert_drops_db_returning_field_when_backend_cannot_return(
        self,
    ):
        features = async_connections["default"].features
        instance = DbDefaultModel(name="Item8", ts=99)
        # accepted mock
        insert_mock = mock.AsyncMock(return_value=[])

        # accepted mock
        with mock.patch.object(
            type(features), "can_return_columns_from_insert", False
        ), mock.patch.object(DbDefaultModel, "_async_do_insert", insert_mock):
            await instance.async_save()

        returning_fields = insert_mock.call_args.args[3]
        returning_names = {f.name for f in returning_fields}
        self.assertNotIn("ts", returning_names)
        self.assertIn("id", returning_names)

    async def test_deferred_instance_saves_only_loaded_fields(self):
        instance = SaveModel(name="Deferred", value=1)
        await instance.async_save()

        # Build an instance with "value" deferred (only id/name loaded).
        loaded = SaveModel.from_db(
            "default", ["id", "name"], [instance.pk, "Deferred"]
        )
        loaded.name = "Deferred-changed"
        # "value" is deferred; the save should auto-target only loaded fields
        # and must not overwrite the deferred column.
        await loaded.async_save()

        stored = await SaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.name, "Deferred-changed")
        self.assertEqual(stored.value, 1)

    async def test_proxy_model_saves_to_concrete_table(self):
        instance = SaveProxyModel(name="ProxyItem", value=3)

        await instance.async_save()

        stored = await SaveModel.async_objects.aget(name="ProxyItem")
        self.assertEqual(stored.value, 3)

    async def test_force_update_without_pk_raises(self):
        instance = SaveModel(name="NoPkForceUpdate", value=1)

        with self.assertRaises(ValueError) as cm:
            await instance.async_save(force_update=True)

        self.assertEqual(
            str(cm.exception),
            "Cannot force an update in save() with no primary key.",
        )

    async def test_pk_with_default_forces_insert(self):
        instance = UuidPkModel(name="UuidItem")

        await instance.async_save()

        self.assertIsNotNone(instance.pk)
        stored = await UuidPkModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.name, "UuidItem")

    async def test_assign_returned_values_defers_stale_fields(self):
        instance = SaveModel(name="Assign", value=1)
        name_field = SaveModel._meta.get_field("name")
        value_field = SaveModel._meta.get_field("value")

        # Fewer returned values than returning fields: the first field is
        # assigned and the leftover field is deferred (dropped from __dict__).
        instance._async_assign_returned_values([99], [name_field, value_field])

        self.assertEqual(instance.name, 99)
        self.assertNotIn("value", instance.__dict__)


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

    async def test_fields_argument_skips_unlisted_relations(self):
        unsaved = SaveModel(name="unsaved-skipped")
        instance = RelatedSaveModel(name="Rel9", fk=unsaved)
        only_name = [RelatedSaveModel._meta.get_field("name")]

        # The unsaved fk would normally raise, but it is not in ``fields`` so
        # the check is skipped entirely.
        await instance._async_prepare_related_fields_for_save(
            operation_name="save", fields=only_name
        )

    async def test_unsaved_generic_foreign_key_is_checked(self):
        instance = SaveModel(name="GenericHost", value=1)
        # Simulate a GenericForeignKey private field pointing at an unsaved
        # related object (avoids depending on the contenttypes app).
        # accepted mock
        generic_field = mock.MagicMock()
        generic_field.is_relation = True
        generic_field.is_cached.return_value = True
        generic_field.fk_field = "object_id"
        generic_field.name = "content_object"
        generic_field.get_cached_value.return_value = SaveModel(
            name="unsaved-generic"
        )
        name_field = SaveModel._meta.get_field("name")

        # accepted mock
        with mock.patch.object(
            SaveModel._meta, "private_fields", [generic_field]
        ):
            # Excluded from ``fields`` -> skipped without raising.
            await instance._async_prepare_related_fields_for_save(
                operation_name="save", fields=[name_field]
            )
            # Included (fields=None) -> the unsaved relation raises.
            with self.assertRaises(ValueError) as cm:
                await instance._async_prepare_related_fields_for_save(
                    operation_name="save"
                )

        self.assertIn(
            "unsaved related object 'content_object'", str(cm.exception)
        )


class TestAsyncValidateForceInsert(AsyncioTestCase):
    async def test_force_insert_false_returns_empty_tuple(self):
        self.assertEqual(SaveModel._async_validate_force_insert(False), ())

    async def test_force_insert_true_returns_cls_tuple(self):
        self.assertEqual(
            SaveModel._async_validate_force_insert(True), (SaveModel,)
        )

    async def test_force_insert_valid_tuple_is_returned(self):
        value = (SaveModel,)
        self.assertIs(SaveModel._async_validate_force_insert(value), value)

    async def test_force_insert_non_bool_non_tuple_raises(self):
        with self.assertRaises(TypeError) as cm:
            SaveModel._async_validate_force_insert(1)

        self.assertEqual(
            str(cm.exception), "force_insert must be a bool or tuple."
        )

    async def test_force_insert_member_not_model_raises(self):
        with self.assertRaises(TypeError) as cm:
            SaveModel._async_validate_force_insert((object,))

        self.assertIn("must be a model subclass", str(cm.exception))

    async def test_force_insert_member_not_a_base_raises(self):
        with self.assertRaises(TypeError) as cm:
            SaveModel._async_validate_force_insert((RelatedSaveModel,))

        self.assertIn("must be a base of", str(cm.exception))

    async def test_async_save_with_force_insert_true_creates_row(self):
        instance = SaveModel(name="ForceInsert", value=7)

        await instance.async_save(force_insert=True)

        self.assertIsNotNone(instance.pk)
        stored = await SaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.value, 7)


class TestAsyncSaveParents(AsyncioTestCase):
    async def test_multi_table_inheritance_insert(self):
        instance = SaveChildModel(parent_value=1, child_value=2)

        await instance.async_save()

        self.assertIsNotNone(instance.pk)
        # The child PK is linked to the parent PK.
        self.assertEqual(instance.pk, instance.saveparentmodel_ptr_id)
        parent = await SaveParentModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(parent.parent_value, 1)
        child = await SaveChildModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(child.child_value, 2)
        self.assertEqual(child.parent_value, 1)

    async def test_multi_table_inheritance_update(self):
        instance = SaveChildModel(parent_value=1, child_value=2)
        await instance.async_save()

        instance.parent_value = 10
        instance.child_value = 20
        await instance.async_save()

        child = await SaveChildModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(child.parent_value, 10)
        self.assertEqual(child.child_value, 20)

    async def test_multi_table_inheritance_update_child_field_only(self):
        instance = SaveChildModel(parent_value=1, child_value=2)
        await instance.async_save()

        instance.child_value = 99
        # update_fields only targets the child table which forces the parent
        # table save to go through _async_do_update with empty values.
        await instance.async_save(update_fields=["child_value"])

        child = await SaveChildModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(child.child_value, 99)
        self.assertEqual(child.parent_value, 1)

    async def test_save_parents_skips_already_updated_parent(self):
        instance = SaveChildModel(parent_value=1, child_value=2)

        inserted = await instance._async_save_parents(
            SaveChildModel,
            "default",
            None,
            (),
            updated_parents={SaveParentModel: True},
        )

        # Parent was reported as already updated, so nothing was inserted.
        self.assertFalse(inserted)

    async def test_save_parents_marks_insert_for_not_updated_parent(self):
        instance = SaveChildModel(parent_value=1, child_value=2)

        inserted = await instance._async_save_parents(
            SaveChildModel,
            "default",
            None,
            (),
            updated_parents={SaveParentModel: False},
        )

        # Parent was reported as inserted (not updated).
        self.assertTrue(inserted)

    async def test_save_child_with_preassigned_saved_parent(self):
        parent = SaveParentModel(parent_value=1)
        await parent.async_save()

        child = SaveChildModel(parent_value=7, child_value=2)
        # Assigning a saved parent caches the link and sets its attname; the
        # parent PK must be synced onto the child and the cache cleared.
        child.saveparentmodel_ptr = parent
        await child.async_save()

        self.assertEqual(child.pk, parent.pk)
        self.assertIsNone(
            SaveChildModel._meta.get_field(
                "saveparentmodel_ptr"
            ).get_cached_value(child, default=None)
        )
        stored = await SaveChildModel.async_objects.aget(pk=child.pk)
        self.assertEqual(stored.child_value, 2)
        self.assertEqual(stored.parent_value, 7)

    async def test_save_parents_syncs_parent_pk_from_link(self):
        parent = SaveParentModel(parent_value=1)
        await parent.async_save()

        child = SaveChildModel(parent_value=1, child_value=2)
        # Set the link attname directly (bypassing the descriptor) so the
        # parent PK attname stays None and must be back-filled from the link.
        child.saveparentmodel_ptr_id = parent.pk
        self.assertIsNone(child.id)

        await child._async_save_parents(SaveChildModel, "default", None, ())

        self.assertEqual(child.id, parent.pk)

    async def test_multi_table_inheritance_marks_rollback_on_error(self):
        connection = async_connections["default"]
        instance = SaveChildModel(parent_value=1, child_value=2)
        # accepted mock
        boom = mock.AsyncMock(side_effect=RuntimeError("boom"))

        # accepted mock
        with mock.patch.object(SaveChildModel, "_async_save_table", boom):
            with self.assertRaises(RuntimeError):
                await instance.async_save()

        self.assertTrue(connection.needs_rollback)
        connection.needs_rollback = False


class TestAsyncDoUpdate(AsyncioTestCase):
    async def test_update_existing_row(self):
        instance = SaveModel(name="UpdateMe", value=1)
        await instance.async_save()

        instance.value = 2
        await instance.async_save()

        stored = await SaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.value, 2)

    async def test_update_with_expression_value_is_read_back(self):
        instance = SaveModel(name="ExprUpdate", value=1)
        await instance.async_save()

        instance.value = Value(41) + Value(1)
        await instance.async_save()

        self.assertEqual(instance.value, 42)
        stored = await SaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.value, 42)

    async def test_force_update_missing_row_raises_not_updated(self):
        instance = SaveModel(name="ForceUpdateMissing", value=1)
        await instance.async_save()

        instance.pk = 987654
        with self.assertRaises(SaveModel.NotUpdated) as cm:
            await instance.async_save(force_update=True)

        self.assertEqual(
            str(cm.exception), "Forced update did not affect any rows."
        )

    async def test_update_fields_missing_row_raises_not_updated(self):
        instance = SaveModel(name="UpdateFieldsMissing", value=1)
        await instance.async_save()

        instance.pk = 876543
        instance.value = 2
        with self.assertRaises(SaveModel.NotUpdated) as cm:
            await instance.async_save(update_fields=["value"])

        self.assertEqual(
            str(cm.exception),
            "Save with update_fields did not affect any rows.",
        )

    async def test_pk_only_update_missing_row_falls_back_to_insert(self):
        instance = PkOnlyModel(id=424242)

        await instance.async_save()

        self.assertTrue(
            await PkOnlyModel.async_objects.filter(pk=424242).aexists()
        )

    async def test_pk_only_resave_is_noop_update(self):
        instance = PkOnlyModel()
        await instance.async_save()

        # Second save has no values to update but the row exists.
        await instance.async_save()

        self.assertEqual(
            await PkOnlyModel.async_objects.filter(pk=instance.pk).acount(), 1
        )

    async def test_select_on_save_update(self):
        instance = SelectOnSaveModel(name="SoS1", value=1)
        await instance.async_save()

        instance.value = 5
        await instance.async_save()

        stored = await SelectOnSaveModel.async_objects.aget(pk=instance.pk)
        self.assertEqual(stored.value, 5)

    async def test_select_on_save_missing_row_falls_back_to_insert(self):
        instance = SelectOnSaveModel(name="SoS2", value=1)
        await instance.async_save()

        instance.pk = 999999
        instance.name = "SoS2-moved"
        await instance.async_save()

        self.assertTrue(
            await SelectOnSaveModel.async_objects.filter(pk=999999).aexists()
        )

    async def test_select_on_save_update_zero_rows_but_row_exists(self):
        instance = SelectOnSaveModel(name="SoS3", value=1)
        await instance.async_save()

        instance.value = 2
        # accepted mock
        update_mock = mock.AsyncMock(return_value=[])
        # accepted mock
        with mock.patch.object(QuerySet, "_update", update_mock):
            await instance.async_save()

        # The update reported zero rows but the row still exists, so the save
        # is treated as a successful (no-op) update rather than an insert.
        self.assertEqual(
            await SelectOnSaveModel.async_objects.filter(
                pk=instance.pk
            ).acount(),
            1,
        )


class TestAsyncOrderWithRespectTo(AsyncioTestCase):
    async def test_order_is_auto_populated_on_insert(self):
        parent = OrderParentModel(name="OrderParent")
        await parent.async_save()

        first = OrderItemModel(parent=parent)
        await first.async_save()
        second = OrderItemModel(parent=parent)
        await second.async_save()
        third = OrderItemModel(parent=parent)
        await third.async_save()

        self.assertEqual(first._order, 0)
        self.assertEqual(second._order, 1)
        self.assertEqual(third._order, 2)

    async def test_order_is_scoped_per_parent(self):
        parent_a = OrderParentModel(name="ParentA")
        await parent_a.async_save()
        parent_b = OrderParentModel(name="ParentB")
        await parent_b.async_save()

        a_item = OrderItemModel(parent=parent_a)
        await a_item.async_save()
        b_item = OrderItemModel(parent=parent_b)
        await b_item.async_save()

        # Each parent gets its own independent ordering starting at 0.
        self.assertEqual(a_item._order, 0)
        self.assertEqual(b_item._order, 0)
