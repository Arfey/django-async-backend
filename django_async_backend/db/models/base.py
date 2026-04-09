"""
Async model mixin providing asave() and adelete() for Django models.

Usage:
    from django.db import models
    from django_async_backend.db.models.base import AsyncModel

    class MyModel(AsyncModel, models.Model):
        name = models.CharField(max_length=100)

Limitations:
    - No pre_save/post_save/pre_delete/post_delete signals
    - adelete() uses raw delete (no cascade handling)
"""

from django.db import connections, router

from django_async_backend.db.transaction import (
    async_atomic,
    async_mark_for_rollback_on_error,
)


class AsyncModel:
    """Mixin that adds truly async asave() and adelete() to Django models."""

    async def asave(
        self,
        *,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        """
        Async version of Model.save(). Skips pre_save/post_save signals.
        """
        self._prepare_related_fields_for_save(operation_name="save")

        using = using or router.db_for_write(self.__class__, instance=self)
        if force_insert and (force_update or update_fields):
            raise ValueError("Cannot force both insert and updating in model saving.")

        deferred_non_generated_fields = {
            f.attname for f in self._meta.concrete_fields if f.attname not in self.__dict__ and f.generated is False
        }
        if update_fields is not None:
            if not update_fields:
                return

            update_fields = frozenset(update_fields)
            field_names = self._meta._non_pk_concrete_field_names
            not_updatable_fields = update_fields.difference(field_names)

            if not_updatable_fields:
                raise ValueError(
                    "The following fields do not exist in this model, are m2m "
                    "fields, primary keys, or are non-concrete fields: %s" % ", ".join(not_updatable_fields)
                )
        elif not force_insert and deferred_non_generated_fields and using == self._state.db and self._is_pk_set():
            field_names = set()
            pk_fields = self._meta.pk_fields
            for field in self._meta.concrete_fields:
                if field not in pk_fields and not hasattr(field, "through"):
                    field_names.add(field.attname)
            loaded_fields = field_names.difference(deferred_non_generated_fields)
            if loaded_fields:
                update_fields = frozenset(loaded_fields)

        await self._asave_base(
            using=using,
            force_insert=force_insert,
            force_update=force_update,
            update_fields=update_fields,
        )

    asave.alters_data = True

    async def _asave_base(
        self,
        raw=False,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        """
        Async version of Model.save_base(). Skips signals.
        """
        using = using or router.db_for_write(self.__class__, instance=self)
        assert not (force_insert and (force_update or update_fields))
        assert update_fields is None or update_fields
        cls = self.__class__
        if cls._meta.proxy:
            cls = cls._meta.concrete_model
        meta = cls._meta

        # Signals skipped intentionally.

        if meta.parents:
            context_manager = async_atomic(using=using, savepoint=False)
        else:
            context_manager = async_mark_for_rollback_on_error(using=using)
        async with context_manager:
            parent_inserted = False
            if not raw:
                force_insert = self._validate_force_insert(force_insert)
                parent_inserted = await self._asave_parents(cls, using, update_fields, force_insert)
            updated = await self._asave_table(
                raw,
                cls,
                force_insert or parent_inserted,
                force_update,
                using,
                update_fields,
            )

        self._state.db = using
        self._state.adding = False
        return updated

    async def _asave_parents(
        self,
        cls,
        using,
        update_fields,
        force_insert,
        updated_parents=None,
    ):
        """Async version of Model._save_parents()."""
        meta = cls._meta
        inserted = False
        if updated_parents is None:
            updated_parents = {}
        for parent, field in meta.parents.items():
            if field and getattr(self, parent._meta.pk.attname) is None and getattr(self, field.attname) is not None:
                setattr(
                    self,
                    parent._meta.pk.attname,
                    getattr(self, field.attname),
                )
            if (parent_updated := updated_parents.get(parent)) is None:
                parent_inserted = await self._asave_parents(
                    cls=parent,
                    using=using,
                    update_fields=update_fields,
                    force_insert=force_insert,
                    updated_parents=updated_parents,
                )
                updated = await self._asave_table(
                    cls=parent,
                    using=using,
                    update_fields=update_fields,
                    force_insert=(parent_inserted or issubclass(parent, force_insert)),
                )
                if not updated:
                    inserted = True
                updated_parents[parent] = updated
            elif not parent_updated:
                inserted = True
            if field:
                setattr(self, field.attname, self._get_pk_val(parent._meta))
                if field.is_cached(self):
                    field.delete_cached_value(self)
        return inserted

    async def _asave_table(
        self,
        raw=False,
        cls=None,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        """Async version of Model._save_table()."""
        meta = cls._meta
        pk_fields = meta.pk_fields
        non_pks_non_generated = [f for f in meta.local_concrete_fields if f not in pk_fields and not f.generated]

        if update_fields:
            non_pks_non_generated = [
                f for f in non_pks_non_generated if f.name in update_fields or f.attname in update_fields
            ]

        if not self._is_pk_set(meta):
            pk_val = meta.pk.get_pk_value_on_save(self)
            setattr(self, meta.pk.attname, pk_val)
        pk_set = self._is_pk_set(meta)
        if not pk_set and (force_update or update_fields):
            raise ValueError("Cannot force an update in save() with no primary key.")
        updated = False
        if (
            not raw
            and not force_insert
            and not force_update
            and self._state.adding
            and all(f.has_default() or f.has_db_default() for f in meta.pk_fields)
        ):
            force_insert = True

        if pk_set and not force_insert:
            base_qs = self._get_async_queryset(cls, using)
            values = [
                (
                    f,
                    None,
                    (getattr(self, f.attname) if raw else f.pre_save(self, False)),
                )
                for f in non_pks_non_generated
            ]
            forced_update = update_fields or force_update
            pk_val = self._get_pk_val(meta)
            returning_fields = [
                f
                for f in meta.local_concrete_fields
                if (f.generated and f.referenced_fields.intersection(non_pks_non_generated))
            ]
            for field, _model, value in values:
                if (update_fields is None or field.name in update_fields) and hasattr(value, "resolve_expression"):
                    returning_fields.append(field)
            results = await self._ado_update(
                base_qs,
                using,
                pk_val,
                values,
                update_fields,
                forced_update,
                returning_fields,
            )
            if updated := bool(results):
                self._assign_returned_values(results[0], returning_fields)
            elif force_update:
                raise self.NotUpdated("Forced update did not affect any rows.")
            elif update_fields:
                raise self.NotUpdated("Save with update_fields did not affect any rows.")
        if not updated:
            insert_fields = [
                f for f in meta.local_concrete_fields if not f.generated and (pk_set or f is not meta.auto_field)
            ]
            returning_fields = list(meta.db_returning_fields)
            can_return_columns_from_insert = connections[using].features.can_return_columns_from_insert
            for field in insert_fields:
                value = getattr(self, field.attname) if raw else field.pre_save(self, add=True)
                if hasattr(value, "resolve_expression"):
                    if field not in returning_fields:
                        returning_fields.append(field)
                elif (
                    field.db_returning
                    and not can_return_columns_from_insert
                    and not (pk_set and field is meta.auto_field)
                ):
                    returning_fields.remove(field)
            results = await self._ado_insert(
                cls._base_manager,
                using,
                insert_fields,
                returning_fields,
                raw,
            )
            if results:
                self._assign_returned_values(results[0], returning_fields)
        return updated

    def _get_async_queryset(self, cls, using):
        """Get an async QuerySet for this model."""
        from django_async_backend.db.models.query import QuerySet

        return QuerySet(model=cls, using=using)

    async def _ado_update(
        self,
        base_qs,
        using,
        pk_val,
        values,
        update_fields,
        forced_update,
        returning_fields,
    ):
        """Async version of Model._do_update()."""
        # Use async queryset instead of Django's sync base_qs
        qs = self._get_async_queryset(self.__class__, using)
        filtered = qs.filter(pk=pk_val)
        if not values:
            if update_fields is not None or await filtered.aexists():
                return [()]
            return []
        if self._meta.select_on_save and not forced_update:
            if not await filtered.aexists():
                return []
            if results := await filtered._update(values, returning_fields):
                return results
            return [()] if await filtered.aexists() else []
        return await filtered._update(values, returning_fields)

    async def _ado_insert(self, manager, using, fields, returning_fields, raw):
        """Async version of Model._do_insert()."""
        # Use async queryset instead of Django's sync manager._insert
        qs = self._get_async_queryset(self.__class__, using)
        return await qs._insert(
            [self],
            fields=fields,
            returning_fields=returning_fields,
            using=using,
            raw=raw,
        )

    async def adelete(self, using=None, keep_parents=False):
        """
        Async delete of this model instance.

        Handles CASCADE and SET_NULL via AsyncCollector. Skips signals.
        """
        from django_async_backend.db.models.deletion import AsyncCollector

        if not self._is_pk_set():
            raise ValueError(
                "%s object can't be deleted because its %s attribute is set "
                "to None." % (self._meta.object_name, self._meta.pk.attname)
            )
        using = using or router.db_for_write(self.__class__, instance=self)

        collector = AsyncCollector(using=using, origin=self)
        await collector.acollect([self], keep_parents=keep_parents)
        return await collector.adelete()

    adelete.alters_data = True

    async def arefresh_from_db(self, using=None, fields=None, from_queryset=None):
        """
        Async version of Model.refresh_from_db(). Reloads field values
        from the database.
        """
        from django.db.models.constants import LOOKUP_SEP

        if fields is None:
            self._prefetched_objects_cache = {}
        else:
            prefetched_objects_cache = getattr(self, "_prefetched_objects_cache", ())
            fields = set(fields)
            for field in fields.copy():
                if field in prefetched_objects_cache:
                    del prefetched_objects_cache[field]
                    fields.remove(field)
            if not fields:
                return
            if any(LOOKUP_SEP in f for f in fields):
                raise ValueError(
                    'Found "%s" in fields argument. Relations and transforms are not allowed in fields.' % LOOKUP_SEP
                )

        qs = self._get_async_queryset(self.__class__, using or self._state.db or self.db)

        db_instance_qs = qs.filter(pk=self.pk)

        deferred_fields = self.get_deferred_fields()
        if fields is not None:
            db_instance_qs = db_instance_qs.only(*fields)
        elif deferred_fields:
            db_instance_qs = db_instance_qs.only(
                *{f.attname for f in self._meta.concrete_fields if f.attname not in deferred_fields},
            )

        db_instance = await db_instance_qs.aget()
        non_loaded_fields = db_instance.get_deferred_fields()
        for field in self._meta.fields:
            if field.attname in non_loaded_fields:
                continue
            if field.concrete:
                setattr(self, field.attname, getattr(db_instance, field.attname))
            if field.is_relation:
                if field.is_cached(db_instance):
                    field.set_cached_value(self, field.get_cached_value(db_instance))
                elif field.is_cached(self):
                    field.delete_cached_value(self)

        for rel in self._meta.related_objects:
            if (fields is None or rel.name in fields) and rel.is_cached(self):
                rel.delete_cached_value(self)

        for field in self._meta.private_fields:
            if (fields is None or field.name in fields) and field.is_relation and field.is_cached(self):
                field.delete_cached_value(self)

        self._state.db = db_instance._state.db
