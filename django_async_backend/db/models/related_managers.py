"""
Async related manager support for reverse FK and M2M relations.

Adds aadd, aremove, aclear, aset, acreate, aget_or_create, aupdate_or_create
to Django's dynamically created related managers for AsyncModel subclasses.
"""

from django.db import router
from django.db.models import signals
from django.db.models.fields.related_descriptors import (
    create_forward_many_to_many_manager,
    create_reverse_many_to_one_manager,
)

from django_async_backend.db.transaction import async_atomic


def _get_async_queryset(manager, *, for_write=False):
    """Build an async QuerySet with the relation filters applied."""
    from django_async_backend.db.models.query import QuerySet

    if for_write:
        db = manager._db or router.db_for_write(manager.model, instance=manager.instance)
    else:
        db = manager._db or router.db_for_read(manager.model, instance=manager.instance)
    qs = QuerySet(model=manager.model, using=db)
    return manager._apply_rel_filters(qs)


def _get_through_queryset(manager, *, db=None):
    """Build an async QuerySet on the through model."""
    from django_async_backend.db.models.query import QuerySet

    if db is None:
        db = manager._db or router.db_for_write(manager.through, instance=manager.instance)
    return QuerySet(model=manager.through, using=db)


# ---------------------------------------------------------------------------
# Reverse FK async mixin
# ---------------------------------------------------------------------------


class AsyncReverseManyToOneMixin:
    async def aadd(self, *objs, bulk=True):
        self._check_fk_val()
        self._remove_prefetched_objects()
        db = router.db_for_write(self.model, instance=self.instance)

        def check_and_update_obj(obj):
            if not isinstance(obj, self.model):
                raise TypeError("'%s' instance expected, got %r" % (self.model._meta.object_name, obj))
            setattr(obj, self.field.name, self.instance)

        if bulk:
            pks = []
            for obj in objs:
                check_and_update_obj(obj)
                if obj._state.adding or obj._state.db != db:
                    raise ValueError("%r instance isn't saved. Use bulk=False or save the object first." % obj)
                pks.append(obj.pk)
            from django_async_backend.db.models.query import QuerySet

            await QuerySet(model=self.model, using=db).filter(pk__in=pks).aupdate(**{self.field.name: self.instance})
        else:
            async with async_atomic(using=db, savepoint=False):
                for obj in objs:
                    check_and_update_obj(obj)
                    await obj.asave()

    aadd.alters_data = True

    async def acreate(self, **kwargs):
        self._check_fk_val()
        self._remove_prefetched_objects()
        kwargs[self.field.name] = self.instance
        qs = _get_async_queryset(self, for_write=True)
        return await qs.acreate(**kwargs)

    acreate.alters_data = True

    async def aget_or_create(self, **kwargs):
        self._check_fk_val()
        kwargs[self.field.name] = self.instance
        qs = _get_async_queryset(self, for_write=True)
        return await qs.aget_or_create(**kwargs)

    aget_or_create.alters_data = True

    async def aupdate_or_create(self, **kwargs):
        self._check_fk_val()
        kwargs[self.field.name] = self.instance
        qs = _get_async_queryset(self, for_write=True)
        return await qs.aupdate_or_create(**kwargs)

    aupdate_or_create.alters_data = True

    async def aset(self, objs, *, bulk=True, clear=False):
        self._check_fk_val()
        objs = tuple(objs)

        if self.field.null:
            db = router.db_for_write(self.model, instance=self.instance)
            async with async_atomic(using=db, savepoint=False):
                if clear:
                    await self.aclear(bulk=bulk)
                    await self.aadd(*objs, bulk=bulk)
                else:
                    old_objs = set()
                    async for obj in _get_async_queryset(self, for_write=True):
                        old_objs.add(obj)
                    new_objs = []
                    for obj in objs:
                        if obj in old_objs:
                            old_objs.discard(obj)
                        else:
                            new_objs.append(obj)
                    await self.aremove(*old_objs, bulk=bulk)
                    await self.aadd(*new_objs, bulk=bulk)
        else:
            await self.aadd(*objs, bulk=bulk)

    aset.alters_data = True


def _add_async_reverse_fk_nullable_methods(cls, rel):
    """Add aremove and aclear only if the FK field is nullable."""
    if not rel.field.null:
        return

    async def aremove(self, *objs, bulk=True):
        if not objs:
            return
        self._check_fk_val()
        val = self.field.get_foreign_related_value(self.instance)
        old_ids = set()
        for obj in objs:
            if not isinstance(obj, self.model):
                raise TypeError("'%s' instance expected, got %r" % (self.model._meta.object_name, obj))
            if self.field.get_local_related_value(obj) == val:
                old_ids.add(obj.pk)
            else:
                raise self.field.remote_field.model.DoesNotExist("%r is not related to %r." % (obj, self.instance))
        await self._aclear(_get_async_queryset(self, for_write=True).filter(pk__in=old_ids), bulk)

    aremove.alters_data = True
    cls.aremove = aremove

    async def aclear(self, *, bulk=True):
        self._check_fk_val()
        await self._aclear(_get_async_queryset(self, for_write=True), bulk)

    aclear.alters_data = True
    cls.aclear = aclear

    async def _aclear(self, queryset, bulk):
        self._remove_prefetched_objects()
        db = router.db_for_write(self.model, instance=self.instance)
        queryset = queryset.using(db)
        if bulk:
            await queryset.aupdate(**{self.field.name: None})
        else:
            async with async_atomic(using=db, savepoint=False):
                async for obj in queryset:
                    setattr(obj, self.field.name, None)
                    await obj.asave(update_fields=[self.field.name])

    _aclear.alters_data = True
    cls._aclear = _aclear


# ---------------------------------------------------------------------------
# M2M async mixin
# ---------------------------------------------------------------------------


class AsyncManyToManyMixin:
    async def aadd(self, *objs, through_defaults=None):
        self._remove_prefetched_objects()
        db = router.db_for_write(self.through, instance=self.instance)
        async with async_atomic(using=db, savepoint=False):
            await self._aadd_items(
                self.source_field_name,
                self.target_field_name,
                *objs,
                through_defaults=through_defaults,
            )
            if self.symmetrical:
                await self._aadd_items(
                    self.target_field_name,
                    self.source_field_name,
                    *objs,
                    through_defaults=through_defaults,
                )

    aadd.alters_data = True

    async def aremove(self, *objs):
        self._remove_prefetched_objects()
        await self._aremove_items(self.source_field_name, self.target_field_name, *objs)

    aremove.alters_data = True

    async def aclear(self):
        db = router.db_for_write(self.through, instance=self.instance)
        async with async_atomic(using=db, savepoint=False):
            await signals.m2m_changed.asend(
                sender=self.through,
                action="pre_clear",
                instance=self.instance,
                reverse=self.reverse,
                model=self.model,
                pk_set=None,
                using=db,
            )
            self._remove_prefetched_objects()
            filters = self._build_remove_filters(super().get_queryset().using(db))
            await _get_through_queryset(self, db=db).filter(filters).adelete()

            await signals.m2m_changed.asend(
                sender=self.through,
                action="post_clear",
                instance=self.instance,
                reverse=self.reverse,
                model=self.model,
                pk_set=None,
                using=db,
            )

    aclear.alters_data = True

    async def aset(self, objs, *, clear=False, through_defaults=None):
        objs = tuple(objs)
        db = router.db_for_write(self.through, instance=self.instance)
        async with async_atomic(using=db, savepoint=False):
            if clear:
                await self.aclear()
                await self.aadd(*objs, through_defaults=through_defaults)
            else:
                old_ids = set()
                async for val in (
                    _get_through_queryset(self, db=db)
                    .filter(**{self.source_field_name: self.related_val[0]})
                    .values_list(self.target_field_name, flat=True)
                ):
                    old_ids.add(val)

                new_objs = []
                for obj in objs:
                    fk_val = (
                        self.target_field.get_foreign_related_value(obj)[0]
                        if isinstance(obj, self.model)
                        else self.target_field.get_prep_value(obj)
                    )
                    if fk_val in old_ids:
                        old_ids.discard(fk_val)
                    else:
                        new_objs.append(obj)

                await self.aremove(*old_ids)
                await self.aadd(*new_objs, through_defaults=through_defaults)

    aset.alters_data = True

    async def acreate(self, *, through_defaults=None, **kwargs):
        qs = _get_async_queryset(self, for_write=True)
        new_obj = await qs.acreate(**kwargs)
        await self.aadd(new_obj, through_defaults=through_defaults)
        return new_obj

    acreate.alters_data = True

    async def aget_or_create(self, *, through_defaults=None, **kwargs):
        qs = _get_async_queryset(self, for_write=True)
        obj, created = await qs.aget_or_create(**kwargs)
        if created:
            await self.aadd(obj, through_defaults=through_defaults)
        return obj, created

    aget_or_create.alters_data = True

    async def aupdate_or_create(self, *, through_defaults=None, **kwargs):
        qs = _get_async_queryset(self, for_write=True)
        obj, created = await qs.aupdate_or_create(**kwargs)
        if created:
            await self.aadd(obj, through_defaults=through_defaults)
        return obj, created

    aupdate_or_create.alters_data = True

    async def _aadd_items(self, source_field_name, target_field_name, *objs, through_defaults=None):
        if not objs:
            return

        from django.db.models.utils import resolve_callables

        through_defaults = dict(resolve_callables(through_defaults or {}))
        target_ids = self._get_target_ids(target_field_name, objs)
        db = router.db_for_write(self.through, instance=self.instance)
        can_ignore_conflicts, must_send_signals, can_fast_add = self._get_add_plan(db, source_field_name)

        through_qs = _get_through_queryset(self, db=db)

        if can_fast_add:
            await through_qs.abulk_create(
                [
                    self.through(
                        **{
                            "%s_id" % source_field_name: self.related_val[0],
                            "%s_id" % target_field_name: target_id,
                        },
                    )
                    for target_id in target_ids
                ],
                ignore_conflicts=True,
            )
            return

        # Async version of _get_missing_target_ids — the sync version
        # uses Django's default manager which triggers SynchronousOnlyOperation.
        existing = set()
        async for val in (
            _get_through_queryset(self, db=db)
            .filter(
                **{
                    source_field_name: self.related_val[0],
                    "%s__in" % target_field_name: target_ids,
                },
            )
            .values_list(target_field_name, flat=True)
        ):
            existing.add(val)
        missing_target_ids = target_ids.difference(existing)

        async with async_atomic(using=db, savepoint=False):
            if must_send_signals:
                await signals.m2m_changed.asend(
                    sender=self.through,
                    action="pre_add",
                    instance=self.instance,
                    reverse=self.reverse,
                    model=self.model,
                    pk_set=missing_target_ids,
                    using=db,
                )

            await through_qs.abulk_create(
                [
                    self.through(
                        **through_defaults,
                        **{
                            "%s_id" % source_field_name: self.related_val[0],
                            "%s_id" % target_field_name: target_id,
                        },
                    )
                    for target_id in missing_target_ids
                ],
                ignore_conflicts=can_ignore_conflicts,
            )

            if must_send_signals:
                await signals.m2m_changed.asend(
                    sender=self.through,
                    action="post_add",
                    instance=self.instance,
                    reverse=self.reverse,
                    model=self.model,
                    pk_set=missing_target_ids,
                    using=db,
                )

    async def _aremove_items(self, source_field_name, target_field_name, *objs):  # noqa: ARG002
        if not objs:
            return

        old_ids = set()
        for obj in objs:
            if isinstance(obj, self.model):
                fk_val = self.target_field.get_foreign_related_value(obj)[0]
                old_ids.add(fk_val)
            else:
                old_ids.add(obj)

        db = router.db_for_write(self.through, instance=self.instance)
        async with async_atomic(using=db, savepoint=False):
            await signals.m2m_changed.asend(
                sender=self.through,
                action="pre_remove",
                instance=self.instance,
                reverse=self.reverse,
                model=self.model,
                pk_set=old_ids,
                using=db,
            )

            target_model_qs = super().get_queryset()
            if target_model_qs._has_filters():
                old_vals = target_model_qs.using(db).filter(
                    **{"%s__in" % self.target_field.target_field.attname: old_ids},
                )
            else:
                old_vals = old_ids
            filters = self._build_remove_filters(old_vals)
            await _get_through_queryset(self, db=db).filter(filters).adelete()

            await signals.m2m_changed.asend(
                sender=self.through,
                action="post_remove",
                instance=self.instance,
                reverse=self.reverse,
                model=self.model,
                pk_set=old_ids,
                using=db,
            )


# ---------------------------------------------------------------------------
# Factory wrappers
# ---------------------------------------------------------------------------


def create_async_reverse_manager(superclass, rel):
    """Create Django's reverse FK manager, then mix in async methods."""
    base_cls = create_reverse_many_to_one_manager(superclass, rel)
    cls = type(base_cls.__name__, (AsyncReverseManyToOneMixin, base_cls), {})
    _add_async_reverse_fk_nullable_methods(cls, rel)
    return cls


def create_async_m2m_manager(superclass, rel, reverse):
    """Create Django's M2M manager, then mix in async methods."""
    base_cls = create_forward_many_to_many_manager(superclass, rel, reverse)
    return type(base_cls.__name__, (AsyncManyToManyMixin, base_cls), {})


# ---------------------------------------------------------------------------
# class_prepared hook
# ---------------------------------------------------------------------------


def register_async_related_managers(sender):
    """Replace related_manager_cls on descriptors for AsyncModel subclasses."""
    from django.db.models.fields.related_descriptors import (
        ManyToManyDescriptor,
        ReverseManyToOneDescriptor,
    )

    for field in sender._meta.get_fields():
        accessor_name = None
        if field.one_to_many and hasattr(field, "get_accessor_name"):
            accessor_name = field.get_accessor_name()
        elif field.many_to_many:
            accessor_name = field.get_accessor_name() if hasattr(field, "get_accessor_name") else field.name

        if accessor_name is None:
            continue

        # The descriptor lives on the model class (or a parent).
        for klass in sender.__mro__:
            descriptor = klass.__dict__.get(accessor_name)
            if descriptor is not None:
                break
        else:
            continue

        if isinstance(descriptor, ManyToManyDescriptor):
            related_model = descriptor.rel.related_model if descriptor.reverse else descriptor.rel.model
            manager_cls = create_async_m2m_manager(
                related_model._default_manager.__class__,
                descriptor.rel,
                reverse=descriptor.reverse,
            )
            # Override the @cached_property by writing to the descriptor's __dict__
            descriptor.__dict__["related_manager_cls"] = manager_cls
        elif isinstance(descriptor, ReverseManyToOneDescriptor):
            related_model = descriptor.rel.related_model
            manager_cls = create_async_reverse_manager(
                related_model._default_manager.__class__,
                descriptor.rel,
            )
            descriptor.__dict__["related_manager_cls"] = manager_cls
