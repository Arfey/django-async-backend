"""
Async deletion support with cascade handling.

Provides AsyncCollector, an async version of Django's Collector that
handles CASCADE, SET_NULL, SET_DEFAULT, and DO_NOTHING relationships.

Fires pre_delete/post_delete signals via asend().
"""

from collections import Counter, defaultdict
from functools import partial, reduce
from operator import attrgetter, or_
from typing import Any

from django.db import connections, models
from django.db.models import query_utils, signals
from django.db.models.deletion import (
    CASCADE,
    DO_NOTHING,
    ProtectedError,
    RestrictedError,
    get_candidate_relations_to_delete,
)

from django_async_backend.db.transaction import (
    async_atomic,
    async_mark_for_rollback_on_error,
)


class AsyncCollector:
    """
    Async version of Django's Collector for cascade deletes.

    Mirrors the structure of django.db.models.deletion.Collector but
    uses async DB calls. Signals are skipped.
    """

    def __init__(self, using, origin=None):
        self.using = using
        self.origin = origin
        self.data = defaultdict(set)
        self.field_updates = defaultdict(list)
        self.restricted_objects: defaultdict[Any, defaultdict[Any, set[Any]]] = defaultdict(partial(defaultdict, set))
        self.fast_deletes = []
        self.dependencies = defaultdict(set)
        self._pending_collects = []

    def collect(
        self,
        objs,
        source=None,
        nullable=False,
        collect_related=True,
        source_attr=None,
        reverse_dependency=False,
        keep_parents=False,
        fail_on_restricted=True,
    ):
        """
        Sync collect() for compatibility with Django's on_delete handlers.
        Queues objects for processing; cascade traversal happens in acollect.
        """
        if not objs:
            return None
        # Queue for later async processing
        self._pending_collects.append(
            (
                objs,
                source,
                nullable,
                collect_related,
                source_attr,
                reverse_dependency,
                keep_parents,
                fail_on_restricted,
            ),
        )
        # Still need to add objects to self.data immediately so the on_delete
        # handlers can track what's been collected
        new_objs = self.add(objs, source, nullable, reverse_dependency=reverse_dependency)
        return new_objs  # noqa: RET504

    def add(self, objs, source=None, nullable=False, reverse_dependency=False):
        if not objs:
            return []
        model = objs[0].__class__
        instances = self.data[model]
        new_objs = [obj for obj in objs if obj not in instances]
        instances.update(new_objs)
        if source is not None and not nullable:
            self.add_dependency(source, model, reverse_dependency=reverse_dependency)
        return new_objs

    def add_dependency(self, model, dependency, reverse_dependency=False):
        if reverse_dependency:
            model, dependency = dependency, model
        self.dependencies[model._meta.concrete_model].add(dependency._meta.concrete_model)

    def add_field_update(self, field, value, objs):
        self.field_updates[(field, value)].append(objs)

    def add_restricted_objects(self, field, objs):
        if objs:
            model = objs[0].__class__
            self.restricted_objects[model][field].update(objs)

    def _has_signal_listeners(self, model):
        return signals.pre_delete.has_listeners(model) or signals.post_delete.has_listeners(model)

    def can_fast_delete(self, objs, from_field=None):
        if from_field and from_field.remote_field.on_delete is not CASCADE:
            return False
        if hasattr(objs, "_meta"):
            model = objs._meta.model
        elif hasattr(objs, "model") and hasattr(objs, "_raw_delete"):
            model = objs.model
        else:
            return False
        opts = model._meta
        return (
            all(link == from_field for link in opts.concrete_model._meta.parents.values())
            and all(
                related.field.remote_field.on_delete is DO_NOTHING
                for related in get_candidate_relations_to_delete(opts)
            )
            and not any(hasattr(field, "bulk_related_objects") for field in opts.private_fields)
        )

    def get_del_batches(self, objs, fields):
        conn_batch_size = max(connections[self.using].ops.bulk_batch_size(fields, objs), 1)
        if len(objs) > conn_batch_size:
            return [objs[i : i + conn_batch_size] for i in range(0, len(objs), conn_batch_size)]
        return [objs]

    async def arelated_objects(self, related_model, related_fields, objs):
        from django_async_backend.db.models.query import QuerySet

        predicate = query_utils.Q.create(
            [(f"{related_field.name}__in", objs) for related_field in related_fields],
            connector=query_utils.Q.OR,
        )
        qs = QuerySet(model=related_model, using=self.using).filter(predicate)
        return [obj async for obj in qs]

    async def _drain_pending_collects(self):
        """Process any collect() calls queued by on_delete handlers."""
        while self._pending_collects:
            pending = list(self._pending_collects)
            self._pending_collects.clear()
            for (
                objs,
                _source,
                _nullable,
                collect_related,
                _source_attr,
                _reverse_dependency,
                keep_parents,
                fail_on_restricted,
            ) in pending:
                # Objects already added to self.data by collect().
                # Now do the async traversal for related objects.
                if not collect_related:
                    continue
                model = type(objs[0]) if objs else None
                if model is None:
                    continue
                await self._acollect_related(
                    objs,
                    model,
                    keep_parents,
                    fail_on_restricted,
                )

    async def _acollect_related(self, new_objs, model, keep_parents, _fail_on_restricted):
        """Async traversal of related objects for cascade handling."""
        model_fast_deletes = defaultdict(list)

        for related in get_candidate_relations_to_delete(model._meta):
            if keep_parents and related.model._meta.concrete_model in model._meta.all_parents:
                continue

            field = related.field
            on_delete = field.remote_field.on_delete

            if on_delete == DO_NOTHING:
                continue

            related_model = related.related_model

            if self.can_fast_delete(related_model, from_field=field):
                model_fast_deletes[related_model].append(field)
                continue

            batches = self.get_del_batches(new_objs, [field])
            for batch in batches:
                sub_objs_list = await self.arelated_objects(related_model, [field], batch)
                if getattr(on_delete, "lazy_sub_objs", False) or sub_objs_list:
                    on_delete(self, field, sub_objs_list, self.using)
                    await self._drain_pending_collects()

        for related_model, related_fields in model_fast_deletes.items():
            from django_async_backend.db.models.query import QuerySet

            batches = self.get_del_batches(new_objs, related_fields)
            for batch in batches:
                predicate = query_utils.Q.create(
                    [(f"{rf.name}__in", batch) for rf in related_fields],
                    connector=query_utils.Q.OR,
                )
                qs = QuerySet(model=related_model, using=self.using).filter(predicate)
                self.fast_deletes.append(qs)

    async def acollect(
        self,
        objs,
        source=None,
        nullable=False,
        collect_related=True,
        source_attr=None,  # noqa: ARG002
        reverse_dependency=False,
        keep_parents=False,
        fail_on_restricted=True,
    ):
        """
        Async version of Collector.collect(). Discovers related objects
        to delete, handling CASCADE, SET_NULL, etc.
        """
        if isinstance(objs, models.QuerySet):
            objs = [obj async for obj in objs]
        if not objs:
            return

        model = type(objs[0])

        # Fast path: if entire batch can be fast-deleted
        if self.can_fast_delete(objs):
            from django_async_backend.db.models.query import QuerySet

            qs = QuerySet(model=model, using=self.using)
            self.fast_deletes.append(qs.filter(pk__in=[obj.pk for obj in objs]))
            return

        new_objs = self.add(objs, source, nullable, reverse_dependency=reverse_dependency)
        if not new_objs:
            return

        # Collect parent models (multi-table inheritance)
        if not keep_parents:
            concrete_model = model._meta.concrete_model
            for ptr in concrete_model._meta.parents.values():
                if ptr:
                    parent_objs = [getattr(obj, ptr.name) for obj in new_objs]
                    await self.acollect(
                        parent_objs,
                        source=model,
                        source_attr=ptr.remote_field.related_name,
                        collect_related=False,
                        reverse_dependency=True,
                        fail_on_restricted=False,
                    )

        if not collect_related:
            return

        # Process related objects
        model_fast_deletes = defaultdict(list)
        protected_objects = defaultdict(list)

        for related in get_candidate_relations_to_delete(model._meta):
            if keep_parents and related.model._meta.concrete_model in model._meta.all_parents:
                continue

            field = related.field
            on_delete = field.remote_field.on_delete

            if on_delete == DO_NOTHING:
                continue

            related_model = related.related_model

            if self.can_fast_delete(related_model, from_field=field):
                model_fast_deletes[related_model].append(field)
                continue

            batches = self.get_del_batches(new_objs, [field])
            for batch in batches:
                sub_objs_list = await self.arelated_objects(related_model, [field], batch)
                if getattr(on_delete, "lazy_sub_objs", False) or sub_objs_list:
                    try:
                        # on_delete handlers (CASCADE, etc.) may call self.collect()
                        # which queues items in _pending_collects
                        on_delete(self, field, sub_objs_list, self.using)
                        # Drain pending collects from on_delete handlers
                        await self._drain_pending_collects()
                    except ProtectedError as error:
                        key = (
                            "'%s.%s'" % (field.model.__name__, field.name),
                            error.protected_objects,
                        )
                        protected_objects[key[0]] = list(key[1])

        if protected_objects:
            raise ProtectedError(
                "Cannot delete some instances of model %r because they are "
                "referenced through protected foreign keys: %s."
                % (
                    model.__name__,
                    ", ".join(protected_objects),
                ),
                set().union(*protected_objects.values()),
            )

        # Queue fast-deletable related objects as async querysets
        for related_model, related_fields in model_fast_deletes.items():
            from django_async_backend.db.models.query import QuerySet

            batches = self.get_del_batches(new_objs, related_fields)
            for batch in batches:
                predicate = query_utils.Q.create(
                    [(f"{rf.name}__in", batch) for rf in related_fields],
                    connector=query_utils.Q.OR,
                )
                qs = QuerySet(model=related_model, using=self.using).filter(predicate)
                self.fast_deletes.append(qs)

        # Drain any remaining pending collects from fast_delete queuing
        await self._drain_pending_collects()

        # Handle generic foreign keys
        for field in model._meta.private_fields:
            if hasattr(field, "bulk_related_objects"):
                sub_objs = field.bulk_related_objects(new_objs, self.using)
                await self.acollect(sub_objs, source=model, nullable=True, fail_on_restricted=False)

        # Validate RESTRICT constraints
        if fail_on_restricted:
            for key, restricted_objs_by_field in self.restricted_objects.items():
                for objs_set in restricted_objs_by_field.values():
                    if objs_set.difference(self.data.get(key, set())):
                        raise RestrictedError(
                            "Cannot delete some instances of model %r because "
                            "they are referenced through restricted foreign keys: %s."
                            % (
                                model.__name__,
                                ", ".join(str(f) for f in restricted_objs_by_field),
                            ),
                            set().union(*list(restricted_objs_by_field.values())),
                        )

    def sort(self):
        sorted_models: list[Any] = []
        concrete_models: set[Any] = set()
        models_list = list(self.data)
        while len(sorted_models) < len(models_list):
            found = False
            for model in models_list:
                if model in sorted_models:
                    continue
                dependencies = self.dependencies.get(model._meta.concrete_model)
                if not (dependencies and dependencies.difference(concrete_models)):
                    sorted_models.append(model)
                    concrete_models.add(model._meta.concrete_model)
                    found = True
            if not found:
                return
        self.data = defaultdict(set, {model: self.data[model] for model in sorted_models})

    def instances_with_model(self):
        for model, instances in self.data.items():
            for obj in instances:
                yield model, obj

    async def adelete(self):
        """
        Async version of Collector.delete(). Executes the deletion plan
        built by acollect(). Fires pre_delete/post_delete signals via asend().
        """
        # Sort instances by PK
        for model, instances in self.data.items():
            self.data[model] = sorted(instances, key=attrgetter("pk"))

        self.sort()
        deleted_counter: Counter[str] = Counter()

        # Single object fast-delete optimization
        if len(self.data) == 1:
            model = next(iter(self.data))
            instances = self.data[model]
            if len(instances) == 1:
                instance = instances[0]
                if self.can_fast_delete(instance):
                    from django_async_backend.db.models.query import QuerySet

                    async with async_mark_for_rollback_on_error(using=self.using):
                        qs = QuerySet(model=model, using=self.using)
                        count = await qs.filter(pk=instance.pk)._raw_delete(using=self.using)
                    setattr(instance, model._meta.pk.attname, None)
                    return count, {model._meta.label: count}

        async with async_atomic(using=self.using, savepoint=False):
            # Send pre_delete signals
            for model, obj in self.instances_with_model():
                if not model._meta.auto_created:
                    await signals.pre_delete.asend(
                        sender=model,
                        instance=obj,
                        using=self.using,
                        origin=self.origin,
                    )

            # Fast deletes (bulk)
            for qs in self.fast_deletes:
                count = await self._async_raw_delete(qs)
                if count:
                    deleted_counter[qs.model._meta.label] += count

            # Field updates (FK nullification)
            for (field, value), instances_list in self.field_updates.items():
                from django_async_backend.db.models.query import QuerySet

                updates = []
                objs = []
                for instances in instances_list:
                    if isinstance(instances, models.QuerySet) and instances._result_cache is None:
                        updates.append(instances)
                    else:
                        objs.extend(instances)
                if updates:
                    combined_updates = reduce(or_, updates)
                    # Use async update
                    async_qs = QuerySet(model=combined_updates.model, using=self.using)
                    await async_qs.filter(pk__in=[obj.pk for obj in combined_updates]).aupdate(**{field.name: value})
                if objs:
                    obj_model = objs[0].__class__
                    async_qs = QuerySet(model=obj_model, using=self.using)
                    await async_qs.filter(pk__in=[obj.pk for obj in objs]).aupdate(**{field.name: value})

            # Reverse instance lists
            for instances in self.data.values():
                instances.reverse()

            # Delete main objects
            for model, instances in self.data.items():
                from django_async_backend.db.models.query import QuerySet

                pk_list = [obj.pk for obj in instances]
                async_qs = QuerySet(model=model, using=self.using)
                count = await async_qs.filter(pk__in=pk_list)._raw_delete(using=self.using)
                if count:
                    deleted_counter[model._meta.label] += count

                if not model._meta.auto_created:
                    for obj in instances:
                        await signals.post_delete.asend(
                            sender=model,
                            instance=obj,
                            using=self.using,
                            origin=self.origin,
                        )

        # Nullify PKs
        for instances in self.data.values():
            for instance in instances:
                setattr(instance, instance._meta.pk.attname, None)

        return sum(deleted_counter.values()), dict(deleted_counter)

    async def _async_raw_delete(self, qs):
        return await qs._raw_delete(using=self.using)
