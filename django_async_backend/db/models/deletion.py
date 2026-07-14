# This file was generated automatically. Do not modify it manually. (based on django 6.0)
from collections import (
    Counter,
    defaultdict,
)
from functools import (
    partial,
    reduce,
)
from inspect import iscoroutinefunction
from itertools import chain
from operator import (
    attrgetter,
    or_,
)

from django.db.models import (
    query_utils,
    signals,
)
from django.db.models.deletion import (
    CASCADE,
    DO_NOTHING,
    PROTECT,
    RESTRICT,
    SET,
    SET_DEFAULT,
    SET_NULL,
    ProtectedError,
    RestrictedError,
    get_candidate_relations_to_delete,
)

from django_async_backend.db import async_connections as connections
from django_async_backend.db.models import sql
from django_async_backend.db.transaction import (
    async_atomic,
    async_mark_for_rollback_on_error,
)


async def ASYNC_CASCADE(collector, field, sub_objs, using):
    await collector.collect(
        sub_objs,
        source=field.remote_field.model,
        source_attr=field.name,
        nullable=field.null,
        fail_on_restricted=False,
    )
    if (
        field.null
        and not connections[using].features.can_defer_constraint_checks
    ):
        collector.add_field_update(field, None, sub_objs)


class Collector:
    async def _async_on_delete(self, on_delete, field, sub_objs):
        # Dispatch an on_delete handler. The sync collector passes a
        # lazy queryset that handlers evaluate implicitly; fetch it up
        # front instead, and route CASCADE to its async twin.
        if getattr(on_delete, "lazy_sub_objs", False):
            # SET_NULL / SET(value): stays lazy for one bulk UPDATE.
            on_delete(self, field, sub_objs, self.using)
            return
        sub_objs = [obj async for obj in sub_objs]
        if not sub_objs:
            return
        if on_delete is CASCADE:
            await ASYNC_CASCADE(self, field, sub_objs, self.using)
        elif iscoroutinefunction(on_delete):
            await on_delete(self, field, sub_objs, self.using)
        else:
            on_delete(self, field, sub_objs, self.using)

    def _gfk_not_supported(self):
        raise NotImplementedError(
            "Async delete does not yet support cascading through "
            "GenericRelation."
        )

    def __init__(self, using, origin=None):
        self.using = using
        # A Model or QuerySet object.
        self.origin = origin
        # Initially, {model: {instances}}, later values become lists.
        self.data = defaultdict(set)
        # {(field, value): [instances, …]}
        self.field_updates = defaultdict(list)
        # {model: {field: {instances}}}
        self.restricted_objects = defaultdict(partial(defaultdict, set))
        # fast_deletes is a list of queryset-likes that can be deleted without
        # fetching the objects into memory.
        self.fast_deletes = []

        # Tracks deletion-order dependency for databases without transactions
        # or ability to defer constraint checks. Only concrete model classes
        # should be included, as the dependencies exist only between actual
        # database tables; proxy models are represented here by their concrete
        # parent.
        self.dependencies = defaultdict(set)  # {model: {models}}

    def add(self, objs, source=None, nullable=False, reverse_dependency=False):
        """
        Add 'objs' to the collection of objects to be deleted. If the call is
        the result of a cascade, 'source' should be the model that caused it,
        and 'nullable' should be set to True if the relation can be null.

        Return a list of all objects that were not already collected.
        """
        if not objs:
            return []
        new_objs = []
        model = objs[0].__class__
        instances = self.data[model]
        for obj in objs:
            if obj not in instances:
                new_objs.append(obj)
        instances.update(new_objs)
        # Nullable relationships can be ignored -- they are nulled out before
        # deleting, and therefore do not affect the order in which objects have
        # to be deleted.
        if source is not None and not nullable:
            self.add_dependency(
                source, model, reverse_dependency=reverse_dependency
            )
        return new_objs

    def add_dependency(self, model, dependency, reverse_dependency=False):
        if reverse_dependency:
            model, dependency = dependency, model
        self.dependencies[model._meta.concrete_model].add(
            dependency._meta.concrete_model
        )
        self.data.setdefault(dependency, self.data.default_factory())

    def add_field_update(self, field, value, objs):
        """
        Schedule a field update. 'objs' must be a homogeneous iterable
        collection of model instances (e.g. a QuerySet).
        """
        self.field_updates[field, value].append(objs)

    def add_restricted_objects(self, field, objs):
        if objs:
            model = objs[0].__class__
            self.restricted_objects[model][field].update(objs)

    def clear_restricted_objects_from_set(self, model, objs):
        if model in self.restricted_objects:
            self.restricted_objects[model] = {
                field: items - objs
                for field, items in self.restricted_objects[model].items()
            }

    async def clear_restricted_objects_from_queryset(self, model, qs):
        if model in self.restricted_objects:
            objs = {
                obj
                async for obj in qs.filter(
                    pk__in=[
                        obj.pk
                        for objs in self.restricted_objects[model].values()
                        for obj in objs
                    ]
                )
            }
            self.clear_restricted_objects_from_set(model, objs)

    def _has_signal_listeners(self, model):
        return signals.pre_delete.has_listeners(
            model
        ) or signals.post_delete.has_listeners(model)

    def can_fast_delete(self, objs, from_field=None):
        """
        Determine if the objects in the given queryset-like or single object
        can be fast-deleted. This can be done if there are no cascades, no
        parents and no signal listeners for the object class.

        The 'from_field' tells where we are coming from - we need this to
        determine if the objects are in fact to be deleted. Allow also
        skipping parent -> child -> parent chain preventing fast delete of
        the child.
        """
        if from_field and from_field.remote_field.on_delete is not CASCADE:
            return False
        if hasattr(objs, "_meta"):
            model = objs._meta.model
        elif hasattr(objs, "model") and hasattr(objs, "_raw_delete"):
            model = objs.model
        else:
            return False
        if self._has_signal_listeners(model):
            return False
        # The use of from_field comes from the need to avoid cascade back to
        # parent when parent delete is cascading to child.
        opts = model._meta
        return (
            all(
                link == from_field
                for link in opts.concrete_model._meta.parents.values()
            )
            and
            # Foreign keys pointing to this model.
            all(
                related.field.remote_field.on_delete is DO_NOTHING
                for related in get_candidate_relations_to_delete(opts)
            )
            and (
                # Something like generic foreign key.
                not any(
                    hasattr(field, "bulk_related_objects")
                    for field in opts.private_fields
                )
            )
        )

    def get_del_batches(self, objs, fields):
        """
        Return the objs in suitably sized batches for the used connection.
        """
        conn_batch_size = max(
            connections[self.using].ops.bulk_batch_size(fields, objs), 1
        )
        if len(objs) > conn_batch_size:
            return [
                objs[i : i + conn_batch_size]
                for i in range(0, len(objs), conn_batch_size)
            ]
        else:
            return [objs]

    async def collect(
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
        Add 'objs' to the collection of objects to be deleted as well as all
        parent instances. 'objs' must be a homogeneous iterable collection of
        model instances (e.g. a QuerySet). If 'collect_related' is True,
        related objects will be handled by their respective on_delete handler.

        If the call is the result of a cascade, 'source' should be the model
        that caused it and 'nullable' should be set to True, if the relation
        can be null.

        If 'reverse_dependency' is True, 'source' will be deleted before the
        current model, rather than after. (Needed for cascading to parent
        models, the one case in which the cascade follows the forwards
        direction of an FK rather than the reverse direction.)

        If 'keep_parents' is True, data of parent model's will be not deleted.

        If 'fail_on_restricted' is False, error won't be raised even if it's
        prohibited to delete such objects due to RESTRICT, that defers
        restricted object checking in recursive calls where the top-level call
        may need to collect more objects to determine whether restricted ones
        can be deleted.
        """

        if hasattr(objs, "__aiter__") and not self.can_fast_delete(objs):
            # The sync collector evaluates querysets lazily via
            # __bool__/__iter__; fetch up front instead.
            objs = [obj async for obj in objs]

        if (
            not keep_parents
            and not hasattr(objs, "__aiter__")
            and objs
            and objs[0].__class__._meta.concrete_model._meta.parents
        ):
            # Collecting parents runs one sync descriptor fetch per
            # instance; reject loudly instead of silently using the
            # sync connection.
            raise NotImplementedError(
                "Async delete does not yet support multi-table inheritance."
            )

        if self.can_fast_delete(objs):
            self.fast_deletes.append(objs)
            return
        new_objs = self.add(
            objs, source, nullable, reverse_dependency=reverse_dependency
        )
        if not new_objs:
            return

        model = new_objs[0].__class__

        if not keep_parents:
            # Recursively collect concrete model's parent models, but not their
            # related objects. These will be found by meta.get_fields()
            concrete_model = model._meta.concrete_model
            for ptr in concrete_model._meta.parents.values():
                if ptr:
                    parent_objs = [getattr(obj, ptr.name) for obj in new_objs]
                    await self.collect(
                        parent_objs,
                        source=model,
                        source_attr=ptr.remote_field.related_name,
                        collect_related=False,
                        reverse_dependency=True,
                        fail_on_restricted=False,
                    )
        if not collect_related:
            return

        model_fast_deletes = defaultdict(list)
        protected_objects = defaultdict(list)
        for related in get_candidate_relations_to_delete(model._meta):
            # Preserve parent reverse relationships if keep_parents=True.
            if (
                keep_parents
                and related.model._meta.concrete_model
                in model._meta.all_parents
            ):
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
                sub_objs = self.related_objects(related_model, [field], batch)
                # Non-referenced fields can be deferred if no signal receivers
                # are connected for the related model as they'll never be
                # exposed to the user. Skip field deferring when some
                # relationships are select_related as interactions between both
                # features are hard to get right. This should only happen in
                # the rare cases where .related_objects is overridden anyway.
                if not (
                    sub_objs.query.select_related
                    or self._has_signal_listeners(related_model)
                ):
                    referenced_fields = set(
                        chain.from_iterable(
                            (
                                rf.attname
                                for rf in rel.field.foreign_related_fields
                            )
                            for rel in get_candidate_relations_to_delete(
                                related_model._meta
                            )
                        )
                    )
                    sub_objs = sub_objs.only(*tuple(referenced_fields))
                if getattr(on_delete, "lazy_sub_objs", False) or sub_objs:
                    try:
                        await self._async_on_delete(on_delete, field, sub_objs)
                    except ProtectedError as error:
                        key = "'%s.%s'" % (field.model.__name__, field.name)
                        protected_objects[key] += error.protected_objects
        if protected_objects:
            raise ProtectedError(
                "Cannot delete some instances of model %r because they are "
                "referenced through protected foreign keys: %s."
                % (
                    model.__name__,
                    ", ".join(protected_objects),
                ),
                set(chain.from_iterable(protected_objects.values())),
            )
        for related_model, related_fields in model_fast_deletes.items():
            batches = self.get_del_batches(new_objs, related_fields)
            for batch in batches:
                sub_objs = self.related_objects(
                    related_model, related_fields, batch
                )
                self.fast_deletes.append(sub_objs)
        for field in model._meta.private_fields:
            if hasattr(field, "bulk_related_objects"):
                # It's something like generic foreign key.
                sub_objs = self._gfk_not_supported()
                await self.collect(
                    sub_objs,
                    source=model,
                    nullable=True,
                    fail_on_restricted=False,
                )

        if fail_on_restricted:
            # Raise an error if collected restricted objects (RESTRICT) aren't
            # candidates for deletion also collected via CASCADE.
            for related_model, instances in self.data.items():
                self.clear_restricted_objects_from_set(
                    related_model, instances
                )
            for qs in self.fast_deletes:
                await self.clear_restricted_objects_from_queryset(qs.model, qs)
            if self.restricted_objects.values():
                restricted_objects = defaultdict(list)
                for related_model, fields in self.restricted_objects.items():
                    for field, objs in fields.items():
                        if objs:
                            key = "'%s.%s'" % (
                                related_model.__name__,
                                field.name,
                            )
                            restricted_objects[key] += objs
                if restricted_objects:
                    raise RestrictedError(
                        "Cannot delete some instances of model %r because "
                        "they are referenced through restricted foreign keys: "
                        "%s."
                        % (
                            model.__name__,
                            ", ".join(restricted_objects),
                        ),
                        set(chain.from_iterable(restricted_objects.values())),
                    )

    def related_objects(self, related_model, related_fields, objs):
        """
        Get a QuerySet of the related model to objs via related fields.
        """
        predicate = query_utils.Q.create(
            [
                (f"{related_field.name}__in", objs)
                for related_field in related_fields
            ],
            connector=query_utils.Q.OR,
        )
        return _async_queryset_for(related_model, self.using).filter(predicate)

    def instances_with_model(self):
        for model, instances in self.data.items():
            for obj in instances:
                yield model, obj

    def sort(self):
        sorted_models = []
        concrete_models = set()
        models = list(self.data)
        while len(sorted_models) < len(models):
            found = False
            for model in models:
                if model in sorted_models:
                    continue
                dependencies = self.dependencies.get(
                    model._meta.concrete_model
                )
                if not (
                    dependencies and dependencies.difference(concrete_models)
                ):
                    sorted_models.append(model)
                    concrete_models.add(model._meta.concrete_model)
                    found = True
            if not found:
                return
        self.data = {model: self.data[model] for model in sorted_models}

    async def delete(self):
        # sort instance collections
        for model, instances in self.data.items():
            self.data[model] = sorted(instances, key=attrgetter("pk"))

        # if possible, bring the models in an order suitable for databases that
        # don't support transactions or cannot defer constraint checks until
        # the end of a transaction.
        self.sort()
        # number of objects deleted for each model label
        deleted_counter = Counter()

        # Optimize for the case with a single obj and no dependencies
        if len(self.data) == 1 and len(instances) == 1:
            instance = list(instances)[0]
            if self.can_fast_delete(instance):
                async with async_mark_for_rollback_on_error(using=self.using):
                    count = await sql.DeleteQuery(model).delete_batch(
                        [instance.pk], self.using
                    )
                setattr(instance, model._meta.pk.attname, None)
                return count, {model._meta.label: count}

        async with async_atomic(using=self.using, savepoint=False):
            # send pre_delete signals
            for model, obj in self.instances_with_model():
                if not model._meta.auto_created:
                    await signals.pre_delete.asend(
                        sender=model,
                        instance=obj,
                        using=self.using,
                        origin=self.origin,
                    )

            # fast deletes
            for qs in self.fast_deletes:
                count = await qs._raw_delete(using=self.using)
                if count:
                    deleted_counter[qs.model._meta.label] += count

            # update fields
            for (field, value), instances_list in self.field_updates.items():
                updates = []
                objs = []
                for instances in instances_list:
                    if (
                        hasattr(instances, "_result_cache")
                        and instances._result_cache is None
                    ):
                        updates.append(instances)
                    else:
                        objs.extend(instances)
                if updates:
                    combined_updates = reduce(or_, updates)
                    await combined_updates.aupdate(**{field.name: value})
                if objs:
                    model = objs[0].__class__
                    query = sql.UpdateQuery(model)
                    await query.update_batch(
                        list({obj.pk for obj in objs}),
                        {field.name: value},
                        self.using,
                    )

            # reverse instance collections
            for instances in self.data.values():
                instances.reverse()

            # delete instances
            for model, instances in self.data.items():
                query = sql.DeleteQuery(model)
                pk_list = [obj.pk for obj in instances]
                count = await query.delete_batch(pk_list, self.using)
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

        for model, instances in self.data.items():
            for instance in instances:
                setattr(instance, model._meta.pk.attname, None)
        return sum(deleted_counter.values()), dict(deleted_counter)


def _async_queryset_for(model, using):
    # Cascades reach models that don't opt into AsyncModelMixin
    # (e.g. auto-created M2M through models); build a queryset
    # directly. Imported lazily to avoid a circular import.
    from django_async_backend.db.models.query import QuerySet

    return QuerySet(model=model, using=using)
