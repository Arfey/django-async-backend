"""Async counterpart of ``django.db.models.query.prefetch_one_level()``.

Django resolves a prefetch by asking the descriptor (or the related manager)
for ``get_prefetch_querysets()``. Every implementation of that hook builds a
*sync* queryset and, for single-valued relations, iterates it right away to
populate the reverse relation cache. Both halves are unusable here: the
queryset has to be an async one, and it can only be consumed with ``async
for``.

So the descriptor code is mirrored below instead of being called. Each branch
returns the same six-tuple Django's hook returns -- unevaluated -- plus a
callback that performs the cache bookkeeping the sync code did while
iterating. ``prefetch_one_level()`` then evaluates the queryset with ``async
for`` and runs that callback.

Relations whose prefetch cannot be expressed this way (generic foreign keys,
generic relations, custom descriptors) raise ``NotSupportedError``.
"""

import copy

from django.core import exceptions
from django.db import NotSupportedError
from django.db.models.expressions import ColPairs
from django.db.models.fields.related_descriptors import (
    ForwardManyToOneDescriptor,
    ReverseOneToOneDescriptor,
    _filter_prefetch_queryset,
)
from django.db.models.fields.tuple_lookups import TupleIn

RELATED_DESCRIPTORS_MODULE = "django.db.models.fields.related_descriptors"


def _async_queryset(model, using=None, hints=None):
    """Async equivalent of ``Manager.get_queryset()`` on a plain manager."""
    # Imported lazily: django_async_backend.db.models.query imports this
    # module at import time.
    from django_async_backend.db.models.query import QuerySet

    return QuerySet(model=model, using=using, hints=hints)


def _descriptor_queryset(model, instance):
    """Async equivalent of the descriptors' ``get_queryset(instance=...)``:
    ``model._base_manager.db_manager(hints={"instance": instance}).fetch_mode(
    instance._state.fetch_mode)``.
    """
    queryset = _async_queryset(model, hints={"instance": instance})
    return queryset.fetch_mode(instance._state.fetch_mode)


def _unsupported(prefetcher):
    name = getattr(prefetcher, "name", None)
    if name is None:
        # Descriptors (e.g. GenericForeignKeyDescriptor) wrap the field.
        name = getattr(getattr(prefetcher, "field", None), "name", None)
    if name is None:
        name = type(prefetcher).__name__
    raise NotSupportedError(
        "prefetch_related() for %s is not supported by the async backend."
        % name
    )


def _single_queryset(querysets):
    if querysets and len(querysets) != 1:
        raise ValueError(
            "querysets argument of get_prefetch_querysets() should have a "
            "length of 1."
        )
    return querysets[0] if querysets else None


def _noop(related_objects):
    pass


def _forward_many_to_one(descriptor, instances, querysets):
    """Mirror of ``ForwardManyToOneDescriptor.get_prefetch_querysets()``."""
    field = descriptor.field
    queryset = _single_queryset(querysets)
    cloning_disabled = queryset is None
    if cloning_disabled:
        queryset = _descriptor_queryset(
            field.remote_field.model, instances[0]
        )._disable_cloning()

    rel_obj_attr = field.get_foreign_related_value
    instance_attr = field.get_local_related_value
    instances_dict = {instance_attr(inst): inst for inst in instances}
    remote_field = field.remote_field
    related_fields = [
        queryset.query.resolve_ref(related_field.name).target
        for related_field in field.foreign_related_fields
    ]
    queryset = queryset.filter(
        TupleIn(
            ColPairs(
                queryset.model._meta.db_table,
                related_fields,
                related_fields,
                field,
            ),
            list(instances_dict),
        )
    )
    # There can be only one object prefetched for each instance so clear
    # ordering if the query allows it without side effects.
    queryset.query.clear_ordering()
    # Restore subsequent cloning operations.
    if cloning_disabled:
        queryset._enable_cloning()

    def post_evaluate(related_objects):
        # Since we're going to assign directly in the cache,
        # we must manage the reverse relation cache manually.
        if not remote_field.multiple:
            for rel_obj in related_objects:
                instance = instances_dict[rel_obj_attr(rel_obj)]
                remote_field.set_cached_value(rel_obj, instance)

    return (
        queryset,
        rel_obj_attr,
        instance_attr,
        True,
        field.cache_name,
        False,
        post_evaluate,
    )


def _reverse_one_to_one(descriptor, instances, querysets):
    """Mirror of ``ReverseOneToOneDescriptor.get_prefetch_querysets()``."""
    related = descriptor.related
    queryset = _single_queryset(querysets)
    cloning_disabled = queryset is None
    if cloning_disabled:
        queryset = _descriptor_queryset(
            related.related_model, instances[0]
        )._disable_cloning()

    rel_obj_attr = related.field.get_local_related_value
    instance_attr = related.field.get_foreign_related_value
    instances_dict = {instance_attr(inst): inst for inst in instances}
    query = {"%s__in" % related.field.name: instances}
    queryset = queryset.filter(**query)
    queryset.query.clear_ordering()
    # Restore subsequent cloning operations.
    if cloning_disabled:
        queryset._enable_cloning()

    def post_evaluate(related_objects):
        for rel_obj in related_objects:
            instance = instances_dict[rel_obj_attr(rel_obj)]
            related.field.set_cached_value(rel_obj, instance)

    return (
        queryset,
        rel_obj_attr,
        instance_attr,
        True,
        related.cache_name,
        False,
        post_evaluate,
    )


def _reverse_many_to_one(manager, instances, querysets):
    """Mirror of the reverse FK ``RelatedManager.get_prefetch_querysets()``."""
    field = manager.field
    queryset = _single_queryset(querysets)
    cloning_disabled = queryset is None
    if cloning_disabled:
        queryset = _async_queryset(
            manager.model, using=manager._db, hints=manager._hints
        )._disable_cloning()

    queryset._add_hints(instance=instances[0])
    queryset = queryset.using(queryset._db or manager._db)

    rel_obj_attr = field.get_local_related_value
    instance_attr = field.get_foreign_related_value
    instances_dict = {instance_attr(inst): inst for inst in instances}
    queryset = _filter_prefetch_queryset(queryset, field.name, instances)
    # Restore subsequent cloning operations.
    if cloning_disabled:
        queryset._enable_cloning()

    def post_evaluate(related_objects):
        # Since we just bypassed the manager's get_queryset(), we must manage
        # the reverse relation manually.
        for rel_obj in related_objects:
            if not field.is_cached(rel_obj):
                instance = instances_dict[rel_obj_attr(rel_obj)]
                field.set_cached_value(rel_obj, instance)

    return (
        queryset,
        rel_obj_attr,
        instance_attr,
        False,
        field.remote_field.cache_name,
        False,
        post_evaluate,
    )


def _many_to_many(manager, instances, querysets):
    """Delegate to ``ManyRelatedManager.get_prefetch_querysets()``.

    That implementation only annotates the queryset with the join table
    columns; it never evaluates it. Handing it an async queryset is therefore
    enough to make it async.
    """
    queryset = _single_queryset(querysets)
    if queryset is None:
        queryset = _async_queryset(
            manager.model, using=manager._db, hints=manager._hints
        )

    return (*manager.get_prefetch_querysets(instances, [queryset]), _noop)


def _get_prefetch_querysets(prefetcher, instances, querysets):
    """Build the related queryset for ``prefetcher`` without evaluating it.

    Return Django's ``get_prefetch_querysets()`` six-tuple plus a
    ``post_evaluate(related_objects)`` callback.
    """
    if isinstance(prefetcher, ForwardManyToOneDescriptor):
        # Also covers ForwardOneToOneDescriptor.
        return _forward_many_to_one(prefetcher, instances, querysets)

    if isinstance(prefetcher, ReverseOneToOneDescriptor):
        return _reverse_one_to_one(prefetcher, instances, querysets)

    # Related managers are built by factories in related_descriptors, so the
    # generated classes are the only reliable marker. Generic relation
    # managers come from django.contrib.contenttypes.fields instead, and are
    # rejected below together with GenericForeignKey.
    if type(prefetcher).__module__ == RELATED_DESCRIPTORS_MODULE:
        if hasattr(prefetcher, "through"):
            return _many_to_many(prefetcher, instances, querysets)
        return _reverse_many_to_one(prefetcher, instances, querysets)

    return _unsupported(prefetcher)


async def prefetch_one_level(instances, prefetcher, lookup, level):
    """
    Helper function for prefetch_related_objects().

    Run prefetches on all instances using the prefetcher object,
    assigning results to relevant caches in instance.

    Return the prefetched objects along with any additional prefetches that
    must be done due to prefetch_related lookups found from default managers.
    """
    (
        rel_qs,
        rel_obj_attr,
        instance_attr,
        single,
        cache_name,
        is_descriptor,
        post_evaluate,
    ) = _get_prefetch_querysets(
        prefetcher, instances, lookup.get_current_querysets(level)
    )
    # We have to handle the possibility that the QuerySet we just got back
    # contains some prefetch_related lookups. We don't want to trigger the
    # prefetch_related functionality by evaluating the query. Rather, we need
    # to merge in the prefetch_related lookups.
    # Copy the lookups in case it is a Prefetch object which could be reused
    # later (happens in nested prefetch_related).
    additional_lookups = [
        copy.copy(additional_lookup)
        for additional_lookup in getattr(
            rel_qs, "_prefetch_related_lookups", ()
        )
    ]
    if additional_lookups:
        # Don't need to clone because the manager should have given us a fresh
        # instance, so we access an internal instead of using public interface
        # for performance reasons.
        rel_qs._prefetch_related_lookups = ()

    all_related_objects = [rel_obj async for rel_obj in rel_qs]
    # The sync descriptors fill the reverse relation caches while iterating;
    # do it now that the queryset has been consumed.
    post_evaluate(all_related_objects)

    rel_obj_cache = {}
    for rel_obj in all_related_objects:
        rel_attr_val = rel_obj_attr(rel_obj)
        rel_obj_cache.setdefault(rel_attr_val, []).append(rel_obj)

    to_attr, as_attr = lookup.get_current_to_attr(level)
    # Make sure `to_attr` does not conflict with a field.
    if as_attr and instances:
        # We assume that objects retrieved are homogeneous (which is the
        # premise of prefetch_related), so what applies to first object applies
        # to all.
        model = instances[0].__class__
        try:
            model._meta.get_field(to_attr)
        except exceptions.FieldDoesNotExist:
            pass
        else:
            raise ValueError(
                "to_attr=%s conflicts with a field on the %s model."
                % (to_attr, model.__name__)
            )

    for obj in instances:
        instance_attr_val = instance_attr(obj)
        vals = rel_obj_cache.get(instance_attr_val, [])

        if single:
            val = vals[0] if vals else None
            if as_attr:
                # A to_attr has been given for the prefetch.
                setattr(obj, to_attr, val)
            elif is_descriptor:
                # cache_name points to a field name in obj.
                # This field is a descriptor for a related object.
                setattr(obj, cache_name, val)
            else:
                # No to_attr has been given for this prefetch operation and the
                # cache_name does not point to a descriptor. Store the value of
                # the field in the object's field cache.
                obj._state.fields_cache[cache_name] = val
        else:
            if as_attr:
                setattr(obj, to_attr, vals)
            else:
                manager = getattr(obj, to_attr)
                # Related managers expose Django's synchronous queryset API.
                # Keep the cache shape consistent even when an async custom
                # queryset supplied the prefetched values.
                qs = manager.get_queryset()
                qs._result_cache = vals
                # We don't want the individual qs doing prefetch_related now,
                # since we have merged this into the current work.
                qs._prefetch_done = True
                obj._prefetched_objects_cache[cache_name] = qs
    return all_related_objects, additional_lookups
