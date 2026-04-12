# Async Related Managers

Add `aadd`, `aremove`, `aclear`, `aset`, `acreate`, `aget_or_create`, `aupdate_or_create` to Django's reverse-FK and M2M managers for AsyncModel subclasses.

## Problem

Django's related managers (`author.article_set`, `article.tags`) are created dynamically by descriptors. They inherit from the model's `_default_manager.__class__` (sync `Manager`) and only have sync methods. Our `AsyncManager` is a separate manager — the related manager infrastructure doesn't know about it.

This means `author.article_set.add(article)` works (sync), but there's no `author.article_set.aadd(article)` (async). Tests that use related-manager ops are currently omitted from the port.

## Design

### New file: `django_async_backend/db/models/related_managers.py`

Contains:

1. **`AsyncReverseManyToOneMixin`** — async methods for reverse FK managers
2. **`AsyncManyToManyMixin`** — async methods for M2M managers
3. **`create_async_reverse_manager(superclass, rel)`** — calls Django's `create_reverse_many_to_one_manager`, returns a subclass with `AsyncReverseManyToOneMixin` mixed in
4. **`create_async_m2m_manager(superclass, rel, reverse)`** — calls Django's `create_forward_many_to_many_manager`, returns a subclass with `AsyncManyToManyMixin` mixed in

### Hook: `class_prepared` signal

Extend the existing `class_prepared` handler in `base.py`. For each `AsyncModel` subclass, iterate `sender._meta.get_fields()`:

- For `one_to_many` fields (reverse FK): get the descriptor from the model, replace its `related_manager_cls` cached property with our factory's output.
- For `many_to_many` fields: same treatment.

The descriptor's `related_manager_cls` is a `@cached_property`. We replace it by writing directly to the descriptor's `__dict__`, which is how cached_property works.

### Async queryset helper

Both mixins share a helper to build an async QuerySet with the correct relation filters:

```python
def _get_async_queryset(self):
    from django_async_backend.db.models.query import QuerySet
    db = self._db or router.db_for_read(self.model, instance=self.instance)
    qs = QuerySet(model=self.model, using=db)
    return self._apply_rel_filters(qs)
```

`_apply_rel_filters` calls `.filter()` which works on any queryset type.

For write operations, `_db` is resolved via `router.db_for_write` instead.

## Reverse FK methods

Mirror Django's `RelatedManager` (from `create_reverse_many_to_one_manager`).

### `aadd(*objs, bulk=True)`

```
for each obj:
    set obj.{field_name} = parent_instance
    set obj.{field_attname} = parent_pk
if bulk:
    await QuerySet.filter(pk__in=pks).aupdate({field_name: parent})
else:
    await obj.asave() for each obj
```

### `aremove(*objs, bulk=True)`

Only available if `field.null is True`. Raises `ValueError` otherwise.

```
for each obj:
    check obj is instance of self.model
if bulk:
    await self._get_async_queryset().filter(pk__in=pks).aupdate({field_name: None})
else:
    for each obj:
        set obj.{field_name} = None
        await obj.asave(update_fields=[field_attname])
```

### `aclear(bulk=True)`

Only available if `field.null is True`.

```
if bulk:
    await self._get_async_queryset().aupdate({field_name: None})
else:
    async for obj in self._get_async_queryset():
        obj.{field_name} = None
        await obj.asave(update_fields=[field_attname])
```

### `aset(objs, bulk=True, clear=False)`

```
objs = tuple(objs)
if clear:
    await self.aclear(bulk=bulk)
    await self.aadd(*objs, bulk=bulk)
else:
    old_ids = {obj.pk async for obj in self._get_async_queryset()}
    new_objs = []
    for obj in objs:
        if (obj.pk if hasattr(obj, 'pk') else obj) in old_ids:
            old_ids.discard(...)
        else:
            new_objs.append(obj)
    await self.aremove(*[obj for obj in old_objs if obj.pk in old_ids], bulk=bulk)
    await self.aadd(*new_objs, bulk=bulk)
```

### `acreate(**kwargs)`, `aget_or_create(**kwargs)`, `aupdate_or_create(**kwargs)`

```
kwargs[field_name] = parent_instance
return await self._get_async_queryset().acreate(**kwargs)
# (or aget_or_create / aupdate_or_create)
```

### Signals

None fired by reverse FK operations (same as Django's sync path — only `post_save` fires per object in non-bulk mode, which will work once async signals are implemented).

## M2M methods

Mirror Django's `ManyRelatedManager` (from `create_forward_many_to_many_manager`).

The through-table QuerySet is constructed as:

```python
def _get_through_queryset(self):
    from django_async_backend.db.models.query import QuerySet
    db = self._db or router.db_for_write(self.through, instance=self.instance)
    return QuerySet(model=self.through, using=db)
```

### `aadd(*objs, through_defaults=None)`

Calls `_aadd_items(source_field_name, target_field_name, *objs, through_defaults=through_defaults)`.

```
resolve obj IDs (pk for model instances, raw values otherwise)
filter existing: await through_qs.filter(source=self_pk, target__in=ids).avalues_list(target_field, flat=True)
new_ids = requested_ids - existing_ids

signal: m2m_changed.send(action="pre_add", pk_set=new_ids, ...)
if new_ids still non-empty after signal:
    build through instances with through_defaults
    await ThroughQuerySet.abulk_create([...], ignore_conflicts=True)
signal: m2m_changed.send(action="post_add", pk_set=new_ids, ...)
```

For symmetrical M2M: repeat for reverse direction.

### `aremove(*objs)`

Not available if through model has custom fields beyond the two FKs.

```
resolve obj IDs
signal: m2m_changed.send(action="pre_remove", pk_set=ids, ...)
await through_qs.filter(source=self_pk, target__in=ids).adelete()
signal: m2m_changed.send(action="post_remove", pk_set=ids, ...)
```

For symmetrical: repeat.

### `aclear()`

Not available if through model has custom fields beyond the two FKs.

```
signal: m2m_changed.send(action="pre_clear", pk_set=None, ...)
await through_qs.filter(source=self_pk).adelete()
signal: m2m_changed.send(action="post_clear", pk_set=None, ...)
```

For symmetrical: repeat.

### `aset(objs, clear=False, through_defaults=None)`

```
objs = tuple(objs)
if clear:
    await self.aclear()
    await self.aadd(*objs, through_defaults=through_defaults)
else:
    old_ids = set(await through_qs.filter(source=self_pk).avalues_list(target_field, flat=True))
    new_ids = resolve_ids(objs)
    to_remove = old_ids - new_ids
    to_add = new_ids - old_ids
    if to_remove:
        await self.aremove(*to_remove)
    if to_add:
        await self.aadd(*[obj for obj in objs if get_id(obj) in to_add], through_defaults=through_defaults)
```

### `acreate(through_defaults=None, **kwargs)`, `aget_or_create(...)`, `aupdate_or_create(...)`

```
obj = await self._get_async_queryset().acreate(**kwargs)
await self.aadd(obj, through_defaults=through_defaults)
return obj
```

For `aget_or_create`: only `aadd` if created. For `aupdate_or_create`: always `aadd` (through defaults may need updating).

### Signals

`m2m_changed` fired synchronously via `signal.send()` (Django's signal dispatch is sync — this is a known Django-core limitation noted in README). Signal kwargs match Django's: `sender=through_model, instance=self.instance, action=..., reverse=..., model=target_model, pk_set=..., using=db`.

## Testing

### New test file: `tests/db/models/query/test_related_managers.py`

Reverse FK tests:
- `aadd` (bulk and non-bulk)
- `aremove` (bulk and non-bulk, verify ValueError on non-null FK)
- `aclear` (bulk and non-bulk)
- `aset` (with and without clear=True)
- `acreate`, `aget_or_create`, `aupdate_or_create`

M2M tests:
- `aadd` (single, multiple, idempotent re-add)
- `aremove` (single, multiple)
- `aclear`
- `aset` (with and without clear=True)
- `acreate`, `aget_or_create`, `aupdate_or_create`
- `aadd` with `through_defaults`
- `m2m_changed` signal fires correctly (pre/post for add/remove/clear)

Edge cases:
- Custom through model
- Self-referential M2M (symmetrical)
- Empty operations (aadd with no args, aset with empty list)

### Port omitted tests

Re-enable the related-manager tests previously omitted from test_lookups.py and test_expressions.py where feasible.

## Models needed

The existing test models may need M2M fields. Check `tests/lookup/models.py` for `Tag.articles` (M2M) and `Article.author` (FK). Add new models if needed for through-model testing.
