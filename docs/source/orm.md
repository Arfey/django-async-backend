# ORM

## AsyncModelMixin

The recommended way to add async ORM support to a model is to inherit from
`AsyncModelMixin`. The mixin gives every model two things without any extra
boilerplate:

- an `async_objects` manager (an `AsyncManager`), so you don't have to declare
  one by hand;
- `async_save()`, `async_delete()` and `async_refresh_from_db()` methods for
  saving, deleting and reloading instances asynchronously.

```python
from django.db import models, DEFAULT_DB_ALIAS
from django_async_backend.db import async_connections
from django_async_backend.db.models.base import AsyncModelMixin


class Book(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=100)


async def main():
    # create / save an instance
    book = Book(name="Django")
    await book.async_save()

    # update via update_fields
    book.name = "Django Async"
    await book.async_save(update_fields=["name"])

    # query through the async_objects manager
    async for i in Book.async_objects.all():
        print(i.id, i.name)

    # delete a single instance, or a whole queryset
    await book.async_delete()
    await Book.async_objects.filter(name="Django Async").adelete()

    await async_connections[DEFAULT_DB_ALIAS].close()
```

:::{admonition} Why `async_save()` and not `asave()`?
:class: important

Django already defines `asave()` and `adelete()` on every model, and they are
not truly async: each is a `sync_to_async(self.save)` wrapper that runs the
blocking query on a threadpool.

Using distinct names keeps both available on the same model. That preserves
**backward compatibility** — existing code calling `asave()` keeps its current
behavior instead of silently changing underneath it — and it **guarantees the
async path is genuinely async**: when you call `async_save()`, you know the
query runs on the asyncio connection, with no threadpool and no hidden sync
connection.
:::

### `async_save()`

Accepts the same keyword arguments as Django's `save()` (`force_insert`,
`force_update`, `using`, `update_fields`) and honors model `Meta` options such
as `select_on_save` and `order_with_respect_to`, as well as multi-table
inheritance.

### `async_delete()`

Accepts the same keyword arguments as Django's `delete()` (`using`,
`keep_parents`), returns the `(count, {label: count})` pair, and cascades
through related objects, sending `pre_delete` / `post_delete` along the way.

`on_delete` handlers are resolved to async equivalents, so the standard
`CASCADE`, `PROTECT`, `RESTRICT`, `SET_NULL`, `SET_DEFAULT`, `SET(...)` and
`DO_NOTHING` all work. A custom **synchronous** `on_delete` callable is
rejected with a `TypeError`, because it would run a blocking query.

### `async_refresh_from_db()`

Reloads the instance's field values from the database. Accepts the same
keyword arguments as Django's `refresh_from_db()` (`using`, `fields`,
`from_queryset`), drops cached related objects and prefetched results, and
leaves fields the reloaded row did not select untouched.

```python
book = await Book.async_objects.aget(name="Django")
await Book.async_objects.filter(pk=book.pk).aupdate(name="Django Async")

await book.async_refresh_from_db()
assert book.name == "Django Async"

# reload a subset
await book.async_refresh_from_db(fields=["name"])
```

`from_queryset` must be an async queryset or an `AsyncManager` — passing
`Model.objects` raises `TypeError`, because a sync queryset would read through
Django's connection and a different transaction.

```{note}
Unlike Django's `refresh_from_db()`, this is **not** what deferred field
access falls back to. Reading a field left out of a deferred load still goes
through Django's synchronous `refresh_from_db()` and raises
`SynchronousOnlyOperation`, because attribute access cannot be awaited. Reload
explicitly instead.
```

```{warning}
Refresh instances that were loaded asynchronously. An instance fetched with
the sync ORM (`Model.objects.get()`) and then reloaded with
`async_refresh_from_db()` is read on the **async** connection — a different
transaction from the one it came from. Nothing detects this, because
`_state.db` records the alias, not which connection registry served it. Inside
a sync `transaction.atomic()` block it will not see that transaction's
uncommitted rows.

Django's own `arefresh_from_db()` stays on the sync connection, so it remains
the right call for an instance that lives in the sync world.
```

## Managers

If you prefer, you can attach an `AsyncManager` to a model explicitly instead
of using `AsyncModelMixin`:

```python
from django.db import models, DEFAULT_DB_ALIAS
from django_async_backend.db import async_connections
from django_async_backend.db.models.manager import AsyncManager


class Book(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)

    async_objects = AsyncManager()


async def main():
    async for i in Book.async_objects.all():
        print(i.id)

    await async_connections[DEFAULT_DB_ALIAS].close()
```

## Content types

`django.contrib.contenttypes` is synchronous, and it gets reached implicitly:
assigning a `GenericForeignKey` calls `ContentType.objects.get_for_model()`
behind the scenes, which raises `SynchronousOnlyOperation` in an async context.

That lookup is cached, so it only touches the database when the cache is cold.
`aget_for_model()` is the async equivalent — warm the cache with it first and
the assignment is served from cache:

```python
from django_async_backend.utils.contenttypes import aget_for_model

await aget_for_model(SaveModel)      # warms ContentType's cache

obj = GenericFkModel(name="x", content_object=target)
await obj.async_save()
```

It takes the same `using` and `for_concrete_model` arguments as
`get_for_model()`, and creates the `ContentType` row if it is missing.

## Pitfalls

```{danger}
**Never mix sync and async ORM calls.** Wrapping `Model.objects` in
`sync_to_async` lands on a *different* connection and therefore a different
transaction — it will not see uncommitted rows from a surrounding
`async_atomic()` block. Always go through `async_objects`.
```

:::{warning}
**There is no async related manager.** `instance.<related>.all()` is the sync
ORM, even on a model using `AsyncModelMixin`. It opens a synchronous
connection behind your back; a test teardown failing with *"database is being
accessed by other users"* is the usual symptom of one leaking. Query the
related model directly through its own `async_objects` manager instead:

```python
# not async — opens a sync connection
await sync_to_async(list)(author.book_set.all())

# do this instead
async for book in Book.async_objects.filter(author=author):
    ...
```
:::

## Compatibility

Legend: ✅ supported · ❌ not supported · ⚠️ supported with caveats

### QuerySet methods

| methods                             | supported | comments |
| ----------------------------------- | --------- | -------- |
| `Model.objects.aget`                | ✅        |          |
| `Model.objects.acreate`             | ✅        |          |
| `Model.objects.acount`              | ✅        |          |
| `Model.objects.none`                | ✅        |          |
| `Model.objects.abulk_create`        | ✅        |          |
| `Model.objects.abulk_update`        | ✅        |          |
| `Model.objects.aget_or_create`      | ✅        |          |
| `Model.objects.aupdate_or_create`   | ✅        |          |
| `Model.objects.aearliest`           | ✅        |          |
| `Model.objects.alatest`             | ✅        |          |
| `Model.objects.afirst`              | ✅        |          |
| `Model.objects.alast`               | ✅        |          |
| `Model.objects.ain_bulk`            | ✅        |          |
| `Model.objects.adelete`             | ✅        |          |
| `Model.objects.aupdate`             | ✅        |          |
| `Model.objects.aexists`             | ✅        |          |
| `Model.objects.acontains`           | ✅        |          |
| `Model.objects.aexplain`            | ✅        |          |
| `Model.objects.araw`                | ❌        |          |
| `Model.objects.all`                 | ✅        |          |
| `Model.objects.filter`              | ✅        |          |
| `Model.objects.exclude`             | ✅        |          |
| `Model.objects.complex_filter`      | ✅        |          |
| `Model.objects.union`               | ✅        |          |
| `Model.objects.intersection`        | ✅        |          |
| `Model.objects.difference`          | ✅        |          |
| `Model.objects.select_related`      | ❌        |          |
| `Model.objects.select_for_update`   | ✅        |          |
| `Model.objects.prefetch_related`    | ❌        |          |
| `Model.objects.aaggregate`          | ❌        |          |
| `Model.objects.annotate`            | ✅        |          |
| `Model.objects.order_by`            | ✅        |          |
| `Model.objects.distinct`            | ✅        |          |
| `Model.objects.extra`               | ✅        |          |
| `Model.objects.reverse`             | ✅        |          |
| `Model.objects.defer`               | ⚠️        | not safe for async, will not be implemented — use `values`/`values_list` |
| `Model.objects.only`                | ⚠️        | not safe for async, will not be implemented — use `values`/`values_list` |
| `Model.objects.using`               | ✅        |          |
| `Model.objects.resolve_expression`  | ✅        |          |
| `Model.objects.ordered`             | ✅        |          |
| `Model.objects.values`              | ✅        |          |
| `Model.objects.values_list`         | ✅        |          |
| `Model.objects.dates`               | ✅        |          |
| `Model.objects.datetimes`           | ✅        |          |
| `Model.objects.alias`               | ✅        |          |
| `Model.objects.aiterator`           | ❌        |          |

### Dunder methods

| methods            | supported | comments |
| ------------------ | --------- | -------- |
| `__aiter__`        | ✅        |          |
| `__iter__`         | ⚠️        | raises `TypeError` — use `async for obj in qs` |
| `__len__`          | ⚠️        | raises `TypeError` — use `await qs.acount()` |
| `__contains__`     | ⚠️        | falls back to `__iter__`, so it raises `TypeError` too |
| `__bool__`         | ⚠️        | truth-testing falls back to `__len__`, so `if qs:` raises `TypeError` — use `await qs.aexists()` |
| `__repr__`         | ✅        |          |
| `__and__`          | ✅        |          |
| `__or__`           | ✅        |          |
| `__xor__`          | ✅        |          |
| `__getitem__`      | ✅        |          |

### Model methods

| methods                  | supported | comments                 |
| ------------------------ | --------- | ------------------------ |
| `Model.asave`            | ✅        | `async_save`             |
| `Model.adelete`          | ✅        | `async_delete`           |
| `Model.arefresh_from_db` | ✅        | `async_refresh_from_db`  |

### RawQuerySet

Not supported ❌

### Related managers

Not supported ❌ — `instance.<related>.all()` is the sync ORM. See
[Pitfalls](#pitfalls).

### Databases

Only **PostgreSQL** is supported, through {pypi}`psycopg` 3.
