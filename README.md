# Async Django ORM (Django Async Backend)

[![CI status](https://img.shields.io/github/actions/workflow/status/Arfey/django-async-backend/test.yml?logo=github&style=for-the-badge&labelColor=%23282828)](https://github.com/Arfey/django-async-backend/actions)
[![Latest Version in PyPI](https://img.shields.io/pypi/v/django-async-backend.svg?style=for-the-badge)](https://pypi.org/project/django-async-backend/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/django-async-backend.svg?style=for-the-badge)](https://pypi.org/project/django-async-backend/)

[![Monthly downloads](https://static.pepy.tech/personalized-badge/django-async-backend?period=month&units=international_system&left_color=grey&right_color=blue&left_text=downloads/month)](https://pepy.tech/projects/django-async-backend)

Async Django ORM and PostgreSQL database backend.

📖 **[Read the documentation](https://django-async-backend.readthedocs.io/en/latest/)**

---

## Installation

```bash
pip install django-async-backend[binary]
```

The `binary` extra installs the C-accelerated `psycopg` implementation. Without
it you get the pure-Python implementation, which is noticeably slower. If you
use connection pooling, add the `pool` extra as well:

```bash
pip install django-async-backend[binary,pool]
```

The package tracks Django's major and minor version — for example `6.0.x`
matches Django `6.0` — because a large part of the ORM layer is generated from
Django's own source.

## Quick start

```python
# settings.py
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}

INSTALLED_APPS = [
    ...
    "django_async_backend",
]
```

```python
from django.db import models
from django_async_backend.db import async_connections
from django_async_backend.db.models.base import AsyncModelMixin
from django_async_backend.db.transaction import async_atomic


class Book(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=100)


async def notify(book_id: int) -> None:
    ...


async def main() -> None:
    connection = async_connections["default"]

    async with async_atomic():
        book = await Book.async_objects.acreate(name="Django")

        # async callbacks are supported; runs only if the transaction commits
        await connection.on_commit(lambda: notify(book.pk))

        book.name = "Django Async"
        await book.async_save(update_fields=["name"])

        async with async_atomic():  # savepoint
            await Book.async_objects.filter(name="draft").adelete()

    print(await Book.async_objects.acount())

    async for row in Book.async_objects.order_by("name"):
        print(row.pk, row.name)
```

Or drop to a raw async cursor:

```python
async with await connection.cursor() as cursor:
    await cursor.execute("SELECT id, name FROM app_book ORDER BY name")

    print(cursor.rowcount)

    async for row in cursor:
        print(row)
```

> [!WARNING]
> **Async does not mean parallel.** Within one async context, all ORM and
> cursor calls share a single connection per database alias, so queries are
> serialized even under `asyncio.gather()`. This mirrors Django's
> [DEP 0009](https://github.com/django/deps/blob/main/accepted/0009-async.rst).

## Supported methods

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

| methods                  | supported | comments     |
| ------------------------ | --------- | ------------ |
| `Model.asave`            | ✅        | `async_save`   |
| `Model.adelete`          | ✅        | `async_delete` |
| `Model.arefresh_from_db` | ❌        |              |

### RawQuerySet

Not supported ❌

### Related managers

Not supported ❌ — `instance.<related>.all()` is the sync ORM. See
[Pitfalls](#pitfalls).
