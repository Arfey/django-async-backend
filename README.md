# Django Async Backend

> **Note:** This is a fork of [Arfey/django-async-backend](https://github.com/Arfey/django-async-backend), developed with AI assistance to explore the remaining blockers for a fully async Django ORM. The goal is to identify what works, what doesn't, and hopefully contribute something useful upstream.
>
> **Django core limitation** (not fixable in this package):
> - **Connection lifecycle**: Django's `request_finished` signal closes sync connections only. ASGI deployments need the `close_async_connections` middleware (see below) to return async connections to the pool.

## Installation & Django Integration

### 1. Install the package

```bash
pip install django-async-backend
```

### 2. Django settings

```python
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}

INSTALLED_APPS = [
    ...
    "django_async_backend",
    ...
]
```

---

## Middleware

When running under ASGI, add `close_async_connections` to `MIDDLEWARE` so connections are returned to the pool at the end of each request. Django's `request_finished` signal only closes sync connections.

```python
MIDDLEWARE = [
    "django_async_backend.middleware.close_async_connections",
    ...
]
```

---

## Connection Pooling

> Connection pooling is not supported when running under a WSGI server (including the Django development server), because WSGI creates a new event loop for each request. This prevents reliable management of connection pool state.

---

## Async Transactions

```python
from django_async_backend.db.transaction import async_atomic

async with async_atomic():
    await MyModel.async_object.acreate(name="test")
```

Nested `async_atomic` blocks create savepoints. Cross-task transaction reuse is detected and raises `RuntimeError`. Use `_independent_connection()` or a parent-level transaction instead.

---

## Model Support

Inherit from `AsyncModel` to get async instance methods:

```python
from django_async_backend.db.models.base import AsyncModel
from django_async_backend.db.models.manager import AsyncManager

class MyModel(AsyncModel, models.Model):
    name = models.CharField(max_length=100)
    async_object = AsyncManager()
```

`AsyncModel` checks at class definition time that any subclass overriding `save()` also overrides `asave()` (and same for `delete()`/`adelete()`), preventing silent logic skipping. Set `async_mro_strict=False` to opt out.

### QuerySet Methods

| Method | Status | Notes |
|--------|--------|-------|
| `aget`, `acreate`, `acount`, `aexists` | Supported | |
| `abulk_create` | Supported | With `ignore_conflicts` and `update_conflicts` |
| `abulk_update`, `aupdate`, `adelete` | Supported | |
| `aget_or_create`, `aupdate_or_create` | Supported | |
| `afirst`, `alast`, `aearliest`, `alatest` | Supported | |
| `ain_bulk`, `araw`, `aexplain`, `acontains` | Supported | |
| `aaggregate`, `aiterator` | Supported | |
| `filter`, `exclude`, `annotate`, `order_by` | Supported | |
| `select_related`, `prefetch_related` | Supported | |
| `select_for_update` | Supported | |
| `values`, `values_list`, `distinct` | Supported | |
| `dates`, `datetimes`, `alias` | Supported | |
| `union`, `intersection`, `difference` | Supported | |
| `defer`, `only`, `using`, `extra`, `reverse` | Supported | |
| `__aiter__`, `__getitem__` | Supported | |

### Model Instance Methods

| Method | Status | Notes |
|--------|--------|-------|
| `asave()` | Supported | Fires `pre_save`/`post_save` via `asend()` |
| `adelete()` | Supported | CASCADE/SET_NULL/PROTECT/RESTRICT, fires `pre_delete`/`post_delete` |
| `arefresh_from_db()` | Supported | |
| `aget_next_by_FOO()` | Supported | Auto-generated for date/datetime fields |
| `aget_previous_by_FOO()` | Supported | Auto-generated for date/datetime fields |

### Related Manager Methods

Reverse FK and M2M managers on `AsyncModel` subclasses automatically get async methods:

| Method | Reverse FK | M2M | Notes |
|--------|-----------|-----|-------|
| `aadd()` | Yes | Yes | Bulk and non-bulk modes for FK |
| `aremove()` | Yes (nullable FK only) | Yes | |
| `aclear()` | Yes (nullable FK only) | Yes | |
| `aset()` | Yes | Yes | With `clear=True/False` |
| `acreate()` | Yes | Yes | |
| `aget_or_create()` | Yes | Yes | |
| `aupdate_or_create()` | Yes | Yes | |

M2M operations fire `m2m_changed` signals via `asend()`.

---

## Running Tests

```bash
uv sync --group dev
uv run pytest
```

Tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to start a PostgreSQL instance automatically.
