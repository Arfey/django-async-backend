# Django Async Backend

> **Note:** This is a fork of [Arfey/django-async-backend](https://github.com/Arfey/django-async-backend). Most of the missing async write operations and model instance methods were added here to explore what the remaining blockers are for a fully async Django ORM. Largely vibe-coded — functional but not production-ready.
>
> **Remaining blockers for full async:**
> - **Signals** — Django's signal system is synchronous. Write operations skip `pre_save`/`post_save`/`pre_delete`/`post_delete`.
> - **Cascade deletes** — `AsyncCollector` handles CASCADE/SET_NULL, but doesn't cover every edge case of Django's `Collector`.
> - **`Model.save()` parity** — `AsyncModel.asave()` bypasses `Model.save()`, so custom save overrides won't run.
> - **User model** — Not async-aware.

## Installation & Django Integration

### 1. Install the package

```bash
pip install django-async-backend
```

### 2. Django settings

In your settings.py, set the database engine to use the async backend

```python
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}
```

Make sure your Django app (and any other required apps) are listed in INSTALLED_APPS

```python
INSTALLED_APPS = [
    ...
    "django_async_backend",
    ...
]
```

---

## Connection Pooling

> ⚠️ Connection pooling is not supported when running under a WSGI server (including the Django development server), because WSGI creates a new event loop for each request. This prevents reliable management of connection pool state. To disable the warning, set `ASYNC_BACKEND_DISABLE_POOL_WARNING=True`

---

## Connection Handler

The connection handler manages database connections for your async backend.

```python
from django_async_backend.db import async_connections

connection = async_connections['default']

async with await connection.cursor() as cursor:
    await cursor.execute("SELECT ...")
    rows = await cursor.fetchall()

await connection.close()
```

- Connections are reused and managed automatically.
- Use await connection.close() to manually close a connection if needed.

## Cursor

Async cursors provide the following methods:

- `execute`
- `executemany`
- `fetchone`
- `fetchmany`
- `fetchall`

```python
async with await connection.cursor() as cursor:
    await cursor.execute("SELECT 1")
    row = await cursor.fetchone()
```

## Async Transactions with `async_atomic`

### Basic Usage

Use `async_atomic` to run async database operations atomically.
All changes inside the block are committed together; if an error occurs, all changes are rolled back.

```python
from django_async_backend.db.transaction import async_atomic

async with async_atomic():
    await create_instance(1)
    # If no error, changes are committed
    # If error, changes are rolled back
```

### Rollback on Error

If an exception is raised inside the block, all changes are rolled back:

```python
async with async_atomic():
    await create_instance(1)
    raise Exception("fail")  # Nothing is committed
```

### Nested Transactions (Savepoints)

You can nest async_atomic blocks.
Each inner block creates a savepoint.
If an error occurs in the inner block, only its changes are rolled back; outer changes remain.

```python
async with async_atomic():
    await create_instance(1)
    try:
        async with async_atomic():
            await create_instance(2)
            raise Exception("fail inner")  # Only instance 2 is rolled back
    except Exception:
        pass
# Only instance 1 is in the database
```

### Using `on_commit` with async transactions

You can register a callback to run after a successful transaction commit using `connection.on_commit`.

```python
connection = async_connections[DEFAULT_DB_ALIAS]

async with async_atomic():
    await connection.on_commit(callback)
```

## Writing Async Tests

### AsyncioTestCase

Use for async tests that do **not** require database transactions.

```python
from django_async_backend.test import AsyncioTestCase


class MyAsyncTests(AsyncioTestCase):
    async def asyncSetUp(self):
        # Setup code

    async def asyncTearDown(self):
        # Cleanup code

    async def test_something(self):
        # Your async test logic
        await do_async_stuff()
```

### AsyncioTransactionTestCase

Use for async tests that need database transaction support (rollbacks, atomic blocks).

```python
from django_async_backend.test import AsyncioTransactionTestCase


class MyTransactionTests(AsyncioTransactionTestCase):
    async def asyncSetUp(self):
        # Setup database

    async def asyncTearDown(self):
        # Cleanup database

    async def test_something(self):
        async with async_atomic():
            # DB operations here
            await do_db_stuff()
```


# ORM support:
### Manager:


```python
from django.db import models, DEFAULT_DB_ALIAS
from django_async_backend.db import async_connections
from django_async_backend.db.models.manager import AsyncManager

class Book(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)

    async_object = AsyncManager()

    class Meta:
        db_table = "books"


async def main():
    async for i in Book.async_object.all():
        print(i.id)

    await async_connections[DEFAULT_DB_ALIAS].close()
```



| methods                             | supported | comments |
| ----------------------------------- | --------- | -------- |
| `Model.objects.aget`                | ✅        |          |
| `Model.objects.acreate`             | ✅        | No signals |
| `Model.objects.acount`              | ✅        |          |
| `Model.objects.none`                | ✅        |          |
| `Model.objects.abulk_create`        | ✅        | No signals |
| `Model.objects.abulk_update`        | ✅        |          |
| `Model.objects.aget_or_create`      | ✅        | No signals |
| `Model.objects.aupdate_or_create`   | ✅        | No signals |
| `Model.objects.aearliest`           | ✅        |          |
| `Model.objects.alatest`             | ✅        |          |
| `Model.objects.afirst`              | ✅        |          |
| `Model.objects.alast`               | ✅        |          |
| `Model.objects.ain_bulk`            | ✅        |          |
| `Model.objects.adelete`             | ✅        | Cascade supported, no signals |
| `Model.objects.aupdate`             | ✅        |          |
| `Model.objects.aexists`             | ✅        |          |
| `Model.objects.acontains`           | ✅        |          |
| `Model.objects.aexplain`            | ✅        |          |
| `Model.objects.araw`                | ✅        |          |
| `Model.objects.all`                 | ✅        |          |
| `Model.objects.filter`              | ✅        |          |
| `Model.objects.exclude`             | ✅        |          |
| `Model.objects.complex_filter`      | ✅        |          |
| `Model.objects.union`               | ✅        |          |
| `Model.objects.intersection`        | ✅        |          |
| `Model.objects.difference`          | ✅        |          |
| `Model.objects.select_related`      | ✅        |          |
| `Model.objects.select_for_update`   | ✅        |          |
| `Model.objects.prefetch_related`    | ✅        |          |
| `Model.objects.annotate`            | ✅        |          |
| `Model.objects.order_by`            | ✅        |          |
| `Model.objects.distinct`            | ✅        |          |
| `Model.objects.extra`               | ✅        |          |
| `Model.objects.reverse`             | ✅        |          |
| `Model.objects.defer`               | ✅        |          |
| `Model.objects.only`                | ✅        |          |
| `Model.objects.using`               | ✅        |          |
| `Model.objects.resolve_expression`  | ✅        |          |
| `Model.objects.ordered`             | ✅        |          |
| `Model.objects.values`              | ✅        |          |
| `Model.objects.values_list`         | ✅        |          |
| `Model.objects.dates`               | ✅        |          |
| `Model.objects.datetimes`           | ✅        |          |
| `Model.objects.alias`               | ✅        |          |
| `__aiter__`                         | ✅        |          |
| `__repr__`                          | ✅        |          |
| `__len__`                           | ✅        |          |
| `__and__`                           | ✅        |          |
| `__or__`                            | ✅        |          |
| `__xor__`                           | ✅        |          |
| `__getitem__`                       | ✅        |          |
| `Model.objects.aiterator`           | ✅        |          |

### RawQuerySet

Supported via `araw()`:

```python
async for obj in Book.async_object.araw("SELECT * FROM books WHERE id > %s", [10]):
    print(obj.name)
```

### Model:

To use async model instance methods, inherit from `AsyncModel`:

```python
from django_async_backend.db.models.base import AsyncModel

class MyModel(AsyncModel, models.Model):
    ...
```

| methods         | supported | comments |
| --------------- | --------- | -------- |
| `Model.asave`   | ✅        | No signals, requires AsyncModel mixin |
| `Model.adelete` | ✅        | No cascade/signals, requires AsyncModel mixin |
| `...`           | ❌        |          |

### User Model / Manager

| methods                     | supported | comments |
| --------------------------- | --------- | -------- |
| `User.is_authenticated`     | ❌        |          |
| `User.is_super_user`        | ❌        |          |
| `User.objects.acreate_user` | ❌        |          |
| `...`                       | ❌        |          |


## ⚙️ Development Setup

**Install pre-commit hooks:**
```bash
pip install pre-commit
pre-commit install
```

**Install dependencies:**
```bash
poetry install --with dev
```

## 🧪 Running Tests

This project uses a comprehensive test suite powered by `unittest`.

**To run tests:**
```bash
docker-compose up postgres -d
DJANGO_SETTINGS_MODULE=settings poetry run python -m unittest discover -s tests
```

**Integration tests run locally.**

The `django_async_backend.db.backends.postgresql` backend is fully compatible with Django's default `django.db.backends.postgresql` backend, as it leverages the default implementation under the hood. To confirm this compatibility, run Django's test suite using the custom backend.

```python
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
    "other": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}
```

To execute them:

```bash
cd tests_django
docker-compose run --build --rm test_django_integration
```

## DEP 0009

https://github.com/django/deps/blob/main/accepted/0009-async.rst

> Whenever a new_connections() block is entered, Django sets a new context with new database connections.

To show show an example how it might looks like with a current implementation we have the `independent_connection` context manager.

```python
import asyncio
from django_async_backend.db import async_connections

async def run_query():
    async with async_connections._independent_connection():
        conn = await async_connections['default']
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT ...")
            return await cursor.fetchall()

results = await asyncio.gather(run_query(), run_query(), run_query())
```

It's just a concept that is not ready for production usage.
