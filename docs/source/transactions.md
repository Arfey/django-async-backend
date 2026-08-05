# Async transactions

Use `async_atomic` to run async database operations atomically. All changes
inside the block are committed together; if an error occurs, all changes are
rolled back.

## Basic usage

```python
from django_async_backend.db.transaction import async_atomic

async with async_atomic():
    await create_instance(1)
    # If no error, changes are committed
    # If error, changes are rolled back
```

## Rollback on error

If an exception is raised inside the block, all changes are rolled back:

```python
async with async_atomic():
    await create_instance(1)
    raise Exception("fail")  # Nothing is committed
```

## Nested transactions (savepoints)

You can nest `async_atomic` blocks. Each inner block creates a savepoint. If an
error occurs in the inner block, only its changes are rolled back; outer
changes remain.

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

## Transactions and asyncio tasks

An async connection is owned by the task that created it. Opening
`async_atomic()` from a **different** task — one spawned with
`asyncio.create_task()` or `asyncio.gather()` — is rejected:

```python
async def writer():
    async with async_atomic():        # different task
        await Book.async_objects.acreate(name="fanout")

await asyncio.create_task(writer())
```

```text
RuntimeError: Transactions cannot be used within nested tasks. Consider using
a higher-level transaction that encompasses all nested tasks, or establish a
separate connection for the task (e.g., _independent_connection).
```

This is a deliberate guard, not a limitation to work around. Child tasks
inherit the parent's connection, so several of them opening transactions on it
concurrently would interleave `BEGIN`/`COMMIT` on one physical connection —
corrupting transaction state and silently losing writes. The check runs before
`BEGIN`, so nothing is written when it fires.

### What works instead

Same task — sequential blocks and nesting — is unaffected. Nesting in the same
task is still a savepoint:

```python
async with async_atomic():
    await create_instance("outer")
    async with async_atomic():        # savepoint
        await create_instance("inner")
```

For fan-out, you have two options.

**One transaction around the fan-out.** The parent owns the transaction and the
child tasks just do queries inside it:

```python
async with async_atomic():
    await asyncio.gather(*(writer(i) for i in range(10)))
```

**A separate connection per task.** Each task gets its own connection, so each
can own its own transaction:

```python
async def writer(i):
    async with async_connections._independent_connection():
        async with async_atomic():
            await create_instance(f"i{i}")

await asyncio.gather(*(writer(i) for i in range(10)))
```

```{note}
`_independent_connection()` is a concept and not ready for production usage —
see [Concurrency and parallelism](concurrency.md#dep-0009). Where it fits, the
first option is the safer choice.
```

## Using `on_commit` with async transactions

You can register a callback to run after a successful transaction commit using
`connection.on_commit`.

```python
from django.db import DEFAULT_DB_ALIAS
from django_async_backend.db import async_connections

connection = async_connections[DEFAULT_DB_ALIAS]

async with async_atomic():
    await connection.on_commit(callback)
```

The callback may be **sync or async** — whatever it returns is awaited if it is
awaitable, so a coroutine function works directly:

```python
async def send_email(book_id):
    ...


async with async_atomic():
    book = await Book.async_objects.acreate(name="Django")
    await connection.on_commit(lambda: send_email(book.pk))
```

Sync and async callbacks can be mixed, and all of them run in registration
order once the outermost block commits. Pass `robust=True` to log an exception
from one callback and carry on with the rest instead of propagating it.

```{danger}
Never mix sync and async ORM calls in the same transaction. A `sync_to_async`
wrapper around `Model.objects` runs on a *different* connection, and therefore
a different transaction — it will not see uncommitted rows from the surrounding
`async_atomic()` block, and will not be rolled back with it. Use
`async_objects` throughout.
```
