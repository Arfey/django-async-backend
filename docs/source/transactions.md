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

An async connection is owned by the task that created it. Using it from a
**different** task — one spawned with `asyncio.create_task()`,
`asyncio.gather()`, or `asyncio.TaskGroup` — is rejected. This applies to *every*
operation on the connection, not just to opening a transaction: an ordinary
query from a foreign task is rejected too.

```python
async def writer():
    await Book.async_objects.acreate(name="fanout")   # different task

await asyncio.create_task(writer())
```

This is a deliberate guard, not a limitation to work around. Child tasks inherit
the parent's connection, so several of them issuing commands on it concurrently
would interleave those commands on one physical connection — corrupting
transaction state and silently losing writes.

The rule is the one SQLAlchemy states for `AsyncSession`: **one transaction per
task**, just as `Session` is one per thread. A connection inside a transaction is
a stateful, sequential object — commands are handled in the exact order they are
emitted, and the transaction's state advances with them. A single database
transaction that receives commands from several tasks at once has no analogue in
a relational database.

### What works instead

Same task — sequential blocks and nesting — is unaffected. Nesting in the same
task is still a savepoint:

```python
async with async_atomic():
    await create_instance("outer")
    async with async_atomic():        # savepoint
        await create_instance("inner")
```

For fan-out, give each task its own connection, so each can own its own
transaction:

```python
async def writer(i):
    async with async_atomic():
        await create_instance(f"i{i}")

await asyncio.gather(
    *(async_new_connection(writer(i)) for i in range(10))
)
```

It also works as a context manager, for a block rather than a call:

```python
async with async_new_connection():
    await create_instance("solo")
```

Each task gets an independent transaction: they commit and roll back
separately, so one failing does not undo the others.

```{danger}
**`async_atomic()` does not extend over a fan-out.**

    async with async_atomic():
        await asyncio.gather(
            *(async_new_connection(writer(i)) for i in range(10))
        )

One transaction cannot span several tasks. Do the work sequentially inside the
block, or use `async_new_connection` above and accept independent transactions.
```

```{warning}
Each `async_new_connection` call opens a **real** database connection for as
long as it runs, so a wide fan-out inside a request multiplies your connection
count and can exhaust the server's limit. Use it where a query genuinely needs
its own transaction or real parallelism — not as a default wrapper. See
[Running queries in parallel](concurrency.md#running-queries-in-parallel).
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
