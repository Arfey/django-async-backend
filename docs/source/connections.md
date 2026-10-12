# Connections

## Connection handler

The connection handler manages database connections for your async backend.

```python
from django_async_backend.db import async_acquire

async with async_acquire() as connection:
    async with await connection.cursor() as cursor:
        await cursor.execute("SELECT ...")
        rows = await cursor.fetchall()
```

- `async_acquire()` reuses the connection the current task already owns, and
  opens (and closes on exit) a new one only when the task owns none. See
  [Acquiring a connection](#acquiring-a-connection) below.
- Use `await connection.close()` to manually close a connection if needed.

Each database alias in `DATABASES` gets its own connection within an async
context. See [Concurrency model](concurrency.md) for what that means for
concurrent queries.

## Acquiring a connection

`async_acquire()` is an async context manager that hands you a connection for
one database alias, opening a new one only when the current task doesn't
already own one.

```python
from django_async_backend.db import async_acquire

async with async_acquire() as connection:
    await Book.async_objects.acreate(name="Dune")
```

The ORM inside the block uses the same connection as `connection`, so you
don't need to pass it anywhere. It's there for the raw cursor or for
`connection.on_commit()`.

### Parameters

`async_acquire(using=DEFAULT_DB_ALIAS, allow_outside_atomic=False)`

- `using`: the alias in `DATABASES` to acquire a connection for. Defaults to
  `"default"`. An unknown alias raises `ConnectionDoesNotExist`.
- `allow_outside_atomic`: set to `True` to allow opening a new connection
  while the parent task is inside `async_atomic()`. Defaults to `False`; see
  [Inside a parent's transaction](#inside-a-parents-transaction).

### What it does

Which connection you get depends on what the current task already has for
`using`:

| Situation | Connection yielded | On exit |
| --- | --- | --- |
| The task already owns a connection | that same connection | left open, still owned by the task |
| No connection exists yet in this context | a new one, owned by the task | closed and removed |
| A connection exists but belongs to another task (e.g. inside `asyncio.gather()`, `create_task()` or a `TaskGroup`) | a new one, owned by this task | closed; the parent's connection is put back |

In every case the code inside the block can use the ORM and cursors without
hitting the "owned by another task" `RuntimeError`. Nested `async_acquire()`
blocks in the same task reuse the outer connection, so wrapping a helper in it
is safe even when the caller has already acquired one.

The connection is closed even if the block raises.

### Inside a parent's transaction

A new connection runs its own transaction and can't see the parent's
uncommitted rows. If the parent task is inside `async_atomic()` when a child
task calls `async_acquire()`, you'd almost certainly be surprised, so it
raises `RuntimeError` instead:

```python
async def writer():
    async with async_acquire():  # RuntimeError: ... outside the parent task's transaction
        ...


async with async_atomic():
    await asyncio.create_task(writer())
```

Pass `allow_outside_atomic=True` when that independence is what you want. The
child then commits on its own, and its work survives a rollback of the parent:

```python
async def audit():
    async with async_acquire(allow_outside_atomic=True):
        await AuditLog.async_objects.acreate(event="attempt")


async with async_atomic():
    await asyncio.create_task(audit())
    raise ValueError  # the parent rolls back, the audit row stays
```

```{warning}
When `async_acquire()` does open a connection, it's a real database
connection. Using it in every branch of a wide `asyncio.gather()` opens one
connection per branch.
```

## Cursors

`await connection.cursor()` returns an `AsyncCursorWrapper` around the
underlying {pypi}`psycopg` cursor. The most commonly used methods are:

- `execute` / `executemany` — awaitable;
- `fetchone` / `fetchmany` / `fetchall` — awaitable;
- `close` — awaitable, though the async context manager closes for you.

```python
async with await connection.cursor() as cursor:
    await cursor.execute("SELECT 1")
    row = await cursor.fetchone()
```

The wrapper is a thin proxy: it implements `execute` and `executemany` itself
(to run Django's `execute_wrappers` and translate database errors), and passes
everything else straight through to the psycopg cursor. So the rest of
psycopg's [cursor
API](https://www.psycopg.org/psycopg3/docs/api/cursors.html) is available too.
Because those come from psycopg unchanged, they keep psycopg's own calling
convention rather than being uniformly awaitable:

- `scroll` and `close` — `await` them;
- `stream` — an async generator: `async for row in cursor.stream(...)`;
- `copy` — an async context manager: `async with cursor.copy(...)`;
- `nextset` — a plain synchronous call;
- `rowcount`, `rownumber` and `description` — attributes, not calls.

```python
async with await connection.cursor() as cursor:
    await cursor.execute("SELECT id, name FROM app_book")

    print(cursor.rowcount)
    print([column.name for column in cursor.description])
```

Cursors are also async-iterable, which streams rows without materializing them
all at once:

```python
async with await connection.cursor() as cursor:
    await cursor.execute("SELECT id FROM app_book")

    async for row in cursor:
        print(row)
```

## Connection pooling

Pooling requires the `pool` extra:

```bash
pip install django-async-backend[binary,pool]
```

```{warning}
Connection pooling is not supported when running under a WSGI server
(including the Django development server), because WSGI creates a new event
loop for each request. This prevents reliable management of connection pool
state.

To disable the warning, set `ASYNC_BACKEND_DISABLE_POOL_WARNING=True`.
```

When running under ASGI, add the [connection-closing
middleware](setup.md#middleware) so connections are returned to the pool at the
end of each request.
