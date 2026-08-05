# Concurrency and parallelism

```{warning}
**Async does not mean parallel here.** Database queries are *not* run in
parallel by default.
```

Within a single async context (such as one request or task), all async ORM and
cursor calls share **one connection per database alias**. Because they share a
connection, queries are serialized — running them under `asyncio.gather()` does
**not** execute them on the database concurrently; they take turns on the
shared connection.

```python
# These three queries do NOT run in parallel. They are serialized on the
# single connection bound to the "default" alias.
await asyncio.gather(
    Book.async_objects.acount(),
    Author.async_objects.acount(),
    Publisher.async_objects.acount(),
)
```

This is a deliberate design choice that mirrors Django's DEP 0009.

## Why this matters

The practical consequence is that async here buys you **non-blocking I/O**, not
query parallelism. While a query is in flight the event loop is free to serve
other requests — which is the win over `sync_to_async`, where a thread is
parked for the duration. What it does not buy you is a single request finishing
its five queries five times faster.

```{seealso}
[Connection handler](connections.md#connection-handler) for how connections
are bound to an async context.
```

## DEP 0009

<https://github.com/django/deps/blob/main/accepted/0009-async.rst>

> Whenever a `new_connections()` block is entered, Django sets a new context
> with new database connections.

To run queries truly in parallel you must opt in explicitly and give each one
its own connection. To show how that might look with the current
implementation, there is the `_independent_connection` context manager. Inside
the block, each async context gets its **own** connection, so queries issued
from separate tasks really do run in parallel:

```python
import asyncio

from django_async_backend.db import async_connections


async def run_query():
    async with async_connections._independent_connection():
        conn = async_connections["default"]

        async with await conn.cursor() as cursor:
            await cursor.execute("SELECT ...")
            return await cursor.fetchall()


results = await asyncio.gather(run_query(), run_query(), run_query())
```

Without the `_independent_connection()` block the three `run_query()` calls
would share one connection and take turns on it, as described above.

```{warning}
This is a concept that is **not ready for production usage**. The leading
underscore is deliberate: the name and semantics may change.
```
