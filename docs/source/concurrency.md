# Concurrency and parallelism

```{warning}
**Async does not mean parallel here.** Database queries are *not* run in
parallel by default.
```

A task gets **one connection per database alias**, and every async ORM and
cursor call in that task takes turns on it. Awaiting several queries in a row
therefore costs the sum of their times, not the maximum:

```python
# Three round trips, one after another, on the single connection bound
# to the "default" alias.
await Book.async_objects.acount()
await Author.async_objects.acount()
await Publisher.async_objects.acount()
```

Nor can you fan the work out onto that connection to speed it up. It is owned
by the task that created it, so handing it to another task — via
`asyncio.gather()`, `asyncio.create_task()` or `asyncio.TaskGroup` — raises
`RuntimeError` rather than quietly interleaving commands on it:

```python
# RuntimeError: gather() wraps each coroutine in its own task, and none of
# them owns the connection.
await asyncio.gather(
    Book.async_objects.acount(),
    Author.async_objects.acount(),
    Publisher.async_objects.acount(),
)
```

This is a deliberate design choice. See
[Transactions and asyncio tasks](transactions.md#transactions-and-asyncio-tasks)
for why sharing one connection between tasks corrupts transaction state.

## Running queries in parallel

To run queries truly in parallel you must opt in explicitly and give each one
its own connection, with `async_new_connection`:

```python
import asyncio

from django_async_backend.db import async_new_connection

results = await asyncio.gather(
    async_new_connection(Book.async_objects.acount()),
    async_new_connection(Author.async_objects.acount()),
    async_new_connection(Publisher.async_objects.acount()),
)
```

This is the same three counts as the failing example above, and now each one
runs on its own connection — so they really do execute concurrently, and the
whole thing costs about as long as the slowest query rather than the sum.

Without `async_new_connection` all three would reach for the calling task's
connection, and `gather()` would reject them with `RuntimeError`.

```{warning}
**Use this sparingly.** Every wrapped call opens a *real* database connection
for as long as it runs. Fanning out inside a request multiplies your connection
count by the width of the fan-out — ten parallel queries per request across
fifty concurrent requests is five hundred connections, and Postgres will refuse
new ones long before that. Reach for it when a specific query genuinely needs
to run alongside another, not as a default wrapper.
```

See [Async transactions](transactions.md#transactions-and-asyncio-tasks) for why
each task needs its own connection in the first place.
