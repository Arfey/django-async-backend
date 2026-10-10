import asyncio
from contextlib import asynccontextmanager
from functools import wraps
from inspect import iscoroutinefunction

from django.db import DEFAULT_DB_ALIAS

from django_async_backend.db.utils import async_connections


@asynccontextmanager
async def _new_connection_scope():
    """
    Swap in a fresh connection per alias for the duration of the block,
    then close them and put the previous ones back.

    The previous connections are restored rather than dropped: the caller
    may already be using one, and orphaning it would silently open a
    second connection on its next query.
    """
    previous = async_connections.all()

    for conn in previous:
        async_connections[conn.alias] = async_connections.create_connection(
            conn.alias
        )

    try:
        yield
    finally:
        opened = async_connections.all()

        close_task = asyncio.gather(*[conn.close() for conn in opened])

        for conn in previous:
            async_connections[conn.alias] = conn

        await close_task


def async_new_connection(arg=None):
    """
    Run async ORM code on its own database connection.

    Each call gets connections that no other task shares, so the work can
    own its own transaction and really run in parallel. Usable three ways::

        # wrap a coroutine
        await async_new_connection(select_books())

        # decorate an async function
        @async_new_connection
        async def select_books():
            ...

        # as an async context manager
        async with async_new_connection():
            ...

    The caller's own connections are put back when the block exits, so a
    surrounding task keeps using the connection it already had::

        async with asyncio.TaskGroup() as tg:
            tg.create_task(select_books())
            tg.create_task(select_books())
    """
    if arg is None:
        return _new_connection_scope()

    if iscoroutinefunction(arg):

        @wraps(arg)
        async def inner(*args, **kwargs):
            async with _new_connection_scope():
                return await arg(*args, **kwargs)

        return inner

    if asyncio.iscoroutine(arg):
        return _run_coroutine(arg)

    raise TypeError(
        "async_new_connection() expects a coroutine, an async function, or "
        "no argument (for use as a context manager), got %r." % type(arg)
    )


async def _run_coroutine(coro):
    async with _new_connection_scope():
        return await coro


@asynccontextmanager
async def async_acquire(using=DEFAULT_DB_ALIAS, allow_outside_atomic=False):
    """
    Yield a connection for ``using``, opening one only when needed.

    Reuses the current task's connection. If there is none, or it belongs
    to another task (e.g. inside ``asyncio.gather()``), opens a new one and
    closes it on exit::

        async with async_acquire() as conn:
            await Book.async_objects.acreate(title="Dune")

    Raises if the new connection would escape the parent's atomic block,
    unless ``allow_outside_atomic=True``.
    """
    task = asyncio.current_task()
    initialized = hasattr(async_connections._connections, using)

    if initialized:
        previous = async_connections[using]

        if previous._task is task:
            yield previous
            return

        # A new connection runs in its own transaction and can't see the
        # parent's uncommitted data. That's rarely intended, so require an
        # explicit opt-in.
        if previous.in_atomic_block and not allow_outside_atomic:
            raise RuntimeError(
                "async_acquire() would open a new connection for alias "
                "'%s' outside the parent task's transaction, so it couldn't "
                "see that transaction's uncommitted data. If that's "
                "intended, pass allow_outside_atomic=True." % using
            )

        conn = async_connections.create_connection(using)
        async_connections[using] = conn
    else:
        previous = None
        conn = async_connections[using]

    # Claim it up front so nested async_acquire() calls reuse it.
    conn._task = task

    try:
        yield conn
    finally:
        if previous is None:
            del async_connections[using]
        else:
            async_connections[using] = previous
        await conn.close()
