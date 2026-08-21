import asyncio
from contextlib import asynccontextmanager
from functools import wraps
from inspect import iscoroutinefunction

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
