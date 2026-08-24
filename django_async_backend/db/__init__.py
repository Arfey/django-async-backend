import asyncio

from django_async_backend.db.new_connection import async_new_connection
from django_async_backend.db.utils import async_connections

__all__ = [
    "async_connections",
    "async_new_connection",
    "close_old_async_connections",
]


async def close_old_async_connections(**kwargs):
    """Release the connections this context opened.

    Connected to request_started and request_finished. Django runs an
    async receiver in a task of its own and copies its context changes
    back out, so what this leaves in the store is what the request
    inherits, owning task included.
    """
    # all() would build a wrapper per unopened alias: owned by this task,
    # and impossible to build at all for a non-async alias.
    conns = async_connections.all(initialized_only=True)

    # Dropped, not just closed: close() keeps the wrapper and its owner.
    # One in an atomic block stays, as the tombstone close() makes of it;
    # replacing it would put the rest of the block outside its
    # transaction.
    for conn in conns:
        if not conn.in_atomic_block:
            del async_connections[conn.alias]

    # The store is already clear, so a close that fails can neither
    # strand nor leak the others.
    results = await asyncio.gather(
        *(conn.close() for conn in conns), return_exceptions=True
    )

    for result in results:
        if isinstance(result, BaseException):
            raise result
