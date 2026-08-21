from django_async_backend.db.new_connection import async_new_connection
from django_async_backend.db.utils import async_connections

__all__ = [
    "async_connections",
    "async_new_connection",
    "close_old_async_connections",
]


async def close_old_async_connections(**kwargs):
    # Close every configured connection alias. close() is a no-op on a
    # wrapper with no underlying connection, so walking aliases this
    # task hasn't touched is free. **kwargs lets the same function be
    # connected as a request_started / request_finished signal handler.
    for conn in async_connections.all():
        await conn.close()
