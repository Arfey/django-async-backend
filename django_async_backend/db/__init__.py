from django_async_backend.db.utils import AsyncConnectionHandler

async_connections = AsyncConnectionHandler()


async def close_old_async_connections(**kwargs):
    # Close every configured connection alias. close() is a no-op on a
    # wrapper with no underlying connection, so walking aliases this
    # task hasn't touched is free. **kwargs lets the same function be
    # connected as a request_started / request_finished signal handler.
    for conn in async_connections.all():
        await conn.close()
