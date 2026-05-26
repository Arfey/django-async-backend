from django_async_backend.db.utils import AsyncConnectionHandler

async_connections = AsyncConnectionHandler()


async def close_old_async_connections(**kwargs):
    # all() (not initialized_only=True) so we still close aliases that
    # were initialized in a gather child's contextvar copy and may not
    # appear in the request task's initialized list. close() is a no-op
    # when the wrapper has no underlying connection.
    for conn in async_connections.all():
        await conn.close()
