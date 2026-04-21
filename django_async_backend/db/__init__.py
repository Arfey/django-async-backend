from django_async_backend.db.utils import AsyncConnectionHandler

async_connections = AsyncConnectionHandler()


async def close_old_async_connections(**kwargs):
    for conn in async_connections.all(initialized_only=True):
        await conn.close()
