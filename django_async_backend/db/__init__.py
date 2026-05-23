from django_async_backend.db.utils import AsyncConnectionHandler

async_connections = AsyncConnectionHandler()


async def close_old_async_connections(**kwargs):
    # don't use initialized_only=True
    # https://github.com/Arfey/django-async-backend/issues/24
    for conn in async_connections.all():
        await conn.close()
