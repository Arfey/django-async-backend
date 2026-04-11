import asyncio

from django.utils.decorators import async_only_middleware

from django_async_backend.db import async_connections


@async_only_middleware
def close_async_connections(get_response):
    """Close async DB connections after each request.

    Django's request_finished signal closes sync connections, but
    async_connections needs its own cleanup to return connections
    to the pool. Add this to MIDDLEWARE when using ASGI:

        MIDDLEWARE = [
            "django_async_backend.middleware.close_async_connections",
            ...
        ]
    """

    async def middleware(request):
        try:
            return await get_response(request)
        finally:
            await asyncio.shield(async_connections.close_all())

    return middleware
