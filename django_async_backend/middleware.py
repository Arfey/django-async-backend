import asyncio

from django.utils.decorators import sync_and_async_middleware

from django_async_backend.db import async_connections


@sync_and_async_middleware
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
    if asyncio.iscoroutinefunction(get_response):

        async def middleware(request):
            try:
                return await get_response(request)
            finally:
                await async_connections.close_all()

    else:

        def middleware(request):
            return get_response(request)

    return middleware
