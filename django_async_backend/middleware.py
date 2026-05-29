import asyncio

from django.utils.decorators import async_only_middleware

from django_async_backend.db import close_old_async_connections


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
        # Defensive cleanup before the request runs. Connection wrappers
        # are thread-local — shared across tasks on the same worker
        # thread — so a prior request's state can persist if its
        # post-cleanup didn't fully complete. close() is a no-op when
        # there's no underlying connection, so this is free in the
        # healthy case.
        await close_old_async_connections()
        try:
            return await get_response(request)
        finally:
            await asyncio.shield(close_old_async_connections())

    return middleware
