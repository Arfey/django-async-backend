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
        # Per issue #24: walking async_connections.all() (not
        # initialized_only=True) ensures wrappers created in gather-child
        # task contexts get a parent-visible close(). close() is a no-op
        # when there's no underlying connection, so in the healthy case
        # the pre-request call costs nothing — it exists to mop up state
        # that survived a prior request on the same worker thread.
        await close_old_async_connections()
        try:
            return await get_response(request)
        finally:
            await asyncio.shield(close_old_async_connections())

    return middleware
