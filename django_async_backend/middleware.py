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
            # asyncio.shield runs close_all in a sub-task, which would
            # fail validate_task_sharing. Explicitly allow the shielded
            # close to touch connections owned by the request task.
            conns = async_connections.all(initialized_only=True)
            for conn in conns:
                conn.inc_task_sharing()
            try:
                await asyncio.shield(async_connections.close_all())
            finally:
                for conn in conns:
                    conn.dec_task_sharing()

    return middleware
