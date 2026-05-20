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

    The same cleanup_scope() pattern can be used as a baseline for
    non-HTTP entry points (background workers, scripts, management
    commands) -- wrap the unit of work in
    `async with async_connections.cleanup_scope(): ...`.
    """

    async def middleware(request):
        async with async_connections.cleanup_scope():
            return await get_response(request)

    return middleware
