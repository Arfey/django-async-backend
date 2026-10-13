import asyncio

from asgiref.sync import async_to_sync
from django.utils.decorators import async_only_middleware

from django_async_backend.db import close_old_async_connections


@async_only_middleware
def close_async_connections(get_response):
    """Close async DB connections after each request.

    Django's request_finished signal closes sync connections, but
    async_connections needs its own cleanup to return connections
    to the pool. It's a middleware, not a signal receiver, because a
    connection can only be used from the task that created it, and Django
    runs async receivers in separate subtasks. Add this to MIDDLEWARE when
    using ASGI:

        MIDDLEWARE = [
            "django_async_backend.middleware.close_async_connections",
            ...
        ]
    """

    async def middleware(request):
        # Init wrappers here so they're stored in this task's
        # context and closed below. If a gather() subtask created them,
        # they'd live in its copied context and leak.
        await close_old_async_connections()
        try:
            response = await get_response(request)
        finally:
            await asyncio.shield(close_old_async_connections())

        # Django runs the middleware to the end and only then streams the
        # body, so connections the body opens would never be closed. Close
        # them once the body is done. Only async bodies can run async
        # queries, so sync ones are left alone.
        if response.streaming and response.is_async:
            response.streaming_content = _close_after(
                response.streaming_content
            )
            # On a client disconnect the body is never finished, so also close
            # on response.close(), which Django still calls.
            response._resource_closers.append(
                async_to_sync(close_old_async_connections)
            )
        return response

    return middleware


async def _close_after(content):
    # A streaming body is consumed after this middleware has returned, so
    # close whatever connections it opened once it's done.
    try:
        async for chunk in content:
            yield chunk
    finally:
        await asyncio.shield(close_old_async_connections())
