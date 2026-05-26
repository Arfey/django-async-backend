from django_async_backend.db.utils import AsyncConnectionHandler

async_connections = AsyncConnectionHandler()


async def close_old_async_connections(**kwargs):
    # all() — not initialized_only=True — so close() runs for every
    # configured alias regardless of whether this task touched it yet.
    # Matches PR #10's signal-handler shape so the same function works
    # both as middleware cleanup and as a request_started /
    # request_finished signal handler. close() is a no-op on a wrapper
    # with no underlying connection, so the extra aliases are free.
    for conn in async_connections.all():
        await conn.close()
