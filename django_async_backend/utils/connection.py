import asyncio
import contextvars
import logging
from contextlib import asynccontextmanager

from django.utils.connection import BaseConnectionHandler

logger = logging.getLogger("django_async_backend")

# Per-scope registry of every wrapper created inside the scope.
#
# Because contextvars copy *values* across task boundaries but the list is
# a mutable object, every asyncio.gather/create_task/TaskGroup child task
# inherits a reference to the same list. Wrappers appended by children
# are therefore visible to the task that opened the scope, even though
# the children's own contextvar dicts (per-task storage) are not.
#
# This is what lets a request-scoped cleanup hook reach connections
# acquired in gather-spawned child tasks.
_scope_wrappers: contextvars.ContextVar = contextvars.ContextVar(
    "django_async_backend.scope_wrappers", default=None
)


class BaseAsyncConnectionHandler(BaseConnectionHandler):

    def __getitem__(self, alias):
        # Reimplemented from BaseConnectionHandler so newly-created wrappers
        # can be registered into the active cleanup scope. Cached lookups
        # skip registration (already registered when first created).
        try:
            return getattr(self._connections, alias)
        except AttributeError:
            if alias not in self.settings:
                raise self.exception_class(
                    f"The connection '{alias}' doesn't exist."
                )
        conn = self.create_connection(alias)
        setattr(self._connections, alias, conn)
        scope = _scope_wrappers.get()
        if scope is not None:
            scope.append(conn)
        return conn

    async def close_all(self):
        await asyncio.gather(
            *[conn.close() for conn in self.all(initialized_only=True)]
        )

    @asynccontextmanager
    async def cleanup_scope(self):
        """Track wrappers created inside this block and close on exit.

        Use this at the boundary of any unit of work that owns its DB
        connections (HTTP request, background task, script entry point).
        Connections acquired in asyncio.gather / create_task / TaskGroup
        children are reachable from the scope's owning task and will be
        returned to the pool when the block exits.

        Limitation: an asyncio.create_task() that is not awaited inside
        the scope continues running with a reference to the scope's
        list. If it acquires a connection after the scope has begun
        teardown, that connection is not closed by this scope. Always
        await spawned tasks before exiting the scope (or wrap them in
        their own scope).
        """
        wrappers = []
        token = _scope_wrappers.set(wrappers)
        try:
            yield
        finally:
            _scope_wrappers.reset(token)
            if wrappers:
                # wrapper.close() is idempotent (no-ops when no
                # connection was acquired) so call it unconditionally.
                # Errors are logged, not re-raised, so one failed close
                # does not strand the rest. The shield ensures cleanup
                # completes even if the scope-owning task is cancelled
                # mid-exit (e.g. a cancelled HTTP request) -- otherwise
                # the cancellation would propagate into the gather and
                # leave some connections checked out.
                results = await asyncio.shield(
                    asyncio.gather(
                        *[w.close() for w in wrappers],
                        return_exceptions=True,
                    )
                )
                for w, result in zip(wrappers, results):
                    if isinstance(result, BaseException):
                        logger.exception(
                            "error closing connection '%s' on scope exit",
                            getattr(w, "alias", "?"),
                            exc_info=result,
                        )

    @asynccontextmanager
    async def _independent_connection(self):
        """
        Creates an isolated connection to enable parallel queries.
        Django reuses connections per-thread, which can block concurrent async
        queries. This context temporarily removes existing connections,
        allowing new, independent ones to be used inside the block.
        Example:
            async def load():
                async with connections._independent_connection():
                    await fetch_data(connections['default'])
            await asyncio.gather(load(), load(), load())
        """
        connections = self.all()

        try:
            for conn in connections:
                self[conn.alias] = self.create_connection(conn.alias)
            yield
        finally:
            close_task = asyncio.gather(*[conn.close() for conn in self.all()])

            for conn in connections:
                self[conn.alias] = conn

            await close_task
