from contextlib import asynccontextmanager

from django.utils.connection import BaseConnectionHandler


class BaseAsyncConnectionHandler(BaseConnectionHandler):

    async def close_all(self):
        # close_all() is called from middleware cleanup paths that may
        # shield into a new task (asyncio.shield wraps in a sub-task),
        # so grant task sharing while closing — the alternative is
        # forcing every caller to inc/dec around the call.
        conns = self.all(initialized_only=True)
        for conn in conns:
            conn.inc_task_sharing()
        try:
            for conn in conns:
                await conn.close()
        finally:
            for conn in conns:
                conn.dec_task_sharing()

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
            new_connections = self.all()
            for conn in connections:
                self[conn.alias] = conn
            for conn in new_connections:
                await conn.close()
