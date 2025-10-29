import asyncio
from contextlib import asynccontextmanager

from django.utils.connection import BaseConnectionHandler


class BaseAsyncConnectionHandler(BaseConnectionHandler):

    async def close_all(self):
        await asyncio.gather(
            *[conn.close() for conn in self.all(initialized_only=True)]
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
