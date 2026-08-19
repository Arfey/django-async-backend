import asyncio

from django.utils.connection import BaseConnectionHandler


class BaseAsyncConnectionHandler(BaseConnectionHandler):

    async def close_all(self):
        await asyncio.gather(
            *[conn.close() for conn in self.all(initialized_only=True)]
        )
