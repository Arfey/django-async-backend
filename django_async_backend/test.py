from unittest import IsolatedAsyncioTestCase

from django.core.signals import request_started
from django.db import reset_queries

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic


class AsyncioTransactionTestCase(IsolatedAsyncioTestCase):
    # todo: fix problem with creating models

    async def _async_pre_setup(self):
        """Hook run between setUp and asyncSetUp.

        Subclasses can override to add their own pre-setup, but must call
        super() first so connection task-sharing is enabled before their
        setup touches any connection.
        """
        # IsolatedAsyncioTestCase runs setUp, test, and tearDown in
        # different asyncio tasks but tests expect to share a single
        # connection across them. Mark every configured connection as
        # task-shared for the duration of the test. Register dec as an
        # async cleanup so it runs after user cleanups (LIFO ordering
        # means the first-registered cleanup is the last to execute).
        self._shared_connections = []
        for name in async_connections.settings.keys():
            conn = async_connections[name]
            conn.inc_task_sharing()
            self._shared_connections.append(conn)
        self.addAsyncCleanup(self._exit_task_sharing)

    async def _exit_task_sharing(self):
        for conn in self._shared_connections:
            conn.dec_task_sharing()

    def _callSetUp(self):
        # Mirrors IsolatedAsyncioTestCase._callSetUp and inserts
        # _async_pre_setup between setUp and asyncSetUp. We can't super()
        # because we need to run code between those two hooks.
        self._asyncioRunner.get_loop()
        self._asyncioTestContext.run(self.setUp)
        self._callAsync(self._async_pre_setup)
        self._callAsync(self.asyncSetUp)


class AsyncioTestCase(AsyncioTransactionTestCase):

    async def _async_pre_setup(self):
        await super()._async_pre_setup()
        self.connections = {}
        self.atomic_cms = {}
        self.atomics = {}

        for name in async_connections.settings.keys():
            connection = async_connections[name]
            self.connections[name] = connection
            self.atomic_cms[name] = async_atomic(name)
            self.atomics[name] = await self.atomic_cms[name].__aenter__()

    async def _close_transaction(self):
        for name in async_connections.settings.keys():
            connection = async_connections[name]
            connection.set_rollback(True)
            await self.atomic_cms[name].__aexit__(None, None, None)
            await connection.close()

    def _callTearDown(self):
        self._callAsync(self.asyncTearDown)
        self._callAsync(self._close_transaction)
        self._asyncioTestContext.run(self.tearDown)


class AsyncCaptureQueriesContext:

    def __init__(self, connection):
        self.connection = connection

    def __len__(self):
        return len(self.captured_queries)

    @property
    def captured_queries(self):
        return self.connection.queries[
            slice(self.initial_queries, self.final_queries)
        ]

    async def __aenter__(self):
        self.force_debug_cursor = self.connection.force_debug_cursor
        self.connection.force_debug_cursor = True
        # Run any initialization queries if needed so that they won't be
        # included as part of the count.
        await self.connection.ensure_connection()
        self.initial_queries = len(self.connection.queries_log)
        self.final_queries = None
        self.reset_queries_disconnected = request_started.disconnect(
            reset_queries
        )
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.connection.force_debug_cursor = self.force_debug_cursor
        if self.reset_queries_disconnected:
            request_started.connect(reset_queries)
        if exc_type is not None:
            return
        self.final_queries = len(self.connection.queries_log)
