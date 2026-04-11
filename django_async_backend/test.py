from unittest import IsolatedAsyncioTestCase

from django.core.signals import request_started
from django.db import reset_queries

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic


class AsyncioTransactionTestCase(IsolatedAsyncioTestCase):
    # TODO: fix problem with creating models
    pass


class AsyncioTestCase(AsyncioTransactionTestCase):
    async def _init_transaction(self):
        self.connections = {}
        self.atomic_cms = {}
        self.atomics = {}

        for name in async_connections.settings.keys():
            connection = async_connections[name]
            self.connections[name] = connection
            atomic = async_atomic(name)
            atomic._from_testcase = True
            self.atomic_cms[name] = atomic
            self.atomics[name] = await self.atomic_cms[name].__aenter__()

    async def _close_transaction(self):
        for name in async_connections.settings.keys():
            connection = async_connections[name]
            connection.set_rollback(True)
            await self.atomic_cms[name].__aexit__(None, None, None)
            await connection.close()

    def _callSetUp(self):
        # Force loop to be initialized and set as the current loop
        # so that setUp functions can use get_event_loop() and get the
        # correct loop instance.
        self._asyncioRunner.get_loop()
        self._asyncioTestContext.run(self.setUp)
        self._callAsync(self._init_transaction)
        self._callAsync(self.asyncSetUp)

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
        return self.connection.queries[slice(self.initial_queries, self.final_queries)]

    async def __aenter__(self):
        self.force_debug_cursor = self.connection.force_debug_cursor
        self.connection.force_debug_cursor = True
        # Run any initialization queries if needed so that they won't be
        # included as part of the count.
        await self.connection.ensure_connection()
        self.initial_queries = len(self.connection.queries_log)
        self.final_queries = None
        self.reset_queries_disconnected = request_started.disconnect(reset_queries)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.connection.force_debug_cursor = self.force_debug_cursor
        if self.reset_queries_disconnected:
            request_started.connect(reset_queries)
        if exc_type is not None:
            return
        self.final_queries = len(self.connection.queries_log)
