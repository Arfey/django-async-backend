from unittest import IsolatedAsyncioTestCase

from django.test.utils import (
    setup_test_environment,
    teardown_test_environment,
)

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic


class AsyncioTransactionTestCase(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        setup_test_environment()

    @classmethod
    def tearDownClass(cls):
        teardown_test_environment()
        super().tearDownClass()


class AsyncioTestCase(AsyncioTransactionTestCase):

    async def _init_transaction(self):
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
