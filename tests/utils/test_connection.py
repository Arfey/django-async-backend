import asyncio
import contextvars
import threading
from unittest.mock import AsyncMock

from django.db import DEFAULT_DB_ALIAS
from django.test import SimpleTestCase
from django.test.utils import override_settings
from django.utils.connection import ConnectionDoesNotExist

from django_async_backend.db import async_connections
from django_async_backend.test import AsyncioTransactionTestCase
from django_async_backend.utils.connection import BaseAsyncConnectionHandler

default_settings = {
    "first": {
        "key": "value1",
    },
    "second": {
        "key": "value2",
    },
}


class AsyncConnection:
    def __init__(self, alias):
        self.alias = alias
        self.close = AsyncMock()


class BaseAsyncConnectionHandlerExample(BaseAsyncConnectionHandler):
    def create_connection(self, alias):
        return AsyncConnection(alias)


class BaseAsyncConnectionHandlerTest(SimpleTestCase):
    def setUp(self):
        self.handler = BaseAsyncConnectionHandlerExample(default_settings)

    def test_settings_from_class(self):
        self.assertEqual(self.handler.settings, default_settings)

    @override_settings(setting_name=default_settings)
    def test_settings_from_django_config(self):
        class CustomNameConnectorHandler(BaseAsyncConnectionHandler):
            settings_name = "setting_name"

        handler = CustomNameConnectorHandler()
        self.assertEqual(handler.settings, default_settings)

    def test_create_connection(self):
        class ConnectionHandler(BaseAsyncConnectionHandler):
            def create_connection(self, alias):
                return super().create_connection(alias)

        msg = "Subclasses must implement create_connection()."
        with self.assertRaisesMessage(NotImplementedError, msg):
            ConnectionHandler().create_connection(None)

    def test_iter(self):
        self.assertEqual(list(self.handler), list(default_settings))

    def test_getitem(self):
        conn1 = self.handler["first"]
        self.assertEqual(conn1.alias, "first")
        self.assertEqual(id(conn1), id(self.handler["first"]))

        conn2 = self.handler["second"]
        self.assertEqual(conn2.alias, "second")
        self.assertEqual(id(conn2), id(self.handler["second"]))

    def test_getitem_wrong_alias(self):
        msg = "The connection 'wrong_alias' doesn't exist."
        with self.assertRaisesRegex(ConnectionDoesNotExist, msg):
            self.handler["wrong_alias"]

    def test_setitem(self):
        conn = AsyncConnection("my custom")
        self.handler["first"] = conn

        self.assertEqual(self.handler["first"], conn)

    def test_delitem(self):
        conn = self.handler["first"]
        del self.handler["first"]

        self.assertNotEqual(conn, self.handler["first"])

    def test_all(self):
        connections = self.handler.all()

        self.assertEqual(len(connections), 2)

        conn1, conn2 = connections
        self.assertEqual(conn1.alias, "first")
        self.assertEqual(conn2.alias, "second")

    def test_all_initialized_only(self):
        self.assertEqual(self.handler.all(initialized_only=True), [])

    async def test_close_all(self):
        conn1 = self.handler["first"]
        conn2 = self.handler["second"]

        conn1.close.assert_not_called()
        conn2.close.assert_not_called()

        await self.handler.close_all()

        conn1.close.assert_called_once_with()
        conn2.close.assert_called_once_with()

    async def test_independent_connection_async_concurrent(self):
        connections = []
        origin_conn = self.handler["first"]

        async def load():
            self.assertEqual(origin_conn, self.handler["first"])

            async with self.handler._independent_connection():
                connections.append(self.handler["first"])

        await asyncio.gather(load(), load(), load())
        connections_ids = set([id(conn) for conn in connections])

        self.assertEqual(len(connections_ids), 3)
        self.assertEqual(len(connections_ids | {id(origin_conn)}), 4)

        for conn in connections:
            conn.close.assert_called_once_with()

        self.assertEqual(origin_conn, self.handler["first"])

    async def test_independent_connection_nested(self):
        connections = []
        origin_conn = self.handler["first"]

        async with self.handler._independent_connection():
            conn1 = self.handler["first"]
            connections.append(conn1)

            async with self.handler._independent_connection():
                conn2 = self.handler["first"]
                connections.append(conn2)
                self.assertNotEqual(conn1, conn2)

            self.assertEqual(conn1, self.handler["first"])

            async with self.handler._independent_connection():
                conn3 = self.handler["first"]
                connections.append(conn3)
                self.assertNotEqual(conn1, conn3)

            self.assertEqual(conn1, self.handler["first"])

        connections_ids = set([id(conn) for conn in connections])

        self.assertEqual(len(connections_ids), 3)
        self.assertEqual(len(connections_ids | {id(origin_conn)}), 4)

        for conn in connections:
            conn.close.assert_called_once_with()

        self.assertEqual(origin_conn, self.handler["first"])

    async def test_independent_connection_with_exception(self):
        class AsyncConnectionWithException:
            def __init__(self, alias):
                self.alias = alias
                self.close = AsyncMock()
                self.close.side_effect = Exception

        class BaseAsyncConnectionHandlerWithError(
            BaseAsyncConnectionHandlerExample
        ):
            def create_connection(self, alias):
                return AsyncConnectionWithException(alias)

        handler = BaseAsyncConnectionHandlerWithError(default_settings)
        origin_conn = handler["first"]
        connections = []

        async def load():
            async with handler._independent_connection():
                connections.append(handler["first"])

        with self.assertRaises(Exception):
            await asyncio.create_task(load())

        with self.assertRaises(Exception):
            async with handler._independent_connection():
                connections.append(handler["first"])

        connections_ids = set([id(conn) for conn in connections])

        self.assertEqual(len(connections_ids), 2)
        self.assertEqual(len(connections_ids | {id(origin_conn)}), 3)

        for conn in connections:
            conn.close.assert_called_once_with()

        self.assertEqual(origin_conn, handler["first"])

    async def test_thread_critical_for_threads(self):
        origin_connections_map = {}
        separate_connections_map = {}

        origin_connections_map["first"] = self.handler["first"]

        def target():
            separate_connections_map["first"] = self.handler["first"]
            separate_connections_map["second"] = self.handler["second"]

        t = threading.Thread(target=target)
        t.start()
        t.join()

        origin_connections_map["second"] = self.handler["second"]

        self.assertNotEqual(
            origin_connections_map["first"],
            separate_connections_map["first"],
        )
        self.assertNotEqual(
            origin_connections_map["second"],
            separate_connections_map["second"],
        )

    async def test_thread_critical_for_coroutines(self):
        origin_connections_map = {}
        separate_connections_map = {}

        origin_connections_map["first"] = self.handler["first"]

        async def load():
            separate_connections_map["first"] = self.handler["first"]
            separate_connections_map["second"] = self.handler["second"]

        await asyncio.create_task(load())

        origin_connections_map["second"] = self.handler["second"]

        self.assertEqual(
            origin_connections_map["first"],
            separate_connections_map["first"],
        )
        self.assertNotEqual(
            origin_connections_map["second"],
            separate_connections_map["second"],
        )


async def _count_pg_backends():
    """Count other connections to this database in pg_stat_activity."""
    async with await async_connections[DEFAULT_DB_ALIAS].cursor() as c:
        res = await c.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() "
            "AND pid != pg_backend_pid()"
        )
        row = await res.fetchone()
        return row[0]


class AsyncConnectionCleanupTests(AsyncioTransactionTestCase):
    """
    In ASGI, each request runs in its own asyncio task with a fresh
    ContextVar context. Unlike sync Django (which uses the
    request_finished signal), there is no built-in mechanism to close
    async connections when a request ends. Without explicit cleanup,
    every request leaks a connection that is never returned to the pool.
    """

    async def test_connections_leak_without_cleanup(self):
        """Requests without close_all() leak connections."""
        baseline = await _count_pg_backends()

        n_requests = 20
        for _ in range(n_requests):

            async def fake_request():
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as c:
                    await c.execute("SELECT 1")

            await asyncio.create_task(
                fake_request(), context=contextvars.Context()
            )

        after = await _count_pg_backends()
        leaked = after - baseline
        self.assertGreaterEqual(
            leaked,
            n_requests // 2,
            f"Expected leaked connections without cleanup, "
            f"got {leaked}.",
        )

    async def test_connections_returned_with_cleanup(self):
        """Requests with close_all() return connections to the pool."""
        baseline = await _count_pg_backends()

        n_requests = 20
        for _ in range(n_requests):

            async def fake_request():
                async with await async_connections[
                    DEFAULT_DB_ALIAS
                ].cursor() as c:
                    await c.execute("SELECT 1")
                await async_connections.close_all()

            await asyncio.create_task(
                fake_request(), context=contextvars.Context()
            )

        after = await _count_pg_backends()
        leaked = after - baseline
        self.assertLessEqual(
            leaked,
            2,
            f"Expected ~0 leaked connections with cleanup, "
            f"got {leaked}.",
        )
