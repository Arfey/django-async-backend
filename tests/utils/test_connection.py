import asyncio
import threading
from unittest.mock import AsyncMock

import pytest
from django.test.utils import override_settings
from django.utils.connection import ConnectionDoesNotExist

from django_async_backend.utils.connection import BaseAsyncConnectionHandler

default_settings = {
    "first": {"key": "value1"},
    "second": {"key": "value2"},
}


class AsyncConnection:
    def __init__(self, alias):
        self.alias = alias
        self.close = AsyncMock()


class BaseAsyncConnectionHandlerExample(BaseAsyncConnectionHandler):
    def create_connection(self, alias):
        return AsyncConnection(alias)


@pytest.fixture
def handler():
    return BaseAsyncConnectionHandlerExample(default_settings)


def test_settings_from_class(handler):
    assert handler.settings == default_settings


def test_settings_from_django_config():
    class CustomNameConnectorHandler(BaseAsyncConnectionHandler):
        settings_name = "setting_name"

    with override_settings(setting_name=default_settings):
        custom = CustomNameConnectorHandler()
        assert custom.settings == default_settings


def test_create_connection_not_implemented():
    class ConnectionHandler(BaseAsyncConnectionHandler):
        def create_connection(self, alias):
            return super().create_connection(alias)

    with pytest.raises(NotImplementedError, match="Subclasses must implement create_connection()."):
        ConnectionHandler().create_connection(None)


def test_iter(handler):
    assert list(handler) == list(default_settings)


def test_getitem_returns_same_instance(handler):
    conn1 = handler["first"]
    assert conn1.alias == "first"
    assert id(conn1) == id(handler["first"])

    conn2 = handler["second"]
    assert conn2.alias == "second"
    assert id(conn2) == id(handler["second"])


def test_getitem_wrong_alias_raises(handler):
    with pytest.raises(ConnectionDoesNotExist, match="The connection 'wrong_alias' doesn't exist."):
        handler["wrong_alias"]


def test_setitem(handler):
    conn = AsyncConnection("my custom")
    handler["first"] = conn
    assert handler["first"] == conn


def test_delitem_creates_new_instance(handler):
    conn = handler["first"]
    del handler["first"]
    assert conn != handler["first"]


def test_all_returns_all_connections(handler):
    connections = handler.all()
    assert len(connections) == 2
    conn1, conn2 = connections
    assert conn1.alias == "first"
    assert conn2.alias == "second"


def test_all_initialized_only_empty_when_not_accessed(handler):
    assert handler.all(initialized_only=True) == []


async def test_close_all_closes_each_connection(handler):
    conn1 = handler["first"]
    conn2 = handler["second"]
    conn1.close.assert_not_called()
    conn2.close.assert_not_called()

    await handler.close_all()

    conn1.close.assert_called_once_with()
    conn2.close.assert_called_once_with()


async def test_independent_connection_async_concurrent(handler):
    connections = []
    origin_conn = handler["first"]

    async def load():
        assert origin_conn == handler["first"]
        async with handler._independent_connection():
            connections.append(handler["first"])

    await asyncio.gather(load(), load(), load())
    connections_ids = {id(conn) for conn in connections}

    assert len(connections_ids) == 3
    assert len(connections_ids | {id(origin_conn)}) == 4

    for conn in connections:
        conn.close.assert_called_once_with()

    assert origin_conn == handler["first"]


async def test_independent_connection_nested(handler):
    connections = []
    origin_conn = handler["first"]

    async with handler._independent_connection():
        conn1 = handler["first"]
        connections.append(conn1)

        async with handler._independent_connection():
            conn2 = handler["first"]
            connections.append(conn2)
            assert conn1 != conn2

        assert conn1 == handler["first"]

        async with handler._independent_connection():
            conn3 = handler["first"]
            connections.append(conn3)
            assert conn1 != conn3

        assert conn1 == handler["first"]

    connections_ids = {id(conn) for conn in connections}
    assert len(connections_ids) == 3
    assert len(connections_ids | {id(origin_conn)}) == 4

    for conn in connections:
        conn.close.assert_called_once_with()

    assert origin_conn == handler["first"]


async def test_independent_connection_with_exception():
    class AsyncConnectionWithException:
        def __init__(self, alias):
            self.alias = alias
            self.close = AsyncMock()
            self.close.side_effect = Exception

    class BaseAsyncConnectionHandlerWithError(BaseAsyncConnectionHandlerExample):
        def create_connection(self, alias):
            return AsyncConnectionWithException(alias)

    handler = BaseAsyncConnectionHandlerWithError(default_settings)
    origin_conn = handler["first"]
    connections = []

    async def load():
        async with handler._independent_connection():
            connections.append(handler["first"])

    with pytest.raises(Exception):
        await asyncio.create_task(load())

    with pytest.raises(Exception):
        async with handler._independent_connection():
            connections.append(handler["first"])

    connections_ids = {id(conn) for conn in connections}
    assert len(connections_ids) == 2
    assert len(connections_ids | {id(origin_conn)}) == 3

    for conn in connections:
        conn.close.assert_called_once_with()

    assert origin_conn == handler["first"]


async def test_thread_critical_isolates_connections_between_threads(handler):
    origin_connections_map = {}
    separate_connections_map = {}
    origin_connections_map["first"] = handler["first"]

    def target():
        separate_connections_map["first"] = handler["first"]
        separate_connections_map["second"] = handler["second"]

    t = threading.Thread(target=target)
    t.start()
    t.join()

    origin_connections_map["second"] = handler["second"]

    assert origin_connections_map["first"] != separate_connections_map["first"]
    assert origin_connections_map["second"] != separate_connections_map["second"]


async def test_thread_critical_shares_connections_within_task(handler):
    origin_connections_map = {}
    separate_connections_map = {}
    origin_connections_map["first"] = handler["first"]

    async def load():
        separate_connections_map["first"] = handler["first"]
        separate_connections_map["second"] = handler["second"]

    await asyncio.create_task(load())

    origin_connections_map["second"] = handler["second"]

    assert origin_connections_map["first"] == separate_connections_map["first"]
    assert origin_connections_map["second"] != separate_connections_map["second"]
