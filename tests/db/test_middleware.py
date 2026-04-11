"""Tests for close_async_connections middleware."""

import copy

import pytest
from django.db import DEFAULT_DB_ALIAS
from django.http import HttpResponse
from django.test import RequestFactory

from django_async_backend.db import async_connections
from django_async_backend.middleware import close_async_connections


def _pool_connection(alias="pool_test", max_size=2):
    """Create a pooled connection with a small pool for testing."""
    connection = async_connections[DEFAULT_DB_ALIAS]
    new_connection = connection.copy(alias)
    new_connection.settings_dict = copy.deepcopy(connection.settings_dict)
    new_connection.settings_dict["OPTIONS"]["pool"] = {
        "min_size": 0,
        "max_size": max_size,
    }
    return new_connection


@pytest.fixture
def rf():
    return RequestFactory()


class TestCloseAsyncConnections:
    """Verify the middleware returns connections to the pool after each request."""

    @pytest.mark.asyncio
    async def test_connections_closed_after_request(self, rf):
        """Middleware calls close_all() so the connection is released."""
        request = rf.get("/")

        async def get_response(request):
            # Open a connection by executing a query
            connection = async_connections[DEFAULT_DB_ALIAS]
            async with await connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
            return HttpResponse("ok")

        handler = close_async_connections(get_response)
        await handler(request)

        # After the middleware runs, the connection should be closed
        connection = async_connections[DEFAULT_DB_ALIAS]
        assert connection.connection is None

    @pytest.mark.asyncio
    async def test_connections_closed_on_exception(self, rf):
        """Connections are cleaned up even when the view raises."""
        request = rf.get("/")

        async def get_response(request):
            connection = async_connections[DEFAULT_DB_ALIAS]
            async with await connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
            raise RuntimeError("view error")

        handler = close_async_connections(get_response)
        with pytest.raises(RuntimeError, match="view error"):
            await handler(request)

        connection = async_connections[DEFAULT_DB_ALIAS]
        assert connection.connection is None

    @pytest.mark.asyncio
    async def test_pool_connections_returned(self, rf):
        """With a pooled connection, verify connections are returned to the pool."""
        new_connection = _pool_connection(max_size=1)
        try:
            request = rf.get("/")

            async def get_response(request):
                await new_connection.connect()
                async with await new_connection.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                return HttpResponse("ok")

            # Manually simulate what the middleware does for this connection
            await get_response(request)

            # Connection is checked out from the pool
            assert new_connection.connection is not None

            # Closing returns it to the pool
            await new_connection.close()
            assert new_connection.connection is None

            # We can get a new connection (pool not exhausted)
            await new_connection.connect()
            async with await new_connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
            assert new_connection.connection is not None
        finally:
            await new_connection.close()
            await new_connection.close_pool()

    @pytest.mark.asyncio
    async def test_pool_exhaustion_without_cleanup(self, rf):
        """Without cleanup, pool exhausts as connections are never returned."""
        from psycopg_pool import PoolTimeout

        new_connection = _pool_connection(
            alias="exhaust_test",
            max_size=1,
        )
        # Use a short timeout so the test doesn't hang
        new_connection.settings_dict["OPTIONS"]["pool"]["timeout"] = 0.5

        try:
            # Check out the only connection
            await new_connection.connect()
            async with await new_connection.cursor() as cursor:
                await cursor.execute("SELECT 1")

            # Try to get another connection from the same pool — should timeout
            conn2 = new_connection.copy()
            with pytest.raises(PoolTimeout):
                await conn2.connect()
        finally:
            await new_connection.close()
            await new_connection.close_pool()

    @pytest.mark.asyncio
    async def test_noop_when_no_connections(self, rf):
        """Middleware is harmless when no async connections were opened."""
        request = rf.get("/")

        async def get_response(request):
            return HttpResponse("ok")

        handler = close_async_connections(get_response)
        # Should not raise
        response = await handler(request)
        assert response.status_code == 200
