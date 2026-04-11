"""Pytest configuration for django-async-backend tests."""

from __future__ import annotations

from contextlib import suppress
from os import environ

import django
import pytest

from tests.fixtures.containers import _start_postgres_container


def pytest_configure(config):
    """Start PostgreSQL container before Django settings are loaded."""
    info = _start_postgres_container()
    config._postgres_container = info

    environ["POSTGRES_HOST"] = info.host
    environ["POSTGRES_PORT"] = str(info.port)

    # Now configure Django with the dynamic settings
    environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    django.setup()

    # Create tables (syncdb) since we don't use migrations
    from django.core.management import call_command

    call_command("migrate", "--run-syncdb", verbosity=0)


def pytest_unconfigure(config):
    """Stop PostgreSQL container after all tests."""
    container_info = getattr(config, "_postgres_container", None)
    if container_info:
        with suppress(Exception):
            container_info.container.stop()


@pytest.fixture
async def reporter_table(async_db):
    """Create and drop a temporary reporter_table_tmp for tests that need it.

    Uses async_db so table setup runs inside the rolled-back test transaction.
    """
    from tests.fixtures.reporter_table import create_reporter_table, drop_reporter_table

    await create_reporter_table()
    yield
    await drop_reporter_table()


@pytest.fixture
async def reporter_table_transaction():
    """Create and drop a temporary reporter_table_tmp without an outer
    transaction. Use for tests that need to observe actual BEGIN/COMMIT/ROLLBACK
    statements or manage connection lifecycle directly.
    """
    from django_async_backend.db import async_connections
    from tests.fixtures.reporter_table import create_reporter_table, drop_reporter_table

    await create_reporter_table()
    try:
        yield
    finally:
        await drop_reporter_table()
        await async_connections["default"].close()


@pytest.fixture
async def async_db():
    """Wraps each test in an async transaction that rolls back on completion.

    Use for any async test that needs database access and should not persist
    its changes between tests.
    """
    from django_async_backend.db import async_connections
    from django_async_backend.db.transaction import async_atomic

    atomic_cms = {}
    for name in async_connections.settings:
        connection = async_connections[name]
        atomic = async_atomic(name)
        atomic._from_testcase = True
        atomic_cms[name] = atomic
        await atomic_cms[name].__aenter__()

    yield

    for name in async_connections.settings:
        connection = async_connections[name]
        connection.set_rollback(True)
        await atomic_cms[name].__aexit__(None, None, None)
        await connection.close()
