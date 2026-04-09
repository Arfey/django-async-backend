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
async def async_db():
    """Wraps each test in an async transaction that rolls back on completion.

    This is the pytest equivalent of AsyncioTestCase._init_transaction /
    _close_transaction. Use it for any async test that needs database access.
    """
    from django_async_backend.db import async_connections
    from django_async_backend.db.transaction import async_atomic

    atomic_cms = {}
    for name in async_connections.settings:
        connection = async_connections[name]
        atomic_cms[name] = async_atomic(name)
        await atomic_cms[name].__aenter__()

    yield

    for name in async_connections.settings:
        connection = async_connections[name]
        connection.set_rollback(True)
        await atomic_cms[name].__aexit__(None, None, None)
        await connection.close()
