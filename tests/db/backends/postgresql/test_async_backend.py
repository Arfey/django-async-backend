import copy
from collections import namedtuple
from unittest import mock

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DEFAULT_DB_ALIAS,
    NotSupportedError,
    ProgrammingError,
)
from django.db.backends.postgresql.psycopg_any import errors
from django.test import override_settings

from django_async_backend.db import async_connections
from tests.fixtures.reporter_table import insert_reporter


def _no_pool_connection(alias=None):
    """Copy the default connection and disable pooling on it.

    Used to get a fresh connection for tests that manage lifecycle directly
    without interfering with the pooled default connection.
    """
    connection = async_connections[DEFAULT_DB_ALIAS]
    new_connection = connection.copy(alias)
    new_connection.settings_dict = copy.deepcopy(connection.settings_dict)
    new_connection.settings_dict["OPTIONS"]["pool"] = False
    return new_connection


# ── Connection parameters ─────────────────────────────────────────────


async def test_database_name_too_long():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    connection = async_connections[DEFAULT_DB_ALIAS]
    settings = connection.settings_dict.copy()
    max_name_length = connection.ops.max_name_length()
    settings["NAME"] = "a" + (max_name_length * "a")
    msg = (
        r"The database name '%s' \(%d characters\) is longer than "
        r"PostgreSQL\'s limit of %s characters. Supply a shorter NAME in "
        r"settings.DATABASES."
    ) % (settings["NAME"], max_name_length + 1, max_name_length)
    with pytest.raises(ImproperlyConfigured, match=msg):
        AsyncDatabaseWrapper(settings).get_connection_params()


async def test_database_name_empty():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    connection = async_connections[DEFAULT_DB_ALIAS]
    settings = connection.settings_dict.copy()
    settings["NAME"] = ""
    msg = (
        r"settings.DATABASES is improperly configured. Please supply the "
        r"NAME or OPTIONS\[\'service\'\] value."
    )
    with pytest.raises(ImproperlyConfigured, match=msg):
        AsyncDatabaseWrapper(settings).get_connection_params()


async def test_service_name():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    connection = async_connections[DEFAULT_DB_ALIAS]
    settings = connection.settings_dict.copy()
    settings["OPTIONS"] = {"service": "my_service"}
    settings["NAME"] = ""
    params = AsyncDatabaseWrapper(settings).get_connection_params()
    assert params["service"] == "my_service"
    assert "database" not in params


async def test_service_name_default_db():
    """None is used to connect to the default 'postgres' db."""
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    connection = async_connections[DEFAULT_DB_ALIAS]
    settings = connection.settings_dict.copy()
    settings["NAME"] = None
    settings["OPTIONS"] = {"service": "django_test"}
    params = AsyncDatabaseWrapper(settings).get_connection_params()
    assert params["dbname"] == "postgres"
    assert "service" not in params


# ── Connection lifecycle ──────────────────────────────────────────────


async def test_connect_and_rollback():
    """SET TIME ZONE is not rolled back after rollback (#17062)."""
    new_connection = _no_pool_connection()
    try:
        async with await new_connection.cursor() as cursor:
            await cursor.execute("RESET TIMEZONE")
            await cursor.execute("SHOW TIMEZONE")
            db_default_tz = (await cursor.fetchone())[0]
        new_tz = "Europe/Paris" if db_default_tz == "UTC" else "UTC"
        await new_connection.close()

        del new_connection.timezone_name

        with override_settings(TIME_ZONE=new_tz):
            await new_connection.set_autocommit(False)
            await new_connection.rollback()

            async with await new_connection.cursor() as cursor:
                await cursor.execute("SHOW TIMEZONE")
                tz = (await cursor.fetchone())[0]
            assert new_tz == tz
    finally:
        await new_connection.close()


async def test_connect_non_autocommit():
    """Connection wrapper reports correct autocommit state when AUTOCOMMIT=False (#21452)."""
    new_connection = _no_pool_connection()
    new_connection.settings_dict["AUTOCOMMIT"] = False
    try:
        async with await new_connection.cursor():
            assert not await new_connection.get_autocommit()
    finally:
        await new_connection.close()


# ── Connection pooling ────────────────────────────────────────────────


async def test_connect_pool():
    from psycopg_pool import PoolTimeout

    new_connection = _no_pool_connection(alias="default_pool")
    new_connection.settings_dict["OPTIONS"]["pool"] = {
        "min_size": 0,
        "max_size": 2,
        "timeout": 5,
    }
    assert new_connection.pool is not None

    connections = []

    async def get_connection():
        conn = new_connection.copy()
        await conn.connect()
        connections.append(conn)
        return conn

    try:
        connection_1 = await get_connection()
        connection_1_backend_pid = connection_1.connection.info.backend_pid
        await get_connection()
        with pytest.raises(PoolTimeout):
            await get_connection()

        await connection_1.close()
        connection_3 = await get_connection()
        assert connection_3.connection.info.backend_pid == connection_1_backend_pid
    finally:
        for conn in connections:
            await conn.close()
        await new_connection.close_pool()


async def test_connect_pool_set_to_true():
    new_connection = _no_pool_connection(alias="default_pool")
    new_connection.settings_dict["OPTIONS"]["pool"] = True
    try:
        assert new_connection.pool is not None
    finally:
        await new_connection.close_pool()


async def test_connect_pool_with_timezone():
    new_time_zone = "Africa/Nairobi"
    new_connection = _no_pool_connection(alias="default_pool")

    try:
        async with await new_connection.cursor() as cursor:
            await cursor.execute("SHOW TIMEZONE")
            tz = (await cursor.fetchone())[0]
            assert new_time_zone != tz
    finally:
        await new_connection.close()

    del new_connection.timezone_name
    new_connection.settings_dict["OPTIONS"]["pool"] = True
    try:
        with override_settings(TIME_ZONE=new_time_zone):
            async with await new_connection.cursor() as cursor:
                await cursor.execute("SHOW TIMEZONE")
                tz = (await cursor.fetchone())[0]
                assert new_time_zone == tz
    finally:
        await new_connection.close()
        await new_connection.close_pool()


async def test_pooling_health_checks():
    new_connection = _no_pool_connection(alias="default_pool")
    new_connection.settings_dict["OPTIONS"]["pool"] = True
    new_connection.settings_dict["CONN_HEALTH_CHECKS"] = False
    try:
        assert new_connection.pool._check is None
    finally:
        await new_connection.close_pool()

    new_connection.settings_dict["CONN_HEALTH_CHECKS"] = True
    try:
        assert new_connection.pool._check is not None
    finally:
        await new_connection.close_pool()


async def test_cannot_open_new_connection_in_atomic_block():
    new_connection = _no_pool_connection(alias="default_pool")
    new_connection.settings_dict["OPTIONS"]["pool"] = True
    new_connection.in_atomic_block = True
    new_connection.closed_in_transaction = True
    with pytest.raises(ProgrammingError, match="Cannot open a new connection in an atomic block."):
        await new_connection.ensure_connection()


async def test_pooling_not_support_persistent_connections():
    new_connection = _no_pool_connection(alias="default_pool")
    new_connection.settings_dict["OPTIONS"]["pool"] = True
    new_connection.settings_dict["CONN_MAX_AGE"] = 10
    with pytest.raises(ImproperlyConfigured, match="Pooling doesn't support persistent connections."):
        new_connection.pool


# ── Isolation level ──────────────────────────────────────────────────


async def test_connect_isolation_level(async_db):
    """Transaction level can be configured via OPTIONS['isolation_level']."""
    from django.db.backends.postgresql.psycopg_any import IsolationLevel

    connection = async_connections[DEFAULT_DB_ALIAS]
    assert connection.connection.isolation_level is None

    new_connection = _no_pool_connection()
    new_connection.settings_dict["OPTIONS"]["isolation_level"] = IsolationLevel.SERIALIZABLE
    try:
        await new_connection.set_autocommit(False)
        assert new_connection.connection.isolation_level == IsolationLevel.SERIALIZABLE
    finally:
        await new_connection.close()


async def test_connect_invalid_isolation_level(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    assert connection.connection.isolation_level is None

    new_connection = _no_pool_connection()
    new_connection.settings_dict["OPTIONS"]["isolation_level"] = -1
    msg = "Invalid transaction isolation level -1 specified. Use one of the psycopg.IsolationLevel values."
    with pytest.raises(ImproperlyConfigured, match=msg):
        await new_connection.ensure_connection()


# ── Role and cursor options ──────────────────────────────────────────


async def test_connect_role():
    """Session role can be configured via OPTIONS['assume_role']."""
    custom_role = "django_nonexistent_role"
    new_connection = _no_pool_connection()
    new_connection.settings_dict["OPTIONS"]["assume_role"] = custom_role
    try:
        with pytest.raises(errors.InvalidParameterValue, match=f'role "{custom_role}" does not exist'):
            await new_connection.connect()
    finally:
        await new_connection.close()


async def test_connect_server_side_binding():
    """Server-side parameter binding can be enabled via OPTIONS['server_side_binding']."""
    from django_async_backend.db.backends.postgresql.base import AsyncServerBindingCursor

    new_connection = _no_pool_connection()
    new_connection.settings_dict["OPTIONS"]["server_side_binding"] = True
    try:
        await new_connection.connect()
        assert new_connection.connection.cursor_factory == AsyncServerBindingCursor
    finally:
        await new_connection.close()


async def test_connect_custom_cursor_factory():
    """Custom cursor factory can be configured via OPTIONS['cursor_factory']."""
    from django_async_backend.db.backends.postgresql.base import AsyncCursor

    class MyCursor(AsyncCursor):
        pass

    new_connection = _no_pool_connection()
    new_connection.settings_dict["OPTIONS"]["cursor_factory"] = MyCursor
    try:
        await new_connection.connect()
        assert new_connection.connection.cursor_factory == MyCursor
    finally:
        await new_connection.close()


async def test_connect_no_is_usable_checks():
    new_connection = _no_pool_connection()
    try:
        with mock.patch.object(new_connection, "is_usable") as is_usable:
            await new_connection.connect()
        is_usable.assert_not_called()
    finally:
        await new_connection.close()


async def test_client_encoding_utf8_enforce():
    new_connection = _no_pool_connection()
    new_connection.settings_dict["OPTIONS"]["client_encoding"] = "iso-8859-2"
    try:
        await new_connection.connect()
        assert new_connection.connection.info.encoding == "utf-8"
    finally:
        await new_connection.close()


# ── Type handling ────────────────────────────────────────────────────


async def _select_val(val):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with await connection.cursor() as cursor:
        await cursor.execute("SELECT %s::text[]", (val,))
        return (await cursor.fetchone())[0]


async def test_select_ascii_array(async_db):
    a = ["awef"]
    b = await _select_val(a)
    assert a[0] == b[0]


async def test_select_unicode_array(async_db):
    a = ["ᄲawef"]
    b = await _select_val(a)
    assert a[0] == b[0]


# ── Operations and utilities ─────────────────────────────────────────


@pytest.mark.parametrize(
    "lookup",
    ["iexact", "contains", "icontains", "startswith", "istartswith", "endswith", "iendswith", "regex", "iregex"],
)
async def test_lookup_cast(lookup):
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseOperations

    do = AsyncDatabaseOperations(connection=None)
    assert "::text" in do.lookup_cast(lookup)


@pytest.mark.parametrize("field_type", ["CharField", "EmailField", "TextField"])
async def test_lookup_cast_isnull_noop(field_type):
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseOperations

    do = AsyncDatabaseOperations(connection=None)
    assert do.lookup_cast("isnull", field_type) == "%s"


async def test_correct_extraction_psycopg_version():
    from django_async_backend.db.backends.postgresql.base import Database, psycopg_version

    with mock.patch.object(Database, "__version__", "4.2.1 (dt dec pq3 ext lo64)"):
        assert psycopg_version() == (4, 2, 1)
    with mock.patch.object(Database, "__version__", "4.2b0.dev1 (dt dec pq3 ext lo64)"):
        assert psycopg_version() == (4, 2)


@override_settings(DEBUG=True)
async def test_copy_cursors(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    await insert_reporter(1)
    await insert_reporter(2)

    copy_sql = "COPY reporter_table_tmp TO STDOUT (FORMAT CSV, HEADER)"
    async with await connection.cursor() as cursor:
        async for row in cursor.copy(copy_sql):
            pass

    assert connection.queries[-1]["sql"] == copy_sql


async def test_get_database_version():
    new_connection = _no_pool_connection()
    version = await new_connection.get_database_version()
    assert len(version) == 2
    assert (await new_connection.get_database_version())[0] == 15


async def test_check_database_version_supported():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    class CustomAsyncDatabaseWrapper(AsyncDatabaseWrapper):
        async def get_database_version(self):
            return (13,)

    new_connection = _no_pool_connection()
    try:
        await new_connection.connect()
        settings = new_connection.settings_dict.copy()
        with pytest.raises(NotSupportedError, match=r"PostgreSQL 14 or later is required \(found 13\)."):
            await CustomAsyncDatabaseWrapper(settings).check_database_version_supported()
    finally:
        await new_connection.close()


async def test_compose_sql_when_no_connection():
    new_connection = _no_pool_connection()
    try:
        result = await new_connection.ops.compose_sql("SELECT %s", ["test"])
        assert result == "SELECT 'test'"
    finally:
        await new_connection.close()


async def _run_timezone_configuration(Wrapper, expected_commit):
    new_connection = _no_pool_connection()
    try:
        async with await new_connection.cursor() as cursor:
            await cursor.execute("RESET TIMEZONE")
            await cursor.execute("SHOW TIMEZONE")
            db_default_tz = (await cursor.fetchone())[0]
        new_tz = "Europe/Paris" if db_default_tz == "UTC" else "UTC"
        new_connection.timezone_name = new_tz

        settings = new_connection.settings_dict.copy()
        conn = new_connection.connection
        result = await Wrapper(settings)._configure_connection(conn)
        assert result is expected_commit
    finally:
        await new_connection.close()


async def test_configure_timezone_commits_when_changed():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    await _run_timezone_configuration(AsyncDatabaseWrapper, expected_commit=True)


async def test_bypass_timezone_configuration_returns_false():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    class CustomAsyncDatabaseWrapper(AsyncDatabaseWrapper):
        async def _configure_timezone(self, connection):
            return False

    await _run_timezone_configuration(CustomAsyncDatabaseWrapper, expected_commit=False)


async def test_bypass_role_configuration():
    from django_async_backend.db.backends.postgresql.base import AsyncDatabaseWrapper

    class CustomAsyncDatabaseWrapper(AsyncDatabaseWrapper):
        async def _configure_role(self, connection):
            return False

    new_connection = _no_pool_connection()
    try:
        await new_connection.connect()
        settings = new_connection.settings_dict.copy()
        settings["OPTIONS"]["assume_role"] = "django_nonexistent_role"
        conn = new_connection.connection
        result = await CustomAsyncDatabaseWrapper(settings)._configure_connection(conn)
        assert result is False
    finally:
        await new_connection.close()


# ── Server-side cursors ──────────────────────────────────────────────


_CURSOR_FIELDS = "name, statement, is_holdable, is_binary, is_scrollable, creation_time"
_PostgresCursor = namedtuple("_PostgresCursor", _CURSOR_FIELDS)


@pytest.fixture
async def reporter_table_with_data(async_db):
    """Use async_db-wrapped table so server-side cursors report is_holdable=False
    (cursors are not WITH HOLD inside an active transaction)."""
    await insert_reporter(0)
    await insert_reporter(1)


async def _inspect_cursors():
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with await connection.cursor() as cursor:
        await cursor.execute(f"SELECT {_CURSOR_FIELDS} FROM pg_cursors;")
        cursors = await cursor.fetchall()
    return [_PostgresCursor._make(c) for c in cursors]


async def _assert_uses_cursor(query, num_expected=1):
    connection = async_connections[DEFAULT_DB_ALIAS]
    cursor_name = "my_server_side_cursor"
    async with connection.create_cursor(name=cursor_name) as cursor:
        await cursor.execute(query)
        await cursor.fetchone()
        cursors = await _inspect_cursors()
        assert len(cursors) == num_expected
        assert cursor_name in cursors[0].name
        assert not cursors[0].is_scrollable
        assert not cursors[0].is_holdable
        assert not cursors[0].is_binary


async def test_server_side_cursor(reporter_table_with_data):
    await _assert_uses_cursor("SELECT * FROM reporter_table_tmp")


async def test_server_side_cursor_many_cursors(reporter_table_with_data):
    connection = async_connections[DEFAULT_DB_ALIAS]
    query = "SELECT * FROM reporter_table_tmp"
    async with connection.create_cursor(name="my_server_side_cursor2") as cursor:
        await cursor.execute(query)
        await cursor.fetchone()
        await _assert_uses_cursor(query, num_expected=2)
