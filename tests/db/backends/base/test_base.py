import gc
import logging
from unittest.mock import MagicMock, patch

import pytest
from django.db import DEFAULT_DB_ALIAS
from django.test.utils import override_settings

from django_async_backend.db import async_connections
from django_async_backend.db.backends.base.base import BaseAsyncDatabaseWrapper
from django_async_backend.db.transaction import async_atomic
from tests.fixtures.reporter_table import fetch_all_reporters, insert_reporter

# ── DatabaseWrapper ────────────────────────────────────────────────────


async def test_repr(async_db):
    conn = async_connections[DEFAULT_DB_ALIAS]
    assert repr(conn) == f"<AsyncDatabaseWrapper vendor={conn.vendor!r} alias='default'>"


async def test_initialization_class_attributes(async_db):
    """Class attributes like features_class and ops_class should be reflected
    as instance attributes of the instantiated backend."""
    conn = async_connections[DEFAULT_DB_ALIAS]
    conn_class = type(conn)
    for class_attr_name, instance_attr_name in [
        ("features_class", "features"),
        ("ops_class", "ops"),
    ]:
        class_attr_value = getattr(conn_class, class_attr_name)
        assert class_attr_value is not None
        instance_attr_value = getattr(conn, instance_attr_name)
        assert isinstance(instance_attr_value, class_attr_value)


async def test_initialization_display_name(async_db):
    assert BaseAsyncDatabaseWrapper.display_name == "unknown"
    assert async_connections[DEFAULT_DB_ALIAS].display_name != "unknown"


async def test_get_database_version_not_implemented():
    with patch.object(BaseAsyncDatabaseWrapper, "__init__", return_value=None):
        msg = "subclasses of BaseAsyncDatabaseWrapper may require a get_database_version."
        with pytest.raises(NotImplementedError, match=msg):
            await BaseAsyncDatabaseWrapper().get_database_version()


async def test_check_database_version_supported_with_none_as_database_version(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    with patch.object(connection.features, "minimum_database_version", None):
        await connection.check_database_version_supported()


async def test_release_memory_without_garbage_collection(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    try:
        gc.disable()
        gc.collect()
        gc.set_debug(gc.DEBUG_SAVEALL)

        test_connection = connection.copy()
        with test_connection.wrap_database_errors:
            assert test_connection.queries == []

        await test_connection.close()
        test_connection = None

        gc.collect()
        assert gc.garbage == []
    finally:
        gc.set_debug(0)
        gc.enable()


# ── Logging ────────────────────────────────────────────────────────────


@override_settings(DEBUG=True)
async def test_commit_debug_log(reporter_table_transaction, caplog):
    conn = async_connections[DEFAULT_DB_ALIAS]
    with caplog.at_level(logging.DEBUG, logger="django.db.backends"):
        async with async_atomic():
            await insert_reporter(1)

    assert len(conn.queries_log) >= 3
    assert conn.queries_log[0]["sql"] == "BEGIN"
    assert "INSERT INTO" in conn.queries_log[1]["sql"]
    assert "reporter_table_tmp" in conn.queries_log[1]["sql"]
    assert conn.queries_log[2]["sql"] == "COMMIT"

    log_messages = [r.getMessage() for r in caplog.records if r.name == "django.db.backends"]
    assert any("BEGIN" in m and f"alias={DEFAULT_DB_ALIAS}" in m for m in log_messages)
    assert any("COMMIT" in m and f"alias={DEFAULT_DB_ALIAS}" in m for m in log_messages)


@override_settings(DEBUG=True)
async def test_rollback_debug_log(reporter_table_transaction, caplog):
    conn = async_connections[DEFAULT_DB_ALIAS]
    with caplog.at_level(logging.DEBUG, logger="django.db.backends"):
        with pytest.raises(Exception, match="Force rollback"):
            async with async_atomic():
                await insert_reporter(1)
                raise Exception("Force rollback")

    assert conn.queries_log[-1]["sql"] == "ROLLBACK"
    log_messages = [r.getMessage() for r in caplog.records if r.name == "django.db.backends"]
    assert any("ROLLBACK" in m and f"alias={DEFAULT_DB_ALIAS}" in m for m in log_messages)


async def test_no_logs_without_debug(reporter_table_transaction, caplog):
    with caplog.at_level(logging.DEBUG, logger="django.db"):
        with pytest.raises(Exception, match="Force rollback"):
            async with async_atomic():
                await insert_reporter(1)
                raise Exception("Force rollback")

    conn = async_connections[DEFAULT_DB_ALIAS]
    assert len(conn.queries_log) == 0
    assert not [r for r in caplog.records if r.name == "django.db"]


# ── Execute Wrappers ───────────────────────────────────────────────────


def _mock_wrapper():
    return MagicMock(side_effect=lambda execute, *args: execute(*args))


async def _call_execute(connection, params=None):
    ret_val = "1" if params is None else "%s"
    sql = "SELECT " + ret_val + connection.features.bare_select_suffix
    async with await connection.cursor() as cursor:
        await cursor.execute(sql, params)


async def _call_executemany(connection, params=None):
    sql = "DELETE FROM reporter_table_tmp WHERE 0=1 AND 0=%s"
    if params is None:
        params = [(i,) for i in range(3)]
    async with await connection.cursor() as cursor:
        await cursor.executemany(sql, params)


async def test_wrapper_invoked(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    wrapper = _mock_wrapper()
    with connection.execute_wrapper(wrapper):
        await _call_execute(connection)
    assert wrapper.called
    (_, sql, params, many, context), _ = wrapper.call_args
    assert "SELECT" in sql
    assert params is None
    assert many is False
    assert context["connection"] == connection


async def test_wrapper_invoked_many(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    wrapper = _mock_wrapper()
    with connection.execute_wrapper(wrapper):
        await _call_executemany(connection)
    assert wrapper.called
    (_, sql, param_list, many, context), _ = wrapper.call_args
    assert "DELETE" in sql
    assert isinstance(param_list, (list, tuple))
    assert many is True
    assert context["connection"] == connection


async def test_database_queried(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    wrapper = _mock_wrapper()
    with connection.execute_wrapper(wrapper):
        async with await connection.cursor() as cursor:
            sql = "SELECT 17" + connection.features.bare_select_suffix
            await cursor.execute(sql)
            seventeen = await cursor.fetchall()
            assert list(seventeen) == [(17,)]
        await _call_executemany(connection)


async def test_nested_wrapper_invoked(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    outer_wrapper = _mock_wrapper()
    inner_wrapper = _mock_wrapper()
    with (
        connection.execute_wrapper(outer_wrapper),
        connection.execute_wrapper(inner_wrapper),
    ):
        await _call_execute(connection)
        assert inner_wrapper.call_count == 1
        await _call_executemany(connection)
        assert inner_wrapper.call_count == 2


async def test_outer_wrapper_blocks(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]

    def blocker(*args):
        pass

    wrapper = _mock_wrapper()
    with (
        connection.execute_wrapper(wrapper),
        connection.execute_wrapper(blocker),
        connection.execute_wrapper(wrapper),
    ):
        async with await connection.cursor() as cursor:
            await cursor.execute("The database never sees this")
            assert wrapper.call_count == 1
            await cursor.executemany("The database never sees this %s", [("either",)])
            assert wrapper.call_count == 2


async def test_outer_async_wrapper_blocks(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async def blocker(*args):
        pass

    wrapper = _mock_wrapper()
    with (
        connection.execute_wrapper(wrapper),
        connection.execute_wrapper(blocker),
        connection.execute_wrapper(wrapper),
    ):
        async with await connection.cursor() as cursor:
            await cursor.execute("The database never sees this")
            assert wrapper.call_count == 1
            await cursor.executemany("The database never sees this %s", [("either",)])
            assert wrapper.call_count == 2


async def test_wrapper_gets_sql(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    wrapper = _mock_wrapper()
    sql = "SELECT 'aloha'" + connection.features.bare_select_suffix
    with connection.execute_wrapper(wrapper):
        async with await connection.cursor() as cursor:
            await cursor.execute(sql)
    (_, reported_sql, _, _, _), _ = wrapper.call_args
    assert reported_sql == sql


async def test_wrapper_connection_specific(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]
    wrapper = _mock_wrapper()

    with async_connections["other"].execute_wrapper(wrapper):
        assert async_connections["other"].execute_wrappers == [wrapper]
        await _call_execute(connection)

    assert not wrapper.called
    assert connection.execute_wrappers == []
    assert async_connections["other"].execute_wrappers == []


@override_settings(DEBUG=True)
async def test_wrapper_debug(async_db):
    connection = async_connections[DEFAULT_DB_ALIAS]

    def wrap_with_comment(execute, sql, params, many, context):
        return execute(f"/* My comment */ {sql}", params, many, context)

    with connection.execute_wrapper(wrap_with_comment):
        await fetch_all_reporters()

    last_query = connection.queries_log[-1]["sql"]
    assert last_query.startswith("/* My comment */")


# ── Connection Health Checks ───────────────────────────────────────────


@pytest.fixture
async def closed_default_connection():
    """Health check tests need a clean closed connection without the
    async_db transaction wrapper, since they assert connection.connection is None."""
    connection = async_connections[DEFAULT_DB_ALIAS]
    await connection.close()
    yield connection
    await connection.close()


def _patch_settings_dict(connection, conn_health_checks):
    patcher = patch.dict(
        connection.settings_dict,
        {
            **connection.settings_dict,
            "CONN_MAX_AGE": None,
            "CONN_HEALTH_CHECKS": conn_health_checks,
        },
    )
    patcher.start()
    return patcher


async def _run_query(connection):
    async with await connection.cursor() as cursor:
        await cursor.execute("SELECT 42" + connection.features.bare_select_suffix)


async def test_health_checks_enabled(closed_default_connection):
    connection = closed_default_connection
    patcher = _patch_settings_dict(connection, conn_health_checks=True)
    try:
        assert connection.connection is None
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await _run_query(connection)

        old_connection = connection.connection
        await connection.close_if_unusable_or_obsolete()
        assert old_connection is connection.connection

        with patch.object(connection, "is_usable", return_value=False) as mocked_is_usable:
            await _run_query(connection)
            new_connection = connection.connection
            assert new_connection is not old_connection
            await _run_query(connection)
            assert new_connection is connection.connection
        assert mocked_is_usable.call_count == 1

        await connection.close_if_unusable_or_obsolete()
        await _run_query(connection)
        await _run_query(connection)
        assert new_connection is connection.connection
    finally:
        patcher.stop()


async def test_health_checks_enabled_errors_occurred(closed_default_connection):
    connection = closed_default_connection
    patcher = _patch_settings_dict(connection, conn_health_checks=True)
    try:
        assert connection.connection is None
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await _run_query(connection)

        old_connection = connection.connection
        connection.errors_occurred = True
        await connection.close_if_unusable_or_obsolete()
        assert old_connection is connection.connection

        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await _run_query(connection)
    finally:
        patcher.stop()


async def test_health_checks_disabled(closed_default_connection):
    connection = closed_default_connection
    patcher = _patch_settings_dict(connection, conn_health_checks=False)
    try:
        assert connection.connection is None
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await _run_query(connection)

        old_connection = connection.connection
        await connection.close_if_unusable_or_obsolete()
        assert old_connection is connection.connection

        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await _run_query(connection)
            assert old_connection is connection.connection
            await _run_query(connection)
            assert old_connection is connection.connection
    finally:
        patcher.stop()


async def test_set_autocommit_health_checks_enabled(closed_default_connection):
    connection = closed_default_connection
    patcher = _patch_settings_dict(connection, conn_health_checks=True)
    try:
        assert connection.connection is None
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await connection.set_autocommit(False)
            await _run_query(connection)
            await connection.commit()
            await connection.set_autocommit(True)

        old_connection = connection.connection
        await connection.close_if_unusable_or_obsolete()
        assert old_connection is connection.connection

        with patch.object(connection, "is_usable", return_value=False) as mocked_is_usable:
            await connection.set_autocommit(False)
            new_connection = connection.connection
            assert new_connection is not old_connection
            await _run_query(connection)
            await connection.commit()
            await connection.set_autocommit(True)
            assert new_connection is connection.connection
        assert mocked_is_usable.call_count == 1

        await connection.close_if_unusable_or_obsolete()
        await connection.set_autocommit(False)
        await _run_query(connection)
        await connection.commit()
        await connection.set_autocommit(True)
        assert new_connection is connection.connection
    finally:
        patcher.stop()


# ── Multi-database ─────────────────────────────────────────────────────


@pytest.mark.parametrize("db", ["default", "other"])
async def test_multi_database_init_connection_state_called_once(async_db, db):
    with patch.object(async_connections[db], "commit", return_value=None):
        with patch.object(
            async_connections[db],
            "check_database_version_supported",
        ) as mocked_version_check:
            await async_connections[db].init_connection_state()
            after_first_calls = len(mocked_version_check.mock_calls)
            await async_connections[db].init_connection_state()
            assert len(mocked_version_check.mock_calls) == after_first_calls
