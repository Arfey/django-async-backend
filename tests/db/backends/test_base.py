import gc
from unittest import skipIf
from unittest.mock import (
    MagicMock,
    patch,
)

import django
from django.db import DEFAULT_DB_ALIAS
from django.test.utils import override_settings

from django_async_backend.db import async_connections
from django_async_backend.db.backends.base.base import BaseAsyncDatabaseWrapper
from django_async_backend.db.transaction import async_atomic
from django_async_backend.test import (
    AsyncioTestCase,
    AsyncioTransactionTestCase,
)


async def create_table():
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE reporter_table_tmp (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            );
            """
        )


async def drop_table():
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute("DROP TABLE reporter_table_tmp;")

    await async_connections[DEFAULT_DB_ALIAS].close()


async def create_instance(id):
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO reporter_table_tmp (name) VALUES ('{id}');"
        )

        return str(id)


async def get_all():
    async with async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        res = await cursor.execute(
            "SELECT name FROM reporter_table_tmp order by name;"
        )

        return [i[0] for i in await res.fetchall()]


class DatabaseWrapperTests(AsyncioTransactionTestCase):

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    async def test_repr(self):
        conn = async_connections[DEFAULT_DB_ALIAS]
        self.assertEqual(
            repr(conn),
            f"<AsyncDatabaseWrapper vendor={conn.vendor!r} alias='default'>",
        )

    async def test_initialization_class_attributes(self):
        """
        The "initialization" class attributes like client_class and
        creation_class should be set on the class and reflected in the
        corresponding instance attributes of the instantiated backend.
        """
        conn = async_connections[DEFAULT_DB_ALIAS]
        conn_class = type(conn)
        attr_names = [
            ("features_class", "features"),
            ("ops_class", "ops"),
        ]
        for class_attr_name, instance_attr_name in attr_names:
            class_attr_value = getattr(conn_class, class_attr_name)
            self.assertIsNotNone(class_attr_value)
            instance_attr_value = getattr(conn, instance_attr_name)
            self.assertIsInstance(instance_attr_value, class_attr_value)

    async def test_initialization_display_name(self):
        self.assertEqual(BaseAsyncDatabaseWrapper.display_name, "unknown")
        self.assertNotEqual(
            async_connections[DEFAULT_DB_ALIAS].display_name, "unknown"
        )

    async def test_get_database_version(self):
        with patch.object(
            BaseAsyncDatabaseWrapper, "__init__", return_value=None
        ):
            msg = (
                "subclasses of BaseAsyncDatabaseWrapper may require a "
                "get_database_version."
            )
            with self.assertRaisesRegex(NotImplementedError, msg):
                await BaseAsyncDatabaseWrapper().get_database_version()

    async def test_check_database_version_supported_with_none_as_database_version(  # noqa: E501
        self,
    ):
        connection = async_connections[DEFAULT_DB_ALIAS]

        with patch.object(
            connection.features, "minimum_database_version", None
        ):
            await connection.check_database_version_supported()

    @skipIf(django.VERSION < (6, 0), "Requires Django 6.0 or higher")
    async def test_release_memory_without_garbage_collection(self):
        # Schedule the restore of the garbage collection settings.
        connection = async_connections[DEFAULT_DB_ALIAS]
        self.addCleanup(gc.set_debug, 0)
        self.addCleanup(gc.enable)

        # Disable automatic garbage collection to control when it's triggered,
        # then run a full collection cycle to ensure `gc.garbage` is empty.
        gc.disable()
        gc.collect()

        # The garbage list isn't automatically populated to avoid CPU overhead,
        # so debugging needs to be enabled to track all unreachable items and
        # have them stored in `gc.garbage`.
        gc.set_debug(gc.DEBUG_SAVEALL)

        # Create a new connection that will be closed during the test, and also
        # ensure that a `DatabaseErrorWrapper` is created for this connection.
        test_connection = connection.copy()
        with test_connection.wrap_database_errors:
            self.assertEqual(test_connection.queries, [])

        # Close the connection and remove references to it. This will mark all
        # objects related to the connection as garbage to be collected.
        await test_connection.close()
        test_connection = None

        # Enforce garbage collection to populate `gc.garbage` for inspection.
        gc.collect()
        self.assertEqual(gc.garbage, [])


class DatabaseWrapperLoggingTests(AsyncioTransactionTestCase):

    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    @override_settings(DEBUG=True)
    async def test_commit_debug_log(self):
        conn = async_connections[DEFAULT_DB_ALIAS]

        with self.assertLogs("django.db.backends", "DEBUG") as cm:
            async with async_atomic():
                await create_instance(1)

            self.assertGreaterEqual(len(conn.queries_log), 3)
            self.assertEqual(conn.queries_log[0]["sql"], "BEGIN")
            self.assertEqual(
                conn.queries_log[1]["sql"],
                "INSERT INTO reporter_table_tmp (name) VALUES ('1');",
            )
            self.assertEqual(conn.queries_log[2]["sql"], "COMMIT")
            self.assertRegex(
                cm.output[0],
                r"DEBUG:django.db.backends:\(\d+.\d{3}\) "
                rf"BEGIN; args=None; alias={DEFAULT_DB_ALIAS}",
            )
            self.assertRegex(
                cm.output[-1],
                r"DEBUG:django.db.backends:\(\d+.\d{3}\) "
                rf"COMMIT; args=None; alias={DEFAULT_DB_ALIAS}",
            )

    @override_settings(DEBUG=True)
    async def test_rollback_debug_log(self):
        conn = async_connections[DEFAULT_DB_ALIAS]

        with self.assertLogs("django.db.backends", "DEBUG") as cm:
            with self.assertRaises(Exception):
                async with async_atomic():
                    await create_instance(1)
                    raise Exception("Force rollback")

            self.assertEqual(conn.queries_log[-1]["sql"], "ROLLBACK")
            self.assertRegex(
                cm.output[-1],
                r"DEBUG:django.db.backends:\(\d+.\d{3}\) "
                rf"ROLLBACK; args=None; alias={DEFAULT_DB_ALIAS}",
            )

    async def test_no_logs_without_debug(self):
        with self.assertNoLogs("django.db", "DEBUG"):
            with self.assertRaises(Exception):
                async with async_atomic():
                    await create_instance(1)
                    raise Exception("Force rollback")

            conn = async_connections[DEFAULT_DB_ALIAS]
            self.assertEqual(len(conn.queries_log), 0)


class ExecuteWrapperTests(AsyncioTestCase):
    async def asyncSetUp(self):
        await create_table()

    async def asyncTearDown(self):
        await drop_table()

    @staticmethod
    async def call_execute(connection, params=None):
        ret_val = "1" if params is None else "%s"
        sql = "SELECT " + ret_val + connection.features.bare_select_suffix
        async with connection.cursor() as cursor:
            await cursor.execute(sql, params)

    async def call_executemany(self, connection, params=None):
        # executemany() must use an update query. Make sure it does nothing
        # by putting a false condition in the WHERE clause.

        sql = "DELETE FROM reporter_table_tmp WHERE 0=1 AND 0=%s"
        if params is None:
            params = [(i,) for i in range(3)]

        async with connection.cursor() as cursor:
            await cursor.executemany(sql, params)

    @staticmethod
    def mock_wrapper():
        return MagicMock(side_effect=lambda execute, *args: execute(*args))

    async def test_wrapper_invoked(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        wrapper = self.mock_wrapper()
        with connection.execute_wrapper(wrapper):
            await self.call_execute(connection)
        self.assertTrue(wrapper.called)
        (_, sql, params, many, context), _ = wrapper.call_args
        self.assertIn("SELECT", sql)
        self.assertIsNone(params)
        self.assertIs(many, False)
        self.assertEqual(context["connection"], connection)

    async def test_wrapper_invoked_many(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        wrapper = self.mock_wrapper()
        with connection.execute_wrapper(wrapper):
            await self.call_executemany(connection)
        self.assertTrue(wrapper.called)
        (_, sql, param_list, many, context), _ = wrapper.call_args
        self.assertIn("DELETE", sql)
        self.assertIsInstance(param_list, (list, tuple))
        self.assertIs(many, True)
        self.assertEqual(context["connection"], connection)

    async def test_database_queried(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        wrapper = self.mock_wrapper()
        with connection.execute_wrapper(wrapper):
            async with connection.cursor() as cursor:
                sql = "SELECT 17" + connection.features.bare_select_suffix
                await cursor.execute(sql)
                seventeen = await cursor.fetchall()
                self.assertEqual(list(seventeen), [(17,)])
            await self.call_executemany(connection)

    async def test_nested_wrapper_invoked(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        outer_wrapper = self.mock_wrapper()
        inner_wrapper = self.mock_wrapper()
        with (
            connection.execute_wrapper(outer_wrapper),
            connection.execute_wrapper(inner_wrapper),
        ):
            await self.call_execute(connection)
            self.assertEqual(inner_wrapper.call_count, 1)
            await self.call_executemany(connection)
            self.assertEqual(inner_wrapper.call_count, 2)

    async def test_outer_wrapper_blocks(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        def blocker(*args):
            pass

        wrapper = self.mock_wrapper()
        c = connection  # This alias shortens the next line.
        with (
            c.execute_wrapper(wrapper),
            c.execute_wrapper(blocker),
            c.execute_wrapper(wrapper),
        ):
            async with c.cursor() as cursor:
                await cursor.execute("The database never sees this")
                self.assertEqual(wrapper.call_count, 1)
                await cursor.executemany(
                    "The database never sees this %s", [("either",)]
                )
                self.assertEqual(wrapper.call_count, 2)

    async def test_outer_async_wrapper_blocks(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async def blocker(*args):
            pass

        wrapper = self.mock_wrapper()
        c = connection  # This alias shortens the next line.
        with (
            c.execute_wrapper(wrapper),
            c.execute_wrapper(blocker),
            c.execute_wrapper(wrapper),
        ):
            async with c.cursor() as cursor:
                await cursor.execute("The database never sees this")
                self.assertEqual(wrapper.call_count, 1)
                await cursor.executemany(
                    "The database never sees this %s", [("either",)]
                )
                self.assertEqual(wrapper.call_count, 2)

    async def test_wrapper_gets_sql(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        wrapper = self.mock_wrapper()
        sql = "SELECT 'aloha'" + connection.features.bare_select_suffix
        with connection.execute_wrapper(wrapper):
            async with connection.cursor() as cursor:
                await cursor.execute(sql)
        (_, reported_sql, _, _, _), _ = wrapper.call_args
        self.assertEqual(reported_sql, sql)

    async def test_wrapper_connection_specific(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        wrapper = self.mock_wrapper()

        with async_connections["other"].execute_wrapper(wrapper):
            self.assertEqual(
                async_connections["other"].execute_wrappers, [wrapper]
            )
            await self.call_execute(connection)

        self.assertFalse(wrapper.called)
        self.assertEqual(connection.execute_wrappers, [])
        self.assertEqual(async_connections["other"].execute_wrappers, [])

    @override_settings(DEBUG=True)
    async def test_wrapper_debug(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        def wrap_with_comment(execute, sql, params, many, context):
            return execute(f"/* My comment */ {sql}", params, many, context)

        with connection.execute_wrapper(wrap_with_comment):
            await get_all()

        last_query = connection.queries_log[-1]["sql"]
        self.assertTrue(last_query.startswith("/* My comment */"))


class ConnectionHealthChecksTests(AsyncioTransactionTestCase):
    databases = {"default"}

    async def asyncSetUp(self):
        # All test cases here need newly configured and created connections.
        # Use the default db connection for convenience.
        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.close()
        self.addAsyncCleanup(connection.close)

    def patch_settings_dict(self, conn_health_checks):
        connection = async_connections[DEFAULT_DB_ALIAS]

        self.settings_dict_patcher = patch.dict(
            connection.settings_dict,
            {
                **connection.settings_dict,
                "CONN_MAX_AGE": None,
                "CONN_HEALTH_CHECKS": conn_health_checks,
            },
        )
        self.settings_dict_patcher.start()
        self.addCleanup(self.settings_dict_patcher.stop)

    async def run_query(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT 42" + connection.features.bare_select_suffix
            )

    async def test_health_checks_enabled(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        self.patch_settings_dict(conn_health_checks=True)
        self.assertIsNone(connection.connection)
        # Newly created connections are considered healthy without performing
        # the health check.
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await self.run_query()

        old_connection = connection.connection
        # Simulate request_finished.
        await connection.close_if_unusable_or_obsolete()
        self.assertIs(old_connection, connection.connection)

        # Simulate connection health check failing.
        with patch.object(
            connection, "is_usable", return_value=False
        ) as mocked_is_usable:
            await self.run_query()
            new_connection = connection.connection
            # A new connection is established.
            self.assertIsNot(new_connection, old_connection)
            # Only one health check per "request" is performed, so the next
            # query will carry on even if the health check fails. Next query
            # succeeds because the real connection is healthy and only the
            # health check failure is mocked.
            await self.run_query()
            self.assertIs(new_connection, connection.connection)
        self.assertEqual(mocked_is_usable.call_count, 1)

        # Simulate request_finished.
        await connection.close_if_unusable_or_obsolete()
        # The underlying connection is being reused further with health checks
        # succeeding.
        await self.run_query()
        await self.run_query()
        self.assertIs(new_connection, connection.connection)

    async def test_health_checks_enabled_errors_occurred(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        self.patch_settings_dict(conn_health_checks=True)
        self.assertIsNone(connection.connection)
        # Newly created connections are considered healthy without performing
        # the health check.
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await self.run_query()

        old_connection = connection.connection
        # Simulate errors_occurred.
        connection.errors_occurred = True
        # Simulate request_started (the connection is healthy).
        await connection.close_if_unusable_or_obsolete()
        # Persistent connections are enabled.
        self.assertIs(old_connection, connection.connection)
        # No additional health checks after the one in
        # close_if_unusable_or_obsolete() are executed during this "request"
        # when running queries.
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await self.run_query()

    async def test_health_checks_disabled(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        self.patch_settings_dict(conn_health_checks=False)
        self.assertIsNone(connection.connection)
        # Newly created connections are considered healthy without performing
        # the health check.
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await self.run_query()

        old_connection = connection.connection
        # Simulate request_finished.
        await connection.close_if_unusable_or_obsolete()
        # Persistent connections are enabled (connection is not).
        self.assertIs(old_connection, connection.connection)
        # Health checks are not performed.
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            await self.run_query()
            # Health check wasn't performed and the connection is unchanged.
            self.assertIs(old_connection, connection.connection)
            await self.run_query()
            # The connection is unchanged after the next query either during
            # the current "request".
            self.assertIs(old_connection, connection.connection)

    async def test_set_autocommit_health_checks_enabled(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        self.patch_settings_dict(conn_health_checks=True)
        self.assertIsNone(connection.connection)
        # Newly created connections are considered healthy without performing
        # the health check.
        with patch.object(connection, "is_usable", side_effect=AssertionError):
            # Simulate outermost atomic block: changing autocommit for
            # a connection.
            await connection.set_autocommit(False)
            await self.run_query()
            await connection.commit()
            await connection.set_autocommit(True)

        old_connection = connection.connection
        # Simulate request_finished.
        await connection.close_if_unusable_or_obsolete()
        # Persistent connections are enabled.
        self.assertIs(old_connection, connection.connection)

        # Simulate connection health check failing.
        with patch.object(
            connection, "is_usable", return_value=False
        ) as mocked_is_usable:
            # Simulate outermost atomic block: changing autocommit for
            # a connection.
            await connection.set_autocommit(False)
            new_connection = connection.connection
            self.assertIsNot(new_connection, old_connection)
            # Only one health check per "request" is performed, so a query will
            # carry on even if the health check fails. This query succeeds
            # because the real connection is healthy and only the health check
            # failure is mocked.
            await self.run_query()
            await connection.commit()
            await connection.set_autocommit(True)
            # The connection is unchanged.
            self.assertIs(new_connection, connection.connection)
        self.assertEqual(mocked_is_usable.call_count, 1)

        # Simulate request_finished.
        await connection.close_if_unusable_or_obsolete()
        # The underlying connection is being reused further with health checks
        # succeeding.
        await connection.set_autocommit(False)
        await self.run_query()
        await connection.commit()
        await connection.set_autocommit(True)
        self.assertIs(new_connection, connection.connection)


class MultiDatabaseTests(AsyncioTransactionTestCase):
    databases = {"default", "other"}

    async def test_multi_database_init_connection_state_called_once(self):
        for db in self.databases:
            with self.subTest(database=db):
                with patch.object(
                    async_connections[db], "commit", return_value=None
                ):
                    with patch.object(
                        async_connections[db],
                        "check_database_version_supported",
                    ) as mocked_database_version_supported:
                        await async_connections[db].init_connection_state()
                        after_first_calls = len(
                            mocked_database_version_supported.mock_calls
                        )
                        await async_connections[db].init_connection_state()
                        self.assertEqual(
                            len(mocked_database_version_supported.mock_calls),
                            after_first_calls,
                        )
