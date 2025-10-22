import copy
from collections import namedtuple
from unittest import mock

from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DEFAULT_DB_ALIAS,
    NotSupportedError,
    ProgrammingError,
)
from django.db.backends.postgresql.psycopg_any import errors
from django.test import override_settings

from django_async_backend.db import async_connections
from django_async_backend.test import AsyncioTestCase


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


def no_pool_connection(alias=None):
    connection = async_connections[DEFAULT_DB_ALIAS]
    new_connection = connection.copy(alias)
    new_connection.settings_dict = copy.deepcopy(connection.settings_dict)
    # Ensure that the second connection circumvents the pool, this is kind
    # of a hack, but we cannot easily change the pool connections.
    new_connection.settings_dict["OPTIONS"]["pool"] = False
    return new_connection


class Tests(AsyncioTestCase):
    databases = {"default", "other"}

    async def test_database_name_too_long(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        connection = async_connections[DEFAULT_DB_ALIAS]

        settings = connection.settings_dict.copy()
        max_name_length = connection.ops.max_name_length()
        settings["NAME"] = "a" + (max_name_length * "a")
        msg = (
            r"The database name '%s' \(%d characters\) is longer than "
            r"PostgreSQL\'s limit of %s characters. Supply a shorter NAME in "
            "settings.DATABASES."
        ) % (settings["NAME"], max_name_length + 1, max_name_length)
        with self.assertRaisesRegex(ImproperlyConfigured, msg):
            AsyncDatabaseWrapper(settings).get_connection_params()

    async def test_database_name_empty(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        connection = async_connections[DEFAULT_DB_ALIAS]

        settings = connection.settings_dict.copy()
        settings["NAME"] = ""
        msg = (
            "settings.DATABASES is improperly configured. Please supply the "
            r"NAME or OPTIONS\[\'service\'\] value."
        )
        with self.assertRaisesRegex(ImproperlyConfigured, msg):
            AsyncDatabaseWrapper(settings).get_connection_params()

    async def test_service_name(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        connection = async_connections[DEFAULT_DB_ALIAS]

        settings = connection.settings_dict.copy()
        settings["OPTIONS"] = {"service": "my_service"}
        settings["NAME"] = ""
        params = AsyncDatabaseWrapper(settings).get_connection_params()
        self.assertEqual(params["service"], "my_service")
        self.assertNotIn("database", params)

    async def test_service_name_default_db(self):
        # None is used to connect to the default 'postgres' db.
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        connection = async_connections[DEFAULT_DB_ALIAS]

        settings = connection.settings_dict.copy()
        settings["NAME"] = None
        settings["OPTIONS"] = {"service": "django_test"}
        params = AsyncDatabaseWrapper(settings).get_connection_params()
        self.assertEqual(params["dbname"], "postgres")
        self.assertNotIn("service", params)

    async def test_connect_and_rollback(self):
        """
        PostgreSQL shouldn't roll back SET TIME ZONE, even if the first
        transaction is rolled back (#17062).
        """
        new_connection = no_pool_connection()
        try:
            # Ensure the database default time zone is different than
            # the time zone in new_connection.settings_dict. We can
            # get the default time zone by reset & show.
            async with new_connection.cursor() as cursor:
                await cursor.execute("RESET TIMEZONE")
                await cursor.execute("SHOW TIMEZONE")
                db_default_tz = (await cursor.fetchone())[0]
            new_tz = "Europe/Paris" if db_default_tz == "UTC" else "UTC"
            await new_connection.close()

            # Invalidate timezone name cache, because the setting_changed
            # handler cannot know about new_connection.
            del new_connection.timezone_name

            # Fetch a new connection with the new_tz as default
            # time zone, run a query and rollback.
            with override_settings(TIME_ZONE=new_tz):
                await new_connection.set_autocommit(False)
                await new_connection.rollback()

                # Now let's see if the rollback rolled back the SET TIME ZONE.
                async with new_connection.cursor() as cursor:
                    await cursor.execute("SHOW TIMEZONE")
                    tz = (await cursor.fetchone())[0]
                self.assertEqual(new_tz, tz)

        finally:
            await new_connection.close()

    async def test_connect_non_autocommit(self):
        """
        The connection wrapper shouldn't believe that autocommit is enabled
        after setting the time zone when AUTOCOMMIT is False (#21452).
        """
        new_connection = no_pool_connection()
        new_connection.settings_dict["AUTOCOMMIT"] = False

        try:
            # Open a database connection.
            async with new_connection.cursor():
                self.assertFalse(await new_connection.get_autocommit())
        finally:
            await new_connection.close()

    async def test_connect_pool(self):
        from psycopg_pool import PoolTimeout

        new_connection = no_pool_connection(alias="default_pool")
        new_connection.settings_dict["OPTIONS"]["pool"] = {
            "min_size": 0,
            "max_size": 2,
            "timeout": 5,
        }
        self.assertIsNotNone(new_connection.pool)

        connections = []

        async def get_connection():
            # copy() reuses the existing alias and as such the same pool.
            conn = new_connection.copy()
            await conn.connect()
            connections.append(conn)
            return conn

        try:
            connection_1 = await get_connection()  # First connection.
            connection_1_backend_pid = connection_1.connection.info.backend_pid
            await get_connection()  # Get the second connection.
            with self.assertRaises(PoolTimeout):
                # The pool has a maximum of 2 connections.
                await get_connection()

            await connection_1.close()  # Release back to the pool.
            connection_3 = await get_connection()
            # Reuses the first connection as it is available.
            self.assertEqual(
                connection_3.connection.info.backend_pid,
                connection_1_backend_pid,
            )
        finally:
            # Release all connections back to the pool.
            for conn in connections:
                await conn.close()
            await new_connection.close_pool()

    async def test_connect_pool_set_to_true(self):
        new_connection = no_pool_connection(alias="default_pool")
        new_connection.settings_dict["OPTIONS"]["pool"] = True
        try:
            self.assertIsNotNone(new_connection.pool)
        finally:
            await new_connection.close_pool()

    async def test_connect_pool_with_timezone(self):
        new_time_zone = "Africa/Nairobi"
        new_connection = no_pool_connection(alias="default_pool")

        try:
            async with new_connection.cursor() as cursor:
                await cursor.execute("SHOW TIMEZONE")
                tz = (await cursor.fetchone())[0]
                self.assertNotEqual(new_time_zone, tz)
        finally:
            await new_connection.close()

        del new_connection.timezone_name
        new_connection.settings_dict["OPTIONS"]["pool"] = True
        try:
            with override_settings(TIME_ZONE=new_time_zone):
                async with new_connection.cursor() as cursor:
                    await cursor.execute("SHOW TIMEZONE")
                    tz = (await cursor.fetchone())[0]
                    self.assertEqual(new_time_zone, tz)
        finally:
            await new_connection.close()
            await new_connection.close_pool()

    async def test_pooling_health_checks(self):
        new_connection = no_pool_connection(alias="default_pool")
        new_connection.settings_dict["OPTIONS"]["pool"] = True
        new_connection.settings_dict["CONN_HEALTH_CHECKS"] = False

        try:
            self.assertIsNone(new_connection.pool._check)
        finally:
            await new_connection.close_pool()

        new_connection.settings_dict["CONN_HEALTH_CHECKS"] = True
        try:
            self.assertIsNotNone(new_connection.pool._check)
        finally:
            await new_connection.close_pool()

    async def test_cannot_open_new_connection_in_atomic_block(self):
        new_connection = no_pool_connection(alias="default_pool")
        new_connection.settings_dict["OPTIONS"]["pool"] = True

        msg = "Cannot open a new connection in an atomic block."
        new_connection.in_atomic_block = True
        new_connection.closed_in_transaction = True
        with self.assertRaisesRegex(ProgrammingError, msg):
            await new_connection.ensure_connection()

    async def test_pooling_not_support_persistent_connections(self):
        new_connection = no_pool_connection(alias="default_pool")
        new_connection.settings_dict["OPTIONS"]["pool"] = True
        new_connection.settings_dict["CONN_MAX_AGE"] = 10
        msg = "Pooling doesn't support persistent connections."
        with self.assertRaisesRegex(ImproperlyConfigured, msg):
            new_connection.pool

    async def test_connect_isolation_level(self):
        """
        The transaction level can be configured with
        DATABASES ['OPTIONS']['isolation_level'].
        """
        from django.db.backends.postgresql.psycopg_any import IsolationLevel

        connection = async_connections[DEFAULT_DB_ALIAS]

        # Since this is a django.test.TestCase, a transaction is in progress
        # and the isolation level isn't reported as 0. This test assumes that
        # PostgreSQL is configured with the default isolation level.
        # Check the level on the psycopg connection, not the Django wrapper.
        self.assertIsNone(connection.connection.isolation_level)

        new_connection = no_pool_connection()
        new_connection.settings_dict["OPTIONS"][
            "isolation_level"
        ] = IsolationLevel.SERIALIZABLE
        try:
            # Start a transaction so the isolation level isn't reported as 0.
            await new_connection.set_autocommit(False)
            # Check the level on the psycopg connection, not the Django
            # wrapper.
            self.assertEqual(
                new_connection.connection.isolation_level,
                IsolationLevel.SERIALIZABLE,
            )
        finally:
            await new_connection.close()

    async def test_connect_invalid_isolation_level(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        self.assertIsNone(connection.connection.isolation_level)
        new_connection = no_pool_connection()
        new_connection.settings_dict["OPTIONS"]["isolation_level"] = -1
        msg = (
            "Invalid transaction isolation level -1 specified. Use one of the "
            "psycopg.IsolationLevel values."
        )
        with self.assertRaisesRegex(ImproperlyConfigured, msg):
            await new_connection.ensure_connection()

    async def test_connect_role(self):
        """
        The session role can be configured with DATABASES
        ["OPTIONS"]["assume_role"].
        """
        try:
            custom_role = "django_nonexistent_role"
            new_connection = no_pool_connection()
            new_connection.settings_dict["OPTIONS"][
                "assume_role"
            ] = custom_role
            msg = f'role "{custom_role}" does not exist'

            with self.assertRaisesRegex(errors.InvalidParameterValue, msg):
                await new_connection.connect()
        finally:
            await new_connection.close()

    async def test_connect_server_side_binding(self):
        """
        The server-side parameters binding role can be enabled with DATABASES
        ["OPTIONS"]["server_side_binding"].
        """
        from django_async_backend.db.backends.postgresql.base import (
            AsyncServerBindingCursor,
        )

        new_connection = no_pool_connection()
        new_connection.settings_dict["OPTIONS"]["server_side_binding"] = True
        try:
            await new_connection.connect()
            self.assertEqual(
                new_connection.connection.cursor_factory,
                AsyncServerBindingCursor,
            )
        finally:
            await new_connection.close()

    async def test_connect_custom_cursor_factory(self):
        """
        A custom cursor factory can be configured with DATABASES["options"]
        ["cursor_factory"].
        """
        from django_async_backend.db.backends.postgresql.base import (
            AsyncCursor,
        )

        class MyCursor(AsyncCursor):
            pass

        new_connection = no_pool_connection()
        new_connection.settings_dict["OPTIONS"]["cursor_factory"] = MyCursor
        try:
            await new_connection.connect()
            self.assertEqual(
                new_connection.connection.cursor_factory, MyCursor
            )
        finally:
            await new_connection.close()

    async def test_connect_no_is_usable_checks(self):
        new_connection = no_pool_connection()
        try:
            with mock.patch.object(new_connection, "is_usable") as is_usable:
                await new_connection.connect()
            is_usable.assert_not_called()
        finally:
            await new_connection.close()

    async def test_client_encoding_utf8_enforce(self):
        new_connection = no_pool_connection()
        new_connection.settings_dict["OPTIONS"][
            "client_encoding"
        ] = "iso-8859-2"
        try:
            await new_connection.connect()
            self.assertEqual(new_connection.connection.info.encoding, "utf-8")
        finally:
            await new_connection.close()

    async def _select(self, val):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with connection.cursor() as cursor:
            await cursor.execute("SELECT %s::text[]", (val,))
            return (await cursor.fetchone())[0]

    async def test_select_ascii_array(self):
        a = ["awef"]
        b = await self._select(a)
        self.assertEqual(a[0], b[0])

    async def test_select_unicode_array(self):
        a = ["ᄲawef"]
        b = await self._select(a)
        self.assertEqual(a[0], b[0])

    async def test_lookup_cast(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseOperations,
        )

        do = AsyncDatabaseOperations(connection=None)
        lookups = (
            "iexact",
            "contains",
            "icontains",
            "startswith",
            "istartswith",
            "endswith",
            "iendswith",
            "regex",
            "iregex",
        )
        for lookup in lookups:
            with self.subTest(lookup=lookup):
                self.assertIn("::text", do.lookup_cast(lookup))

    async def test_lookup_cast_isnull_noop(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseOperations,
        )

        do = AsyncDatabaseOperations(connection=None)
        # Using __isnull lookup doesn't require casting.
        tests = [
            "CharField",
            "EmailField",
            "TextField",
        ]
        for field_type in tests:
            with self.subTest(field_type=field_type):
                self.assertEqual(do.lookup_cast("isnull", field_type), "%s")

    async def test_correct_extraction_psycopg_version(self):
        from django_async_backend.db.backends.postgresql.base import (
            Database,
            psycopg_version,
        )

        with mock.patch.object(
            Database, "__version__", "4.2.1 (dt dec pq3 ext lo64)"
        ):
            self.assertEqual(psycopg_version(), (4, 2, 1))
        with mock.patch.object(
            Database, "__version__", "4.2b0.dev1 (dt dec pq3 ext lo64)"
        ):
            self.assertEqual(psycopg_version(), (4, 2))

    @override_settings(DEBUG=True)
    async def test_copy_cursors(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        await create_table()

        try:
            await create_instance(1)
            await create_instance(2)

            copy_sql = "COPY reporter_table_tmp TO STDOUT (FORMAT CSV, HEADER)"

            async with connection.cursor() as cursor:
                async for row in cursor.copy(copy_sql):
                    pass

            self.assertEqual(connection.queries[-1]["sql"], copy_sql)
        finally:
            await drop_table()

    async def test_get_database_version(self):
        new_connection = no_pool_connection()
        version = await new_connection.get_database_version()
        self.assertTrue(len(version) == 2)
        self.assertEqual((await new_connection.get_database_version())[0], 15)

    async def test_check_database_version_supported(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        class CustomAsyncDatabaseWrapper(AsyncDatabaseWrapper):
            async def get_database_version(self):
                return (13,)

        new_connection = no_pool_connection()
        self.addCleanup(new_connection.close)
        await new_connection.connect()
        settings = new_connection.settings_dict.copy()

        msg = r"PostgreSQL 14 or later is required \(found 13\)."
        with self.assertRaisesRegex(NotSupportedError, msg):
            await CustomAsyncDatabaseWrapper(
                settings
            ).check_database_version_supported()

    async def test_compose_sql_when_no_connection(self):
        new_connection = no_pool_connection()
        try:
            self.assertEqual(
                await new_connection.ops.compose_sql("SELECT %s", ["test"]),
                "SELECT 'test'",
            )
        finally:
            await new_connection.close()

    async def test_bypass_timezone_configuration(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        class CustomAsyncDatabaseWrapper(AsyncDatabaseWrapper):
            async def _configure_timezone(self, connection):
                return False

        for Wrapper, commit in [
            (AsyncDatabaseWrapper, True),
            (CustomAsyncDatabaseWrapper, False),
        ]:
            with self.subTest(wrapper=Wrapper, commit=commit):
                new_connection = no_pool_connection()
                self.addCleanup(new_connection.close)

                # Set the database default time zone to be different from
                # the time zone in new_connection.settings_dict.
                async with new_connection.cursor() as cursor:
                    await cursor.execute("RESET TIMEZONE")
                    await cursor.execute("SHOW TIMEZONE")
                    db_default_tz = (await cursor.fetchone())[0]
                new_tz = "Europe/Paris" if db_default_tz == "UTC" else "UTC"
                new_connection.timezone_name = new_tz

                settings = new_connection.settings_dict.copy()
                conn = new_connection.connection
                self.assertIs(
                    await Wrapper(settings)._configure_connection(conn), commit
                )

    async def test_bypass_role_configuration(self):
        from django_async_backend.db.backends.postgresql.base import (
            AsyncDatabaseWrapper,
        )

        class CustomAsyncDatabaseWrapper(AsyncDatabaseWrapper):
            async def _configure_role(self, connection):
                return False

        new_connection = no_pool_connection()
        self.addCleanup(new_connection.close)
        await new_connection.connect()

        settings = new_connection.settings_dict.copy()
        settings["OPTIONS"]["assume_role"] = "django_nonexistent_role"
        conn = new_connection.connection
        self.assertIs(
            await CustomAsyncDatabaseWrapper(settings)._configure_connection(
                conn
            ),
            False,
        )


class ServerSideCursorsPostgresTests(AsyncioTestCase):
    cursor_fields = (
        "name, statement, is_holdable, is_binary, is_scrollable, creation_time"
    )
    PostgresCursor = namedtuple("PostgresCursor", cursor_fields)

    async def asyncSetUp(self):
        await create_table()

        self.p0 = await create_instance(0)
        self.p1 = await create_instance(1)

    async def asyncTearDown(self):
        await drop_table()

    async def inspect_cursors(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT {fields} FROM pg_cursors;".format(
                    fields=self.cursor_fields
                )
            )
            cursors = await cursor.fetchall()
        return [self.PostgresCursor._make(cursor) for cursor in cursors]

    async def assertUsesCursor(self, query, num_expected=1):
        connection = async_connections[DEFAULT_DB_ALIAS]
        cursor_name = "my_server_side_cursor"
        async with connection.create_cursor(name=cursor_name) as cursor:
            # await cursor.execute("SELECT * FROM reporter_table_tmp")
            await cursor.execute(query)
            # Open the cursor by fetching a row
            await cursor.fetchone()

            # Now inspect open cursors
            cursors = await self.inspect_cursors()
            self.assertEqual(len(cursors), num_expected)
            self.assertIn(cursor_name, cursors[0].name)
            self.assertFalse(cursors[0].is_scrollable)
            self.assertFalse(cursors[0].is_holdable)
            self.assertFalse(cursors[0].is_binary)

    async def test_server_side_cursor(self):
        await self.assertUsesCursor("SELECT * FROM reporter_table_tmp")

    async def test_server_side_cursor_many_cursors(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        query = "SELECT * FROM reporter_table_tmp"
        async with connection.create_cursor(
            name="my_server_side_cursor2"
        ) as cursor:
            # await cursor.execute("SELECT * FROM reporter_table_tmp")
            await cursor.execute(query)
            # Open the cursor by fetching a row
            await cursor.fetchone()

            await self.assertUsesCursor(query, num_expected=2)
