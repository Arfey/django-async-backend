import asyncio
import threading

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import NO_DB_ALIAS
from django.db.backends.postgresql.base import DatabaseWrapper
from django.db.backends.postgresql.psycopg_any import (
    IsolationLevel,
    get_adapters_template,
    is_psycopg3,
    register_tzloader,
)

from django_async_backend.db.backends.base.base import BaseAsyncDatabaseWrapper
from django_async_backend.db.backends.utils import (
    AsyncCursorDebugWrapper as BaseAsyncCursorDebugWrapper,
)

try:
    import psycopg as Database
except ImportError:
    raise ImproperlyConfigured("Error loading psycopg module")


if not is_psycopg3:
    raise ImproperlyConfigured(
        "psycopg version 3 or newer is required; "
        f"you have {Database.__version__}"
    )


TIMESTAMPTZ_OID = Database.adapters.types["timestamptz"].oid


class AsyncDatabaseWrapper(BaseAsyncDatabaseWrapper):
    vendor = DatabaseWrapper.vendor
    display_name = DatabaseWrapper.display_name
    data_types = DatabaseWrapper.data_types
    data_type_check_constraints = DatabaseWrapper.data_type_check_constraints
    data_types_suffix = DatabaseWrapper.data_types_suffix
    operators = DatabaseWrapper.operators
    pattern_esc = DatabaseWrapper.pattern_esc
    pattern_ops = DatabaseWrapper.pattern_ops

    Database = Database
    features_class = DatabaseWrapper.features_class
    ops_class = DatabaseWrapper.ops_class

    # PostgreSQL backend-specific attributes.
    _named_cursor_idx = 0
    _connection_pools = {}
    _pg_version = None

    @property
    def pool(self):
        pool_options = self.settings_dict["OPTIONS"].get("pool")
        if self.alias == NO_DB_ALIAS or not pool_options:
            return None

        if self.alias not in self._connection_pools:
            if self.settings_dict.get("CONN_MAX_AGE", 0) != 0:
                raise ImproperlyConfigured(
                    "Pooling doesn't support persistent connections."
                )
            # Set the default options.
            if pool_options is True:
                pool_options = {}

            try:
                from psycopg_pool import AsyncConnectionPool
            except ImportError as err:
                raise ImproperlyConfigured(
                    "Error loading psycopg_pool module.\n"
                    "Did you install psycopg[pool]?"
                ) from err

            connect_kwargs = self.get_connection_params()
            # Ensure we run in autocommit, Django properly sets it later on.
            connect_kwargs["autocommit"] = True
            enable_checks = self.settings_dict["CONN_HEALTH_CHECKS"]
            pool = AsyncConnectionPool(
                kwargs=connect_kwargs,
                open=False,  # Do not open the pool during startup.
                configure=self._configure_connection,
                check=(
                    AsyncConnectionPool.check_connection
                    if enable_checks
                    else None
                ),
                **pool_options,
            )
            # setdefault() ensures that multiple threads don't set this in
            # parallel. Since we do not open the pool during it's init above,
            # this means that at worst during startup multiple threads generate
            # pool objects and the first to set it wins.
            self._connection_pools.setdefault(self.alias, pool)

        return self._connection_pools[self.alias]

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
            del self._connection_pools[self.alias]

    async def get_database_version(self):
        """
        Return a tuple of the database's version.
        E.g. for pg_version 120004, return (12, 4).
        """
        return divmod(await self.pg_version(), 10000)

    def get_connection_params(self):
        settings_dict = self.settings_dict
        # None may be used to connect to the default 'postgres' db
        if settings_dict["NAME"] == "" and not settings_dict["OPTIONS"].get(
            "service"
        ):
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME or OPTIONS['service'] value."
            )
        if len(settings_dict["NAME"] or "") > self.ops.max_name_length():
            raise ImproperlyConfigured(
                "The database name '%s' (%d characters) is longer than "
                "PostgreSQL's limit of %d characters. Supply a shorter NAME "
                "in settings.DATABASES."
                % (
                    settings_dict["NAME"],
                    len(settings_dict["NAME"]),
                    self.ops.max_name_length(),
                )
            )
        if settings_dict["NAME"]:
            conn_params = {
                "dbname": settings_dict["NAME"],
                **settings_dict["OPTIONS"],
            }
        elif settings_dict["NAME"] is None:
            # Connect to the default 'postgres' db.
            settings_dict["OPTIONS"].pop("service", None)
            conn_params = {"dbname": "postgres", **settings_dict["OPTIONS"]}
        else:
            conn_params = {**settings_dict["OPTIONS"]}
        conn_params["client_encoding"] = "UTF8"

        conn_params.pop("assume_role", None)
        conn_params.pop("isolation_level", None)

        server_side_binding = conn_params.pop("server_side_binding", None)
        conn_params.setdefault(
            "cursor_factory",
            (
                AsyncServerBindingCursor
                if server_side_binding is True
                else AsyncCursor
            ),
        )
        if settings_dict["USER"]:
            conn_params["user"] = settings_dict["USER"]
        if settings_dict["PASSWORD"]:
            conn_params["password"] = settings_dict["PASSWORD"]
        if settings_dict["HOST"]:
            conn_params["host"] = settings_dict["HOST"]
        if settings_dict["PORT"]:
            conn_params["port"] = settings_dict["PORT"]

        conn_params["context"] = get_adapters_template(
            settings.USE_TZ, self.timezone
        )
        # Disable prepared statements by default to keep connection poolers
        # working. Can be reenabled via OPTIONS in the settings dict.
        conn_params["prepare_threshold"] = conn_params.pop(
            "prepare_threshold", None
        )

        return conn_params

    async def get_new_connection(self, conn_params):
        # self.isolation_level must be set:
        # - after connecting to the database in order to obtain the database's
        #   default when no value is explicitly specified in options.
        # - before calling _set_autocommit() because if autocommit is on, that
        #   will set connection.isolation_level to ISOLATION_LEVEL_AUTOCOMMIT.
        options = self.settings_dict["OPTIONS"]
        set_isolation_level = False
        try:
            isolation_level_value = options["isolation_level"]
        except KeyError:
            self.isolation_level = IsolationLevel.READ_COMMITTED
        else:
            # Set the isolation level to the value from OPTIONS.
            try:
                self.isolation_level = IsolationLevel(isolation_level_value)
                set_isolation_level = True
            except ValueError:
                raise ImproperlyConfigured(
                    "Invalid transaction isolation "
                    f"level {isolation_level_value} specified. Use one of the"
                    " psycopg.IsolationLevel values."
                )
        if self.pool:
            # If nothing else has opened the pool, open it now.
            await self.pool.open()
            connection = await self.pool.getconn()
        else:
            connection = await self.Database.AsyncConnection.connect(
                **conn_params
            )
        if set_isolation_level:
            connection.isolation_level = self.isolation_level

        return connection

    async def ensure_timezone(self):
        # Close the pool so new connections pick up the correct timezone.
        await self.close_pool()
        if self.connection is None:
            return False
        return await self._configure_timezone(self.connection)

    async def _configure_timezone(self, connection):
        conn_timezone_name = connection.info.parameter_status("TimeZone")
        timezone_name = self.timezone_name
        if timezone_name and conn_timezone_name != timezone_name:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    self.ops.set_time_zone_sql(), [timezone_name]
                )
            return True
        return False

    async def _configure_role(self, connection):
        if new_role := self.settings_dict["OPTIONS"].get("assume_role"):
            async with connection.cursor() as cursor:
                sql = self.ops.compose_sql("SET ROLE %s", [new_role])
                await cursor.execute(sql)
            return True
        return False

    async def _configure_connection(self, connection):
        # This function is called from init_connection_state and from the
        # psycopg pool itself after a connection is opened.

        # Commit after setting the time zone.
        commit_tz = await self._configure_timezone(connection)
        # Set the role on the connection. This is useful if the credential used
        # to login is not the same as the role that owns database resources. As
        # can be the case when using temporary or ephemeral credentials.
        commit_role = await self._configure_role(connection)

        return commit_role or commit_tz

    async def _close(self):
        if self.connection is not None:
            # `wrap_database_errors` only works for `putconn` as long as there
            # is no `reset` function set in the pool because it is deferred
            # into a thread and not directly executed.
            with self.wrap_database_errors:
                if self.pool:
                    # Ensure the correct pool is returned. This is a workaround
                    # for tests so a pool can be changed on setting changes
                    # (e.g. USE_TZ, TIME_ZONE).
                    await self.connection._pool.putconn(self.connection)
                    # Connection can no longer be used.
                    self.connection = None
                else:
                    return await self.connection.close()

    async def init_connection_state(self):
        await super().init_connection_state()

        if self.connection is not None and not self.pool:
            commit = await self._configure_connection(self.connection)

            if commit and not await self.get_autocommit():
                await self.connection.commit()

    def create_cursor(self, name=None):
        if name:
            if (
                self.settings_dict["OPTIONS"].get("server_side_binding")
                is not True
            ):
                # psycopg >= 3 forces the usage of server-side bindings for
                # named cursors so a specialized class that implements
                # server-side cursors while performing client-side bindings
                # must be used if `server_side_binding` is disabled (default).
                cursor = AsyncServerSideCursor(
                    self.connection,
                    name=name,
                    scrollable=False,
                    withhold=self.connection.autocommit,
                )
            else:
                # In autocommit mode, the cursor will be used outside of a
                # transaction, hence use a holdable cursor.
                cursor = self.connection.cursor(
                    name, scrollable=False, withhold=self.connection.autocommit
                )
        else:
            cursor = self.connection.cursor()

        # Register the cursor timezone only if the connection disagrees, to
        # avoid copying the adapter map.
        tzloader = self.connection.adapters.get_loader(
            TIMESTAMPTZ_OID,
            Database.pq.Format.TEXT,
        )
        if self.timezone != tzloader.timezone:
            register_tzloader(self.timezone, cursor)

        return cursor

    def tzinfo_factory(self, offset):
        return self.timezone

    def chunked_cursor(self):
        self._named_cursor_idx += 1
        # Get the current async task
        # Note that right now this is behind @async_unsafe, so this is
        # unreachable, but in future we'll start loosening this restriction.
        # For now, it's here so that every use of "threading" is
        # also async-compatible.
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        # Current task can be none even if the current_task call didn't error
        if current_task:
            task_ident = str(id(current_task))
        else:
            task_ident = "sync"
        # Use that and the thread ident to get a unique name
        return self._cursor(
            name="_django_curs_%d_%s_%d"
            % (
                # Avoid reusing name in other threads / tasks
                threading.current_thread().ident,
                task_ident,
                self._named_cursor_idx,
            )
        )

    async def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    async def is_usable(self):
        if self.connection is None:
            return False
        try:
            # Use a psycopg cursor directly, bypassing Django's utilities.
            async with self.connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
        except Database.Error:
            return False
        else:
            return True

    async def close_if_health_check_failed(self):
        if self.pool:
            # The pool only returns healthy connections.
            return
        return await super().close_if_health_check_failed()

    async def pg_version(self):
        if self._pg_version is None:
            async with self.temporary_connection():
                self._pg_version = self.connection.info.server_version

        return self._pg_version

    def make_debug_cursor(self, cursor):
        return AsyncCursorDebugWrapper(cursor, self)


class AsyncServerBindingCursor(Database.AsyncCursor):
    pass


class AsyncCursor(Database.AsyncClientCursor):
    pass


class AsyncServerSideCursor(
    Database.client_cursor.ClientCursorMixin, Database.AsyncServerCursor
):
    """
    ClientCursorMixin forces the usage of client-side bindings while
    ServerCursor implements the logic required to declare and scroll
    through named cursors.

    Mixing ClientCursorMixin in wouldn't be necessary if Cursor allowed to
    specify how parameters should be bound instead, which ServerCursor
    would inherit, but that's not the case.
    """


class AsyncCursorDebugWrapper(BaseAsyncCursorDebugWrapper):
    async def copy(self, statement):
        with self.debug_sql(statement):
            return await self.cursor.copy(statement)
