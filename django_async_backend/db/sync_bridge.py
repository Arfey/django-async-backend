"""
Sync-bridged async DatabaseWrapper used by AsyncioRollbackTestCase.

Routes every operation that would normally hit an async psycopg connection
through Django's sync connection via ``sync_to_async``. This lets a Django
``TestCase`` wrap a single transaction around both sync ORM calls and code
that uses ``async_connections`` for raw async SQL, so test-end rollback
covers everything.

Trade-offs (intentional):

- All async I/O is forced onto a sync_to_async worker thread. There is no
  real concurrency between two tasks using the bridged connection — they
  serialize through Django's sync wrapper. This is fine for tests; it
  would not be fine for production.
- ``async_atomic`` creates savepoints on the sync connection. The TestCase
  has already opened a transaction (autocommit=False on the sync conn),
  so async_atomic's "already in a transaction at the connection level"
  branch fires and nests savepoints on top of Django's own per-test
  savepoint. PostgreSQL is the source of truth for the savepoint stack.
"""
import _thread
import collections
from contextlib import contextmanager

from asgiref.sync import sync_to_async
from django.db import connections as django_sync_connections
from django.db.transaction import TransactionManagementError


class _BridgedAsyncCursor:
    """Mimics AsyncCursorWrapper but delegates to a sync cursor.

    Only the methods that user code calls on the cursor are implemented.
    """

    def __init__(self, sync_cursor, db):
        self.cursor = sync_cursor
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await sync_to_async(self.cursor.close, thread_sensitive=True)()
        except Exception:
            pass

    async def execute(self, sql, params=None):
        return await sync_to_async(self.cursor.execute, thread_sensitive=True)(
            sql, params
        )

    async def executemany(self, sql, param_list):
        return await sync_to_async(
            self.cursor.executemany, thread_sensitive=True
        )(sql, param_list)

    async def fetchone(self):
        return await sync_to_async(self.cursor.fetchone, thread_sensitive=True)()

    async def fetchall(self):
        return await sync_to_async(self.cursor.fetchall, thread_sensitive=True)()

    async def fetchmany(self, size=None):
        if size is None:
            size = self.cursor.arraysize
        return await sync_to_async(
            self.cursor.fetchmany, thread_sensitive=True
        )(size)

    async def close(self):
        await sync_to_async(self.cursor.close, thread_sensitive=True)()

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @property
    def description(self):
        return self.cursor.description

    @property
    def lastrowid(self):
        return self.cursor.lastrowid


class SyncBridgedAsyncWrapper:
    """Drop-in for AsyncDatabaseWrapper that runs everything on the sync conn.

    State the async transaction machinery reads/writes (``in_atomic_block``,
    ``savepoint_ids``, etc.) lives on this proxy as plain attributes. Actual
    SQL — including SAVEPOINT statements — is dispatched to the sync conn.
    """

    def __init__(self, alias, sync_conn=None):
        """``sync_conn`` should be the Django sync connection captured on
        the main test thread — the one TestCase opened its transaction on.
        Falling back to a fresh per-thread lookup defeats the bridge."""
        self.alias = alias
        self._sync = (
            sync_conn if sync_conn is not None
            else django_sync_connections[alias]
        )

        # Async-side state machine fields read/written by async_atomic.
        # Initialized fresh per test class (see AsyncioRollbackTestCase).
        self.in_atomic_block = False
        self.savepoint_state = 0
        self.savepoint_ids = []
        self.atomic_blocks = []
        self.commit_on_exit = True
        self.needs_rollback = False
        self.rollback_exc = None
        self.closed_in_transaction = False

        self.execute_wrappers = []
        self.queries_log = collections.deque(maxlen=9000)
        self.force_debug_cursor = False
        self.run_on_commit = []
        self.run_commit_hooks_on_set_autocommit_on = False
        self.errors_occurred = False
        self.health_check_done = True
        self.health_check_enabled = False

        # Required by code that introspects task ownership.
        self._task = None

    # ----- Attributes that defer to the sync connection -----

    @property
    def vendor(self):
        return self._sync.vendor

    @property
    def display_name(self):
        return self._sync.display_name

    @property
    def settings_dict(self):
        return self._sync.settings_dict

    @property
    def features(self):
        return self._sync.features

    @property
    def ops(self):
        return self._sync.ops

    @property
    def Database(self):
        return self._sync.Database

    @property
    def connection(self):
        # Truthy whenever the sync conn is open. async_atomic checks this in
        # the closed_in_transaction branch.
        return self._sync.connection

    @connection.setter
    def connection(self, value):
        # async_atomic only ever sets this back to None on a closed_in_tx exit;
        # we route that to the sync conn.
        self._sync.connection = value

    @property
    def autocommit(self):
        return self._sync.autocommit

    @autocommit.setter
    def autocommit(self, value):
        # async_atomic mirrors what get_autocommit returned. Don't actually
        # change the sync conn — TestCase owns its autocommit state.
        pass

    @property
    def queries_logged(self):
        return self.force_debug_cursor

    @property
    def queries(self):
        return list(self.queries_log)

    # ----- Connection lifecycle -----

    async def ensure_connection(self):
        if self._sync.connection is None:
            await sync_to_async(self._sync.ensure_connection, thread_sensitive=True)()

    async def close(self):
        # Don't actually close the sync conn — TestCase owns it.
        return

    async def close_if_health_check_failed(self):
        return

    async def close_if_unusable_or_obsolete(self):
        return

    async def get_database_version(self):
        return await sync_to_async(
            self._sync.get_database_version, thread_sensitive=True
        )()

    async def check_database_version_supported(self):
        return

    # ----- Transaction primitives -----

    async def get_autocommit(self):
        await self.ensure_connection()
        return self._sync.autocommit

    async def set_autocommit(
        self, autocommit, force_begin_transaction_with_broken_autocommit=False
    ):
        # No-op: TestCase controls autocommit on the sync conn. async_atomic
        # only calls this when entering/leaving its outermost block; in our
        # context async_atomic detects autocommit is already off and uses
        # the savepoint code path instead.
        return

    async def commit(self):
        # No-op: TestCase will rollback the outer transaction.
        self.errors_occurred = False
        self.run_commit_hooks_on_set_autocommit_on = True

    async def rollback(self):
        self.errors_occurred = False
        self.needs_rollback = False
        self.run_on_commit = []

    # ----- Savepoints (route to the underlying sync conn) -----

    async def _exec_on_sync(self, sql):
        sync = self._sync

        def _run():
            with sync.cursor() as cursor:
                cursor.execute(sql)

        await sync_to_async(_run, thread_sensitive=True)()

    async def _savepoint(self, sid):
        await self._exec_on_sync(self._sync.ops.savepoint_create_sql(sid))

    async def _savepoint_rollback(self, sid):
        await self._exec_on_sync(self._sync.ops.savepoint_rollback_sql(sid))

    async def _savepoint_commit(self, sid):
        await self._exec_on_sync(self._sync.ops.savepoint_commit_sql(sid))

    async def _savepoint_allowed(self):
        return (
            self._sync.features.uses_savepoints
            and not await self.get_autocommit()
        )

    async def savepoint(self):
        if not await self._savepoint_allowed():
            return
        thread_ident = _thread.get_ident()
        tid = str(thread_ident).replace("-", "")
        self.savepoint_state += 1
        # `as_` prefix avoids any chance of collision with sync-side savepoint
        # names that share the same scheme.
        sid = "as_%s_x%d" % (tid, self.savepoint_state)
        await self._savepoint(sid)
        return sid

    async def savepoint_rollback(self, sid):
        if not await self._savepoint_allowed():
            return
        await self._savepoint_rollback(sid)
        self.run_on_commit = [
            (sids, func, robust)
            for (sids, func, robust) in self.run_on_commit
            if sid not in sids
        ]

    async def savepoint_commit(self, sid):
        if not await self._savepoint_allowed():
            return
        await self._savepoint_commit(sid)

    def clean_savepoints(self):
        self.savepoint_state = 0

    # ----- Cursor -----

    async def cursor(self):
        await self.ensure_connection()
        sync_cursor = await sync_to_async(self._sync.cursor, thread_sensitive=True)()
        return _BridgedAsyncCursor(sync_cursor, self)

    def chunked_cursor(self):
        return self.cursor()

    # ----- Validation helpers (called by async_atomic / cursor paths) -----

    def validate_thread_sharing(self):
        # We've called inc_thread_sharing on the sync conn for the test
        # class duration; allow async-side use freely.
        return

    def validate_no_atomic_block(self):
        if self.in_atomic_block:
            raise TransactionManagementError(
                "This is forbidden when an 'atomic' block is active."
            )

    def validate_no_broken_transaction(self):
        if self.needs_rollback:
            raise TransactionManagementError(
                "An error occurred in the current transaction. You can't "
                "execute queries until the end of the 'atomic' block."
            ) from self.rollback_exc

    def get_rollback(self):
        if not self.in_atomic_block:
            raise TransactionManagementError(
                "The rollback flag doesn't work outside of an 'atomic' block."
            )
        return self.needs_rollback

    def set_rollback(self, rollback):
        if not self.in_atomic_block:
            raise TransactionManagementError(
                "The rollback flag doesn't work outside of an 'atomic' block."
            )
        self.needs_rollback = rollback

    @property
    def wrap_database_errors(self):
        # Use the sync conn's error wrapper — it knows the right exception
        # types for the underlying driver.
        return _NoopErrorWrapper()

    @contextmanager
    def execute_wrapper(self, wrapper):
        self.execute_wrappers.append(wrapper)
        try:
            yield
        finally:
            self.execute_wrappers.pop()

    async def on_commit(self, func, robust=False):
        # Inside a TestCase the outer transaction is rolled back, so on_commit
        # callbacks never fire. Match Django's TestCase.captureOnCommitCallbacks
        # semantics: queue them on the run_on_commit list with the active
        # savepoint set; users can opt into a captureOnCommitCallbacks helper
        # if they want to assert they were registered.
        if not callable(func):
            raise TypeError("on_commit()'s callback must be a callable.")
        if self.in_atomic_block:
            self.run_on_commit.append((set(self.savepoint_ids), func, robust))


class _NoopErrorWrapper:
    """No-op replacement for DatabaseErrorWrapper.

    Django's sync error wrapper translates psycopg exceptions into Django's
    common DB exception hierarchy. Our bridged cursor calls run inside
    sync_to_async on the sync wrapper which already does its own error
    translation, so we don't need a second layer here.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def __call__(self, fn):
        return fn
