import asyncio
import types
from functools import wraps
from unittest import IsolatedAsyncioTestCase

from asgiref.sync import async_to_sync
from django.core.signals import request_started
from django.db import connections as django_sync_connections
from django.db import reset_queries
from django.test import TestCase as DjangoTestCase

from django_async_backend.db import async_connections
from django_async_backend.db.sync_bridge import SyncBridgedAsyncWrapper
from django_async_backend.db.transaction import async_atomic


def _refresh_connection_task_ownership_decorator(fn):
    @wraps(fn)
    async def inner(*args, **kwargs):
        task = asyncio.current_task()
        for name in async_connections.settings.keys():
            connection = async_connections[name]
            connection._task = task
        return await fn(*args, **kwargs)

    return inner


class AsyncioTransactionTestCase(IsolatedAsyncioTestCase):
    # todo: fix problem with creating models
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        for name, method in list(vars(cls).items()):
            if name.startswith("test") and callable(method):
                setattr(
                    cls,
                    name,
                    _refresh_connection_task_ownership_decorator(method),
                )

    def _callSetUp(self):
        self._asyncioRunner.get_loop()
        self._asyncioTestContext.run(self.setUp)
        self._callAsync(
            _refresh_connection_task_ownership_decorator(self.asyncSetUp)
        )

    async def _close_connection(self):
        for name in async_connections.settings.keys():
            await async_connections[name].close()

    def _callTearDown(self):
        self._callAsync(
            _refresh_connection_task_ownership_decorator(self.asyncTearDown)
        )
        self._callAsync(
            _refresh_connection_task_ownership_decorator(
                self._close_connection
            )
        )
        self._asyncioTestContext.run(self.tearDown)


class AsyncioTestCase(AsyncioTransactionTestCase):

    async def _init_transaction(self):
        self.connections = {}
        self.atomic_cms = {}
        self.atomics = {}

        for name in async_connections.settings.keys():
            connection = async_connections[name]
            self.connections[name] = connection
            self.atomic_cms[name] = async_atomic(name)
            self.atomics[name] = await self.atomic_cms[name].__aenter__()

    async def _close_transaction(self):
        for name in async_connections.settings.keys():
            connection = async_connections[name]
            connection.set_rollback(True)
            await self.atomic_cms[name].__aexit__(None, None, None)
            await connection.close()

    def _callSetUp(self):
        # Force loop to be initialized and set as the current loop
        # so that setUp functions can use get_event_loop() and get the
        # correct loop instance.
        self._asyncioRunner.get_loop()
        self._asyncioTestContext.run(self.setUp)
        self._callAsync(self._init_transaction)
        self._callAsync(
            _refresh_connection_task_ownership_decorator(self.asyncSetUp)
        )

    def _callTearDown(self):
        self._callAsync(
            _refresh_connection_task_ownership_decorator(self.asyncTearDown)
        )
        self._callAsync(self._close_transaction)
        self._asyncioTestContext.run(self.tearDown)


class AsyncioRollbackTestCase(DjangoTestCase):
    """Django ``TestCase`` that also rolls back ``async_connections`` writes.

    Routes every ``async_connections[alias]`` operation through Django's sync
    connection for the duration of the test class. The single transaction
    Django opens around the class then covers writes done via either pool,
    and per-test savepoints roll both back together.

    The proxy is installed at ``setUpClass`` by swapping
    ``async_connections._connections`` (and Django's
    ``connections._connections``) to thread-blind ``SimpleNamespace``\\s
    for the class duration. That means even sync test methods that drive
    async code through ``async_to_sync(...)`` see the proxy on the worker
    thread — without that swap, the worker resolves a fresh per-thread
    async connection that opens its own autocommit transaction.

    Async test methods (``async def test_*``) are run via ``async_to_sync``;
    optional ``asyncSetUp`` / ``asyncTearDown`` hooks share the same event
    loop and task as the test method. Sync test methods run as Django's
    own ``TestCase`` would.

    Tradeoff: there's no real concurrency between two tasks using the
    bridged connection during a test — they serialise through Django's
    sync wrapper. Same shape as Django's regular TestCase semantics
    (one transaction, no parallelism). Production keeps its real async
    pool unchanged.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        aliases = list(cls._databases_names(include_mirrors=False))
        sync_conns = {
            alias: django_sync_connections[alias] for alias in aliases
        }

        # Allow worker threads spawned by ``async_to_sync`` to use the
        # sync conn opened on the main thread.
        for conn in sync_conns.values():
            conn.inc_thread_sharing()

        proxies = {
            alias: SyncBridgedAsyncWrapper(alias, sync_conn=conn)
            for alias, conn in sync_conns.items()
        }
        cls._async_rollback_proxies = proxies

        # Swap both connection stores for thread-blind SimpleNamespaces.
        # ``django_sync_connections._connections`` is normally an
        # asgiref.Local → ContextVar; the SimpleNamespace makes the
        # asgiref worker thread reuse the same wrapper instead of
        # opening its own autocommit connection. Same trick for
        # ``async_connections._connections`` so any task accessing
        # ``async_connections[alias]`` — main thread or worker — gets
        # the proxy.
        cls._async_rollback_orig_sync_storage = (
            django_sync_connections._connections
        )
        django_sync_connections._connections = types.SimpleNamespace(
            **sync_conns
        )

        cls._async_rollback_orig_async_storage = (
            async_connections._connections
        )
        async_connections._connections = types.SimpleNamespace(**proxies)

    @classmethod
    def tearDownClass(cls):
        async_connections._connections = (
            cls._async_rollback_orig_async_storage
        )
        django_sync_connections._connections = (
            cls._async_rollback_orig_sync_storage
        )
        for alias in cls._databases_names(include_mirrors=False):
            django_sync_connections[alias].dec_thread_sharing()
        super().tearDownClass()

    def _reset_proxy_state(self):
        """Reset per-test mutable state on every proxy.

        The class-scope proxies persist for the whole class; their
        atomic/savepoint bookkeeping needs to start fresh for each
        test method (Django's ``TestCase`` opens a per-test savepoint
        on the sync conn separately).
        """
        for proxy in self._async_rollback_proxies.values():
            proxy.in_atomic_block = False
            proxy.savepoint_state = 0
            proxy.savepoint_ids = []
            proxy.atomic_blocks = []
            proxy.commit_on_exit = True
            proxy.needs_rollback = False
            proxy.rollback_exc = None
            proxy.run_on_commit = []
            proxy._task = None

    def _callTestMethod(self, method):
        self._reset_proxy_state()

        # Django's SimpleTestCase.__call__ has already wrapped async
        # test methods with async_to_sync, so ``method`` here is sync.
        # Look at the original on the class to decide which path to take.
        raw = getattr(type(self), self._testMethodName, None)
        if asyncio.iscoroutinefunction(raw):
            async_to_sync(self._run_async_test)(
                raw.__get__(self, type(self))
            )
        else:
            # Sync test method. Anything inside that calls
            # ``async_to_sync(some_async)`` will run on the asgiref
            # worker thread — the SimpleNamespace storage swap means
            # it still sees the proxies installed in setUpClass.
            super()._callTestMethod(method)

    async def _run_async_test(self, method):
        current_task = asyncio.current_task()
        for proxy in self._async_rollback_proxies.values():
            proxy._task = current_task

        async_setup = getattr(self, "asyncSetUp", None)
        if asyncio.iscoroutinefunction(async_setup):
            await async_setup()
        if method is not None:
            await method()
        async_teardown = getattr(self, "asyncTearDown", None)
        if asyncio.iscoroutinefunction(async_teardown):
            await async_teardown()


class AsyncCaptureQueriesContext:

    def __init__(self, connection):
        self.connection = connection

    def __len__(self):
        return len(self.captured_queries)

    @property
    def captured_queries(self):
        return self.connection.queries[
            slice(self.initial_queries, self.final_queries)
        ]

    async def __aenter__(self):
        self.force_debug_cursor = self.connection.force_debug_cursor
        self.connection.force_debug_cursor = True
        # Run any initialization queries if needed so that they won't be
        # included as part of the count.
        await self.connection.ensure_connection()
        self.initial_queries = len(self.connection.queries_log)
        self.final_queries = None
        self.reset_queries_disconnected = request_started.disconnect(
            reset_queries
        )
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.connection.force_debug_cursor = self.force_debug_cursor
        if self.reset_queries_disconnected:
            request_started.connect(reset_queries)
        if exc_type is not None:
            return
        self.final_queries = len(self.connection.queries_log)
