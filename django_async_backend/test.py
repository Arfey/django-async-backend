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
    connection for the duration of the test. The single transaction Django
    opens around the class then covers writes done via either pool, and
    per-test savepoints roll both back together.

    Async test methods (``async def test_*``) are run via ``async_to_sync``;
    optional ``asyncSetUp`` / ``asyncTearDown`` hooks share the same event
    loop and task as the test method.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Allow the dedicated async_to_sync worker thread to use the sync conn
        # opened on the main test thread.
        for alias in cls._databases_names(include_mirrors=False):
            django_sync_connections[alias].inc_thread_sharing()

    @classmethod
    def tearDownClass(cls):
        for alias in cls._databases_names(include_mirrors=False):
            django_sync_connections[alias].dec_thread_sharing()
        super().tearDownClass()

    def _callTestMethod(self, method):
        # Django's SimpleTestCase.__call__ has already wrapped async test
        # methods with async_to_sync, so ``method`` here is sync. Look at
        # the original on the class to decide which path to take.
        raw = getattr(type(self), self._testMethodName, None)
        if asyncio.iscoroutinefunction(raw):
            sync_conns = {
                alias: django_sync_connections[alias]
                for alias in type(self)._databases_names(include_mirrors=False)
            }
            async_to_sync(self._run_async_test)(
                raw.__get__(self, type(self)), sync_conns
            )
        else:
            super()._callTestMethod(method)

    async def _run_async_test(self, method, sync_conns):
        proxies = {
            alias: SyncBridgedAsyncWrapper(alias, sync_conn=conn)
            for alias, conn in sync_conns.items()
        }

        # Make sync_to_async calls inside this test see the same Django
        # sync wrapper the TestCase opened its transaction on. Django's
        # default connections._connections is thread-local; swapping in a
        # plain SimpleNamespace makes it thread-blind for the test's
        # duration so the asgiref worker thread reuses the main wrapper
        # instead of opening a new autocommit connection.
        sync_conn_storage = types.SimpleNamespace(**sync_conns)
        original_sync_storage = django_sync_connections._connections
        django_sync_connections._connections = sync_conn_storage

        current_task = asyncio.current_task()
        originals = {}
        for alias, proxy in proxies.items():
            proxy._task = current_task
            try:
                originals[alias] = getattr(
                    async_connections._connections, alias
                )
            except AttributeError:
                originals[alias] = None
            setattr(async_connections._connections, alias, proxy)
        try:
            async_setup = getattr(self, "asyncSetUp", None)
            if asyncio.iscoroutinefunction(async_setup):
                await async_setup()
            if method is not None:
                await method()
            async_teardown = getattr(self, "asyncTearDown", None)
            if asyncio.iscoroutinefunction(async_teardown):
                await async_teardown()
        finally:
            for alias, original in originals.items():
                if original is None:
                    if hasattr(async_connections._connections, alias):
                        delattr(async_connections._connections, alias)
                else:
                    setattr(async_connections._connections, alias, original)
            django_sync_connections._connections = original_sync_storage


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
