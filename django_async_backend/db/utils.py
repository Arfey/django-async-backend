import asyncio
import threading
from contextvars import ContextVar

from asgiref.sync import iscoroutinefunction
from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS
from django.db.utils import ConnectionHandler
from django.db.utils import DatabaseErrorWrapper as _DatabaseErrorWrapper
from django.db.utils import load_backend

from django_async_backend.utils.connection import BaseAsyncConnectionHandler


class DatabaseErrorWrapper(_DatabaseErrorWrapper):
    def __call__(self, func):
        # Note that we are intentionally not using @wraps here for performance
        # reasons. Refs #21109.
        if iscoroutinefunction(func):

            async def inner(*args, **kwargs):
                with self:
                    return await func(*args, **kwargs)

        else:

            def inner(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)

        return inner


class _TaskAwareLocal:
    """
    Connection storage that gives each async task its own connections.

    In sync contexts, uses thread-local storage (same as Django).
    In async contexts, uses a ContextVar keyed by asyncio.current_task()
    so concurrent tasks on the same event loop thread each get their
    own database connections.

    Django's default Local (from asgiref) with thread_critical=True stores
    connections per-thread. Since all async tasks share one event loop
    thread, they share one connection — corrupting transaction state
    (in_atomic_block, savepoint_ids, needs_rollback) under concurrency.

    This class is a drop-in replacement: BaseConnectionHandler accesses
    self._connections via getattr/setattr/delattr, all of which are
    delegated to the per-task namespace. Connection aliases (e.g.
    "default") are stored as attributes; internal state uses the "_"
    prefix convention to avoid collision.
    """

    def __init__(self):
        self._thread_local = threading.local()
        self._task_connections = ContextVar(
            "_task_connections", default=None
        )

    def _get_storage(self):
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None

        if task is None:
            return self._thread_local

        # Each task gets its own namespace. When asyncio.create_task()
        # copies the parent context, the child inherits a reference to
        # the parent's _TaskNamespace. The identity check detects this
        # and creates a fresh namespace for the child, so connections
        # are never shared across tasks.
        storage = self._task_connections.get()
        if storage is None or storage._task_ref is not task:
            storage = _TaskNamespace(task)
            self._task_connections.set(storage)

        return storage

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._get_storage(), name)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._get_storage(), name, value)

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
        else:
            delattr(self._get_storage(), name)


class _TaskNamespace:
    """Simple attribute namespace tied to a specific async task."""

    def __init__(self, task):
        self._task_ref = task


class AsyncConnectionHandler(BaseAsyncConnectionHandler):
    settings_name = ConnectionHandler.settings_name

    def __init__(self, settings=None):
        super().__init__(settings)
        self._connections = _TaskAwareLocal()

    def configure_settings(self, databases):
        databases = super().configure_settings(databases)
        if databases == {}:
            databases[DEFAULT_DB_ALIAS] = {
                "ENGINE": "django.db.backends.dummy"
            }
        elif DEFAULT_DB_ALIAS not in databases:
            raise ImproperlyConfigured(
                f"You must define a '{DEFAULT_DB_ALIAS}' database."
            )
        elif databases[DEFAULT_DB_ALIAS] == {}:
            databases[DEFAULT_DB_ALIAS]["ENGINE"] = "django.db.backends.dummy"

        # Configure default settings.
        for conn in databases.values():
            conn.setdefault("ATOMIC_REQUESTS", False)
            conn.setdefault("AUTOCOMMIT", True)
            conn.setdefault("ENGINE", "django.db.backends.dummy")
            if conn["ENGINE"] == "django.db.backends." or not conn["ENGINE"]:
                conn["ENGINE"] = "django.db.backends.dummy"
            conn.setdefault("CONN_MAX_AGE", 0)
            conn.setdefault("CONN_HEALTH_CHECKS", False)
            conn.setdefault("OPTIONS", {})
            conn.setdefault("TIME_ZONE", None)
            for setting in ["NAME", "USER", "PASSWORD", "HOST", "PORT"]:
                conn.setdefault(setting, "")

            test_settings = conn.setdefault("TEST", {})
            default_test_settings = [
                ("CHARSET", None),
                ("COLLATION", None),
                ("MIGRATE", True),
                ("MIRROR", None),
                ("NAME", None),
            ]
            for key, value in default_test_settings:
                test_settings.setdefault(key, value)
        return databases

    def create_connection(self, alias):
        db = self.settings[alias]
        backend = load_backend(db["ENGINE"])

        if not hasattr(backend, "AsyncDatabaseWrapper"):
            raise self.exception_class(
                f"The async connection '{alias}' doesn't exist."
            )

        return backend.AsyncDatabaseWrapper(db, alias)
