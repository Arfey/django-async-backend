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


class AsyncConnectionHandler(BaseAsyncConnectionHandler):
    settings_name = ConnectionHandler.settings_name
    # Connections needs to still be an actual thread local, as it's truly
    # thread-critical. Database backends should use @async_unsafe to protect
    # their code from async contexts, but this will give those contexts
    # separate connections in case it's needed as well. There's no cleanup
    # after async contexts, though, so we don't allow that if we can help it.
    thread_critical = True

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
