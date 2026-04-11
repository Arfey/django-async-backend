# flake8: noqa: F401
import warnings

from django.conf import settings
from django.db.backends.postgresql.base import *

# for compatibility with django.db.backends.postgresql
from django.db.backends.postgresql.base import (
    DatabaseWrapper as _DatabaseWrapper,
)

from django_async_backend.db.backends.postgresql.async_base import (
    AsyncCursor,
    AsyncCursorDebugWrapper,
    AsyncDatabaseOperations,
    AsyncDatabaseWrapper,
    AsyncServerBindingCursor,
    AsyncServerSideCursor,
)


class DatabaseWrapper(_DatabaseWrapper):
    def get_connection_params(self):
        params = super().get_connection_params()

        if not getattr(settings, "ASYNC_BACKEND_DISABLE_POOL_WARNING", False) and self.settings_dict.get(
            "OPTIONS", {}
        ).get("pool"):
            warnings.warn(
                "Using the asynchronous Django backend with a connection pool is "
                "unsafe in synchronous mode. Connection pools require the same "
                "event loop for each request to manage state, but the WSGI "
                "handler creates a new event loop for every request.",
                RuntimeWarning,
            )

        return params
