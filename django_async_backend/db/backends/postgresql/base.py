# flake8: noqa: F401
# for compatibility with django.db.backends.postgresql
from django.db.backends.postgresql.base import *

from django_async_backend.db.backends.postgresql.async_base import (
    AsyncCursor,
    AsyncCursorDebugWrapper,
    AsyncDatabaseWrapper,
    AsyncServerBindingCursor,
    AsyncServerSideCursor,
)
