from asgiref.sync import iscoroutinefunction
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
    settings_name = "DATABASES"
    # Connections needs to still be an actual thread local, as it's truly
    # thread-critical. Database backends should use @async_unsafe to protect
    # their code from async contexts, but this will give those contexts
    # separate connections in case it's needed as well. There's no cleanup
    # after async contexts, though, so we don't allow that if we can help it.
    thread_critical = True

    def configure_settings(self, databases):
        return ConnectionHandler.configure_settings(self, databases)

    def create_connection(self, alias):
        db = self.settings[alias]
        backend = load_backend(db["ENGINE"])

        if not hasattr(backend, "AsyncDatabaseWrapper"):
            raise self.exception_class(
                f"The async connection '{alias}' doesn't exist."
            )

        return backend.AsyncDatabaseWrapper(db, alias)
