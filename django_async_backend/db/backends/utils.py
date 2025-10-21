import functools
import logging
import time
import warnings
from contextlib import contextmanager

from django.apps import apps
from django.db.backends.utils import CursorWrapper
from django_async_backend.utils.await_maybe import await_maybe

logger = logging.getLogger("django_async_backend.db.backends")


class AsyncCursorWrapper:

    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db

    WRAP_ERROR_ATTRS = CursorWrapper.WRAP_ERROR_ATTRS

    APPS_NOT_READY_WARNING_MSG = CursorWrapper.APPS_NOT_READY_WARNING_MSG

    def __getattr__(self, attr):
        cursor_attr = getattr(self.cursor, attr)
        if attr in self.WRAP_ERROR_ATTRS:
            return self.db.wrap_database_errors(cursor_attr)
        else:
            return cursor_attr

    async def __aiter__(self):
        with self.db.wrap_database_errors:
            async for item in self.cursor:
                yield item

    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        # Close instead of passing through to avoid backend-specific behavior
        # (#17671). Catch errors liberally because errors in cleanup code
        # aren't useful.
        try:
            await self.close()
        except self.db.Database.Error:
            pass

    # The following methods cannot be implemented in __getattr__, because the
    # code must run when the method is invoked, not just when it is accessed.

    async def execute(self, sql, params=None):
        return await self._execute_with_wrappers(
            sql, params, many=False, executor=self._execute
        )

    async def executemany(self, sql, param_list):
        return await self._execute_with_wrappers(
            sql, param_list, many=True, executor=self._executemany
        )

    async def _execute_with_wrappers(self, sql, params, many, executor):
        context = {"connection": self.db, "cursor": self}
        for wrapper in reversed(self.db.execute_wrappers):
            executor = functools.partial(wrapper, executor)

        return await await_maybe(executor(sql, params, many, context))

    async def _execute(self, sql, params, *ignored_wrapper_args):
        # Raise a warning during app initialization (stored_app_configs is only
        # ever set during testing).
        if not apps.ready and not apps.stored_app_configs:
            warnings.warn(
                self.APPS_NOT_READY_WARNING_MSG, category=RuntimeWarning
            )

        self.db.validate_no_broken_transaction()

        with self.db.wrap_database_errors:
            if params is None:
                # params default might be backend specific.
                return await self.cursor.execute(sql)
            else:
                return await self.cursor.execute(sql, params)

    async def _executemany(self, sql, param_list, *ignored_wrapper_args):
        # Raise a warning during app initialization (stored_app_configs is only
        # ever set during testing).
        if not apps.ready and not apps.stored_app_configs:
            warnings.warn(
                self.APPS_NOT_READY_WARNING_MSG, category=RuntimeWarning
            )

        self.db.validate_no_broken_transaction()

        with self.db.wrap_database_errors:
            await self.cursor.executemany(sql, param_list)


class AsyncCursorDebugWrapper(AsyncCursorWrapper):

    async def execute(self, sql, params=None):
        with self.debug_sql(sql, params, use_last_executed_query=True):
            return await super().execute(sql, params)

    async def executemany(self, sql, param_list):
        with self.debug_sql(sql, param_list, many=True):
            return await super().executemany(sql, param_list)

    @contextmanager
    def debug_sql(
        self, sql=None, params=None, use_last_executed_query=False, many=False
    ):
        start = time.monotonic()
        try:
            yield
        finally:
            stop = time.monotonic()
            duration = stop - start
            if use_last_executed_query:
                sql = self.db.ops.last_executed_query(self.cursor, sql, params)
            try:
                times = len(params) if many else ""
            except TypeError:
                # params could be an iterator.
                times = "?"
            self.db.queries_log.append(
                {
                    "sql": "%s times: %s" % (times, sql) if many else sql,
                    "time": "%.3f" % duration,
                }
            )
            logger.debug(
                "(%.3f) %s; args=%s; alias=%s",
                duration,
                sql,
                params,
                self.db.alias,
                extra={
                    "duration": duration,
                    "sql": sql,
                    "params": params,
                    "alias": self.db.alias,
                },
            )
