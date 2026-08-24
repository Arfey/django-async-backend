from django.db import DEFAULT_DB_ALIAS
from django.test import TestCase

from django_async_backend.db.utils import AsyncConnectionHandler
from django_async_backend.test import AsyncioTransactionTestCase


def mixed_handler():
    """A handler with an async alias next to a sync-only sqlite one."""
    return AsyncConnectionHandler(
        {
            DEFAULT_DB_ALIAS: {
                "ENGINE": "django_async_backend.db.backends.postgresql",
                "NAME": "postgres",
                "USER": "postgres",
                "PASSWORD": "postgres",
                "HOST": "localhost",
                "PORT": 5432,
            },
            "legacy": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            },
        }
    )


class AsyncConnectionHandlerSyncTests(TestCase):
    def test_cannot_create_async_connection_without_running_event_loop(self):
        with self.assertRaises(RuntimeError) as cm:
            AsyncConnectionHandler()[DEFAULT_DB_ALIAS]

        self.assertEqual(
            str(cm.exception),
            "Cannot create an async connection without a running event loop.",
        )


class AsyncConnectionHandlerAllTests(AsyncioTransactionTestCase):

    async def test_all_skips_sync_only_aliases(self):
        handler = mixed_handler()

        self.assertEqual(
            [conn.alias for conn in handler.all()], [DEFAULT_DB_ALIAS]
        )

    async def test_getitem_still_raises_for_sync_only_alias(self):
        handler = mixed_handler()

        with self.assertRaises(handler.exception_class) as cm:
            handler["legacy"]

        self.assertEqual(
            str(cm.exception), "The async connection 'legacy' doesn't exist."
        )

    async def test_all_initialized_only_skips_untouched_aliases(self):
        handler = mixed_handler()

        self.assertEqual(handler.all(initialized_only=True), [])

        handler[DEFAULT_DB_ALIAS]

        self.assertEqual(
            [conn.alias for conn in handler.all(initialized_only=True)],
            [DEFAULT_DB_ALIAS],
        )
