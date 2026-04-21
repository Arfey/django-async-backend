from django.db import DEFAULT_DB_ALIAS
from django.test import TestCase

from django_async_backend.db.utils import AsyncConnectionHandler


class AsyncConnectionHandlerSyncTests(TestCase):
    def test_cannot_create_async_connection_without_running_event_loop(self):
        with self.assertRaises(RuntimeError) as cm:
            AsyncConnectionHandler()[DEFAULT_DB_ALIAS]

        self.assertEqual(
            str(cm.exception),
            "Cannot create an async connection without a running event loop.",
        )
