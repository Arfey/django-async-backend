from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from asgiref.sync import sync_to_async
from django.core import signals
from django.db import (
    DEFAULT_DB_ALIAS,
    DatabaseError,
)

from django_async_backend.db import (
    async_connections,
    close_old_async_connections,
)
from django_async_backend.db.transaction import async_atomic


def stored_aliases():
    """Aliases the current context actually holds a wrapper for."""
    return sorted(
        conn.alias for conn in async_connections.all(initialized_only=True)
    )


class ReleaseTestCase(IsolatedAsyncioTestCase):
    databases = "__all__"

    async def asyncSetUp(self):
        await self.drain()

    async def asyncTearDown(self):
        await self.drain()

    async def drain(self):
        """Empty the store without going through the code under test."""
        for conn in async_connections.all(initialized_only=True):
            del async_connections[conn.alias]
            try:
                await conn.close()
            except Exception:
                # A test may have deliberately broken this connection.
                pass


class CloseOldAsyncConnectionsTest(ReleaseTestCase):
    async def test_releases_the_aliases_this_context_opened(self):
        first = async_connections[DEFAULT_DB_ALIAS]
        second = async_connections["other"]
        self.assertEqual(stored_aliases(), ["default", "other"])

        await close_old_async_connections()

        self.assertEqual(stored_aliases(), [])
        # Dropped, not merely closed: the next access builds a new wrapper
        # owned by whichever task asks for it, rather than handing back one
        # owned by a task that has moved on.
        self.assertIsNot(async_connections[DEFAULT_DB_ALIAS], first)
        self.assertIsNot(async_connections["other"], second)

    async def test_does_not_open_aliases_this_context_never_used(self):
        create_connection = async_connections.create_connection
        opened = []

        def spy(alias):
            opened.append(alias)
            return create_connection(alias)

        with patch.object(async_connections, "create_connection", spy):
            await close_old_async_connections()

        self.assertEqual(opened, [])

    async def test_ignores_an_alias_that_is_not_an_async_backend(self):
        # A project adopting this library can have an ordinary Django
        # alias alongside an async one. Building a wrapper for it raises,
        # so an untouched alias must not be built.
        async_connections.settings["legacy"] = dict(
            async_connections.settings[DEFAULT_DB_ALIAS],
            ENGINE="django.db.backends.postgresql",
        )
        self.addCleanup(async_connections.settings.pop, "legacy")
        async_connections[DEFAULT_DB_ALIAS]

        await close_old_async_connections()

        self.assertEqual(stored_aliases(), [])

    async def test_closes_the_underlying_connection(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        async with await connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
        self.assertIsNotNone(connection.connection)

        await close_old_async_connections()

        self.assertIsNone(connection.connection)

    async def test_closes_every_alias_even_when_one_fails(self):
        failing = async_connections[DEFAULT_DB_ALIAS]
        healthy = async_connections["other"]
        await healthy.ensure_connection()

        async def close():
            raise DatabaseError("cannot hand this one back")

        failing.close = close

        with self.assertRaises(DatabaseError):
            await close_old_async_connections()

        # The healthy alias is released rather than left open and
        # unreachable, and no wrapper is left for the next task.
        self.assertIsNone(healthy.connection)
        self.assertEqual(stored_aliases(), [])

    async def test_keeps_a_wrapper_that_is_inside_a_transaction(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        reached_the_assertions = False

        try:
            async with async_atomic():
                await close_old_async_connections()

                # The tombstone stays, so the rest of the block cannot
                # silently carry on against a fresh connection outside
                # its transaction.
                self.assertIs(async_connections[DEFAULT_DB_ALIAS], connection)
                self.assertTrue(connection.closed_in_transaction)
                reached_the_assertions = True
        except DatabaseError:
            # Leaving the block reports the broken transaction. That is
            # the point: the failure is loud.
            pass

        self.assertTrue(reached_the_assertions)

    async def test_accepts_signal_kwargs(self):
        """Connected as a request_started / request_finished receiver, so
        it has to tolerate whatever the sender passes."""
        await async_connections[DEFAULT_DB_ALIAS].ensure_connection()

        await close_old_async_connections(
            sender=object(), signal=object(), scope={}
        )

        self.assertEqual(stored_aliases(), [])


class RequestSignalsTest(ReleaseTestCase):
    """Release through the signals the app connects on startup.

    These deliberately avoid AsyncioTransactionTestCase, which re-stamps
    ownership of every alias before each test method -- the very state
    under test here.
    """

    def test_the_app_connects_the_receiver(self):
        for signal in (signals.request_started, signals.request_finished):
            with self.subTest(signal=signal):
                _, async_receivers = signal._live_receivers(None)
                self.assertIn(close_old_async_connections, async_receivers)

    async def test_request_started_releases_a_stale_connection(self):
        stale = async_connections[DEFAULT_DB_ALIAS]
        await stale.ensure_connection()

        await signals.request_started.asend(sender=None, scope={})

        self.assertEqual(stored_aliases(), [])
        self.assertIsNone(stale.connection)

    async def test_request_started_opens_nothing_on_an_empty_store(self):
        await signals.request_started.asend(sender=None, scope={})

        self.assertEqual(stored_aliases(), [])

    async def test_request_started_leaves_no_foreign_connection(self):
        await signals.request_started.asend(sender=None, scope={})

        async_connections[DEFAULT_DB_ALIAS].validate_task_sharing()

    async def test_a_request_can_query_after_request_started(self):
        await signals.request_started.asend(sender=None, scope={})

        connection = async_connections[DEFAULT_DB_ALIAS]
        async with await connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
            self.assertEqual(await cursor.fetchone(), (1,))

    async def test_request_finished_releases_what_the_request_opened(self):
        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.ensure_connection()
        self.assertEqual(stored_aliases(), [DEFAULT_DB_ALIAS])

        await signals.request_finished.asend(sender=None)

        self.assertEqual(stored_aliases(), [])
        self.assertIsNone(connection.connection)

    async def test_request_finished_releases_from_a_sync_send(self):
        # ASGIHandler does not asend() request_finished for a normal
        # response: it awaits sync_to_async(response.close), and
        # HttpResponse.close() sends the signal synchronously.
        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.ensure_connection()

        await sync_to_async(signals.request_finished.send)(sender=None)

        self.assertEqual(stored_aliases(), [])
        self.assertIsNone(connection.connection)
