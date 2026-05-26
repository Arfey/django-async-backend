from unittest.mock import AsyncMock, patch

from django.test import SimpleTestCase

from django_async_backend.db import (
    async_connections,
    close_old_async_connections,
)


class CloseOldAsyncConnectionsTest(SimpleTestCase):
    async def test_closes_every_configured_alias_even_when_uninitialized(self):
        """`all()` (not initialized_only=True) is required so wrappers
        created inside an asyncio.gather child's contextvar copy still get
        their close() invoked — those wrappers don't appear in the parent
        task's initialized list."""
        first = AsyncMock()
        first.alias = "first"
        second = AsyncMock()
        second.alias = "second"

        with patch.object(
            async_connections, "all", return_value=[first, second]
        ) as mocked_all:
            await close_old_async_connections()

        mocked_all.assert_called_once_with()
        _, kwargs = mocked_all.call_args
        self.assertNotIn("initialized_only", kwargs)
        first.close.assert_awaited_once_with()
        second.close.assert_awaited_once_with()

    async def test_accepts_signal_kwargs(self):
        """Connected as a request_started/request_finished signal handler
        in some deployments — must accept arbitrary kwargs."""
        with patch.object(async_connections, "all", return_value=[]):
            await close_old_async_connections(
                sender=object(), signal=object(), environ={}
            )
