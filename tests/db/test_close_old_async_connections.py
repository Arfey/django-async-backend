from unittest.mock import AsyncMock, patch

from django.test import SimpleTestCase

from django_async_backend.db import (
    async_connections,
    close_old_async_connections,
)


class CloseOldAsyncConnectionsTest(SimpleTestCase):
    async def test_closes_every_configured_alias(self):
        first = AsyncMock()
        first.alias = "first"
        second = AsyncMock()
        second.alias = "second"

        with patch.object(
            async_connections, "all", return_value=[first, second]
        ) as mocked_all:
            await close_old_async_connections()

        mocked_all.assert_called_once_with()
        first.close.assert_awaited_once_with()
        second.close.assert_awaited_once_with()

    async def test_accepts_signal_kwargs(self):
        """Connected as a request_started/request_finished signal handler
        in some deployments — must accept arbitrary kwargs."""
        with patch.object(async_connections, "all", return_value=[]):
            await close_old_async_connections(
                sender=object(), signal=object(), environ={}
            )
