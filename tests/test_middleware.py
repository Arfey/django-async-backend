import asyncio
from unittest.mock import AsyncMock, patch

from django.test import SimpleTestCase

from django_async_backend.middleware import close_async_connections


class CloseAsyncConnectionsMiddlewareTest(SimpleTestCase):
    async def test_closes_connections_after_response(self):
        get_response = AsyncMock(return_value="response")
        middleware = close_async_connections(get_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=AsyncMock(),
        ) as mocked_close:
            result = await middleware("request")

        self.assertEqual(result, "response")
        get_response.assert_awaited_once_with("request")
        mocked_close.assert_awaited_once_with()

    async def test_closes_connections_when_get_response_raises(self):
        get_response = AsyncMock(side_effect=RuntimeError("boom"))
        middleware = close_async_connections(get_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=AsyncMock(),
        ) as mocked_close:
            with self.assertRaisesMessage(RuntimeError, "boom"):
                await middleware("request")

        mocked_close.assert_awaited_once_with()

    async def test_close_is_shielded_from_cancellation(self):
        """If the outer task is cancelled mid-response, the cleanup must
        still run to completion so pool connections don't strand."""
        cleanup_finished = False

        async def slow_close(**kwargs):
            nonlocal cleanup_finished
            await asyncio.sleep(0.05)
            cleanup_finished = True

        async def slow_response(_request):
            await asyncio.sleep(0.05)
            return "response"

        middleware = close_async_connections(slow_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=slow_close,
        ):
            task = asyncio.create_task(middleware("request"))
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(
            cleanup_finished,
            "close_old_async_connections must finish even when the request "
            "task is cancelled (asyncio.shield).",
        )
