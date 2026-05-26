import asyncio
from unittest.mock import AsyncMock, patch

from django.test import SimpleTestCase

from django_async_backend.middleware import close_async_connections


class CloseAsyncConnectionsMiddlewareTest(SimpleTestCase):
    async def test_closes_connections_before_and_after_response(self):
        """Mirrors the request_started + request_finished signal pair so
        the request runs against a freshly empty connection set and any
        stragglers it leaves behind get returned to the pool."""
        order = []
        get_response = AsyncMock(
            side_effect=lambda req: order.append("response") or "response"
        )
        middleware = close_async_connections(get_response)

        async def tracking_close(**kwargs):
            order.append("close")

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=tracking_close,
        ):
            result = await middleware("request")

        self.assertEqual(result, "response")
        get_response.assert_awaited_once_with("request")
        self.assertEqual(order, ["close", "response", "close"])

    async def test_closes_connections_when_get_response_raises(self):
        get_response = AsyncMock(side_effect=RuntimeError("boom"))
        middleware = close_async_connections(get_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=AsyncMock(),
        ) as mocked_close:
            with self.assertRaisesMessage(RuntimeError, "boom"):
                await middleware("request")

        # Once before get_response, once in the finally after it raised.
        self.assertEqual(mocked_close.await_count, 2)

    async def test_close_is_shielded_from_cancellation(self):
        """If the outer task is cancelled mid-response, the post-response
        cleanup must still run to completion so pool connections don't
        strand. Pre-response close is intentionally NOT shielded — a
        cancellation that early just aborts the request."""
        post_response_cleanup_count = 0
        in_response = asyncio.Event()

        async def close(**kwargs):
            if in_response.is_set():
                # Running from the finally — must complete despite cancel.
                await asyncio.sleep(0.05)
                nonlocal post_response_cleanup_count
                post_response_cleanup_count += 1

        async def slow_response(_request):
            in_response.set()
            await asyncio.sleep(0.05)
            return "response"

        middleware = close_async_connections(slow_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=close,
        ):
            task = asyncio.create_task(middleware("request"))
            await in_response.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            # Give the shielded background coroutine a chance to finish.
            await asyncio.sleep(0.1)

        self.assertEqual(
            post_response_cleanup_count,
            1,
            "close_old_async_connections must finish even when the request "
            "task is cancelled (asyncio.shield).",
        )
