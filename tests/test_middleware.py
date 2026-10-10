import asyncio
from unittest.mock import patch

from django.test import SimpleTestCase

from django_async_backend.middleware import close_async_connections


class CloseAsyncConnectionsMiddlewareTest(SimpleTestCase):
    async def test_closes_connections_before_and_after_response(self):
        """Mirrors the request_started + request_finished signal pair so
        the request runs against a freshly empty connection set and any
        stragglers it leaves behind get returned to the pool."""
        order = []

        async def get_response(_request):
            order.append("response")
            return "response"

        async def tracking_close(**kwargs):
            order.append("close")

        middleware = close_async_connections(get_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=tracking_close,
        ):
            result = await middleware("request")

        self.assertEqual(result, "response")
        self.assertEqual(order, ["close", "response", "close"])

    async def test_closes_connections_when_get_response_raises(self):
        order = []

        async def get_response(_request):
            order.append("response")
            raise RuntimeError("boom")

        async def tracking_close(**kwargs):
            order.append("close")

        middleware = close_async_connections(get_response)

        with patch(
            "django_async_backend.middleware.close_old_async_connections",
            new=tracking_close,
        ):
            with self.assertRaisesMessage(RuntimeError, "boom"):
                await middleware("request")

        # Pre-close, response (which raises), then finally-close.
        self.assertEqual(order, ["close", "response", "close"])

    async def test_close_is_shielded_from_cancellation(self):
        """If the outer task is cancelled mid-response, the post-response
        cleanup must still run to completion so pool connections don't
        strand. Pre-response close is intentionally NOT shielded — a
        cancellation that early just aborts the request."""
        in_response = asyncio.Event()
        cleanup_done = asyncio.Event()

        async def close(**kwargs):
            if not in_response.is_set():
                # Pre-response close — fast path, nothing to assert.
                return
            # Finally close — yield once so the outer task can deliver
            # the cancellation, then prove we still complete.
            await asyncio.sleep(0)
            cleanup_done.set()

        async def slow_response(_request):
            in_response.set()
            await asyncio.sleep(1)  # cancelled before this finishes
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
            await asyncio.wait_for(cleanup_done.wait(), timeout=1)

        self.assertTrue(
            cleanup_done.is_set(),
            "close_old_async_connections must finish even when the request "
            "task is cancelled (asyncio.shield).",
        )
