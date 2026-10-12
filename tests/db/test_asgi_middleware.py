import asyncio
import sys

from asgiref.testing import ApplicationCommunicator
from django.core.asgi import get_asgi_application
from django.core.signals import got_request_exception
from django.http import (
    HttpResponse,
    StreamingHttpResponse,
)
from django.test import AsyncRequestFactory
from django.urls import path
from test_app.models import TestModel

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncioTransactionTestCase,
    override_settings,
)


async def index(request):
    await TestModel.async_objects.acount()
    return HttpResponse("ok")


async def streaming(request):
    async def body():
        yield str(await TestModel.async_objects.acount()).encode()
        body_connections.append(async_connections["default"])

    return StreamingHttpResponse(body())


body_connections = []


async def streaming_two_chunks(request):
    async def body():
        await TestModel.async_objects.acount()
        body_connections.append(async_connections["default"])
        yield b"first"
        yield b"second"

    return StreamingHttpResponse(body())


urlpatterns = [
    path("", index),
    path("streaming", streaming),
    path("streaming-two-chunks", streaming_two_chunks),
]


@override_settings(
    ROOT_URLCONF=__name__,
    ALLOWED_HOSTS=["testserver"],
    MIDDLEWARE=["django_async_backend.middleware.close_async_connections"],
)
class ASGIMiddlewareTaskOwnershipTest(
    # Not AsyncioTestCase: that wraps each test in an atomic block per alias,
    # and this test closes and drops those connections so the request opens
    # its own, which leaves _close_transaction() with no atomic block to exit.
    AsyncioTransactionTestCase
):
    async_request_factory = AsyncRequestFactory()

    async def _drop_test_owned_connections(self):
        for connection in async_connections.all():
            await connection.close()
            del async_connections[connection.alias]

    async def test_request_does_not_trip_task_ownership(self):
        await self._drop_test_owned_connections()

        captured = []

        def record_exception(sender, **kwargs):
            captured.append(sys.exc_info()[1])

        got_request_exception.connect(record_exception)
        self.addCleanup(got_request_exception.disconnect, record_exception)

        scope = self.async_request_factory._base_scope(path="/")
        communicator = ApplicationCommunicator(get_asgi_application(), scope)
        await communicator.send_input({"type": "http.request", "body": b""})

        start = await communicator.receive_output(timeout=5)
        self.assertEqual(start["type"], "http.response.start")

        body = await communicator.receive_output(timeout=5)
        self.assertEqual(body["type"], "http.response.body")

        self.assertEqual(
            start["status"],
            200,
            "request failed with: %r" % (captured,),
        )
        self.assertEqual(body["body"], b"ok")

        # The middleware's post-response close runs after the body; it must
        # not blow up either.
        await communicator.wait(timeout=5)

    async def test_streaming_body_connection_is_released(self):
        """The body runs after the middleware has returned; a query in it
        must not leave its connection open."""
        await self._drop_test_owned_connections()
        body_connections.clear()

        scope = self.async_request_factory._base_scope(path="/streaming")
        communicator = ApplicationCommunicator(get_asgi_application(), scope)
        await communicator.send_input({"type": "http.request", "body": b""})

        start = await communicator.receive_output(timeout=5)
        self.assertEqual(start["status"], 200)

        chunks = []
        while True:
            message = await communicator.receive_output(timeout=5)
            chunks.append(message.get("body", b""))
            if not message.get("more_body"):
                break
        await communicator.wait(timeout=5)

        self.assertEqual(b"".join(chunks), b"0")
        self.assertEqual(len(body_connections), 1)
        self.assertIsNone(body_connections[0].connection)

    async def test_disconnect_while_send_blocks_releases_connection(self):
        """Django only aclose()s its own __aiter__, so a disconnect while
        send() is blocked leaves the body generators suspended. The
        connection must still be released via response.close()."""
        await self._drop_test_owned_connections()
        body_connections.clear()

        inbox = asyncio.Queue()
        await inbox.put({"type": "http.request", "body": b""})
        first_chunk_sent = asyncio.Event()

        async def receive():
            return await inbox.get()

        async def send(message):
            if message.get("body") == b"first":
                first_chunk_sent.set()
                # A slow client: block until the disconnect cancels us.
                await asyncio.Event().wait()

        scope = self.async_request_factory._base_scope(
            path="/streaming-two-chunks"
        )
        request = asyncio.create_task(
            get_asgi_application()(scope, receive, send)
        )
        await asyncio.wait_for(first_chunk_sent.wait(), timeout=5)
        await inbox.put({"type": "http.disconnect"})
        await asyncio.wait_for(request, timeout=5)

        self.assertEqual(len(body_connections), 1)
        self.assertIsNone(body_connections[0].connection)
