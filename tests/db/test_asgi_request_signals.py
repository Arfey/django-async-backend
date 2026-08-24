import sys

from asgiref.testing import ApplicationCommunicator
from django.core.asgi import get_asgi_application
from django.core.signals import got_request_exception
from django.http import HttpResponse
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


urlpatterns = [path("", index)]


@override_settings(ROOT_URLCONF=__name__, ALLOWED_HOSTS=["testserver"])
class ASGIRequestSignalTaskOwnershipTest(
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

        # request_finished is emitted after the body; it must not blow up
        # either.
        await communicator.wait(timeout=5)
