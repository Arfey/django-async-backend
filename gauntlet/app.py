"""One ASGI app, parameterised by environment, for the gauntlet driver.

Every arm of the matrix runs this same app; only the environment differs.
The views report the invariants the driver asserts on, so a failure names
the broken invariant rather than only a status code.

Connections carry a per-run ``application_name`` so the driver can count
its own server's backends and nothing else. Without that, any other
client on the same database moves the count and a leak hides in the noise.
"""

import asyncio
import json
import os

import django
from django.conf import settings

DEFAULT_ENGINE = "django_async_backend.db.backends.postgresql"

NONCE = os.environ.get("GAUNTLET_NONCE", "gauntlet")
POOL_MAX = int(os.environ.get("POOL_MAX", "0"))
CONN_MAX_AGE = int(os.environ.get("CONN_MAX_AGE", "0"))
EXTRA_ALIASES = os.environ.get("EXTRA_ALIASES") == "1"


def postgres_alias():
    options = {"application_name": NONCE}
    if POOL_MAX:
        options["pool"] = {"min_size": 1, "max_size": POOL_MAX, "timeout": 5}
    return {
        "ENGINE": os.environ.get("ENGINE", DEFAULT_ENGINE),
        "NAME": os.environ.get("PGDATABASE", "postgres"),
        "USER": os.environ.get("PGUSER", "postgres"),
        "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
        "HOST": os.environ.get("PGHOST", "localhost"),
        "PORT": os.environ.get("PGPORT", "5432"),
        "CONN_MAX_AGE": CONN_MAX_AGE,
        "OPTIONS": options,
    }


databases = {"default": postgres_alias()}
if EXTRA_ALIASES:
    # A second async alias, to catch a release that only handles one, and
    # an ordinary Django alias, which has no async wrapper to build at all.
    databases["other"] = postgres_alias()
    databases["legacy"] = {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }

settings.configure(
    SECRET_KEY="gauntlet",
    DEBUG=False,
    ALLOWED_HOSTS=["*"],
    USE_TZ=True,
    INSTALLED_APPS=["django_async_backend"],
    MIDDLEWARE=[],
    ROOT_URLCONF=__name__,
    DATABASES=databases,
    LOGGING={
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {"console": {"class": "logging.StreamHandler"}},
        "root": {"handlers": ["console"], "level": "ERROR"},
    },
)
django.setup()

from django.core import signals  # noqa: E402
from django.core.asgi import get_asgi_application  # noqa: E402
from django.db import DEFAULT_DB_ALIAS  # noqa: E402
from django.http import HttpResponse  # noqa: E402
from django.urls import path  # noqa: E402

import django_async_backend  # noqa: E402
from django_async_backend.db import (  # noqa: E402
    async_connections,
    async_new_connection,
    close_old_async_connections,
)
from django_async_backend.db.transaction import async_atomic  # noqa: E402


def stored():
    """Aliases the current context holds a connection wrapper for."""
    return sorted(
        conn.alias for conn in async_connections.all(initialized_only=True)
    )


async def scalar(sql, params=None, alias=DEFAULT_DB_ALIAS):
    connection = async_connections[alias]
    async with await connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return (await cursor.fetchone())[0]


def reply(report):
    return HttpResponse(json.dumps(report), content_type="application/json")


def attempt(report, key="ok"):
    """Record whether a block succeeded, without letting it 500."""

    class Attempt:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            report[key] = exc is None
            if exc is not None:
                report["error"] = type(exc).__name__
                report["detail"] = str(exc)[:160]
            return True

    return Attempt()


async def one_query(request):
    """The ordinary case: one unit of work, one connection."""
    report = {"store_on_entry": stored()}
    async with attempt(report):
        report["pid"] = await scalar("SELECT pg_backend_pid()")
    return reply(report)


async def fan_out_bare(request):
    """Fan-out with no opt-in, which a child is not allowed to do."""
    report = {"store_on_entry": stored()}
    await scalar("SELECT 1")
    results = await asyncio.gather(
        scalar("SELECT 1"), scalar("SELECT 1"), return_exceptions=True
    )
    report["children"] = [
        type(r).__name__ if isinstance(r, BaseException) else "ok"
        for r in results
    ]
    return reply(report)


async def fan_out_opted_in(request):
    """Fan-out through async_new_connection, which is the way to do it."""

    async def child():
        async with async_new_connection():
            return await scalar("SELECT 1")

    report = {}
    results = await asyncio.gather(child(), child(), return_exceptions=True)
    report["children"] = [
        type(r).__name__ if isinstance(r, BaseException) else "ok"
        for r in results
    ]
    return reply(report)


async def transaction(request):
    """A request whose work happens inside a transaction."""
    report = {"store_on_entry": stored()}
    async with attempt(report):
        async with async_atomic():
            report["pid"] = await scalar("SELECT pg_backend_pid()")
    return reply(report)


async def release_on_signal(request, which):
    """Dispatch one request signal with a connection already open.

    Each half of the wiring in apps.py is asserted separately. A release
    that only runs on one of the two signals leaves this store dirty.
    """
    signal = {
        "started": signals.request_started,
        "finished": signals.request_finished,
    }[which]
    connection = async_connections[DEFAULT_DB_ALIAS]
    await connection.ensure_connection()
    report = {"store_before": stored()}
    async with attempt(report):
        if signal is signals.request_started:
            await signal.asend(sender=None, scope={})
        else:
            await signal.asend(sender=None)
    report["store_after"] = stored()
    report["connection_closed"] = connection.connection is None
    return reply(report)


async def boundary_then_new_task(request):
    """A non-HTTP boundary, where releasing has to do both halves.

    A worker or a mounted ASGI sub-app releases at the end of a unit of
    work and then runs the next one in a task of its own, inside the same
    context. Closing the connection is not enough there: a wrapper left
    in the store belongs to the task that released it, and the next task
    is not allowed to use it.
    """
    await scalar("SELECT 1")
    await close_old_async_connections()
    report = {"store_after_release": stored()}

    async def next_unit_of_work():
        # A worker releases inside its own task, the only context that
        # can see the connection it opened.
        try:
            return await scalar("SELECT pg_backend_pid()")
        finally:
            await close_old_async_connections()

    async with attempt(report):
        report["pid"] = await asyncio.create_task(next_unit_of_work())
    return reply(report)


async def atomic_boundary(request):
    """A release that happens while a transaction is open.

    close() turns such a wrapper into a tombstone that refuses to
    reconnect. The release has to leave it in the store: replacing it
    would put the rest of the block on a fresh connection outside the
    transaction, and the block would report success.
    """
    report = {}
    async with attempt(report, key="block_completed"):
        async with async_atomic():
            connection = async_connections[DEFAULT_DB_ALIAS]
            await connection.ensure_connection()
            await close_old_async_connections()
            report["same_wrapper"] = (
                async_connections[DEFAULT_DB_ALIAS] is connection
            )
            inner = {}
            async with attempt(inner, key="query_succeeded"):
                await scalar("SELECT 1")
            report["query_after_release_succeeded"] = inner["query_succeeded"]
    return reply(report)


async def two_aliases(request):
    """Release with more than one async alias open.

    Also covers an ordinary Django alias being configured: no async
    wrapper can be built for it, so an untouched one must be left alone.
    """
    await scalar("SELECT 1")
    await scalar("SELECT 1", alias="other")
    report = {"store_before": stored()}
    async with attempt(report):
        await close_old_async_connections()
    report["store_after"] = stored()
    return reply(report)


async def signal_started(request):
    return await release_on_signal(request, "started")


async def signal_finished(request):
    return await release_on_signal(request, "finished")


async def ping(request):
    """Liveness, plus proof of which server and which library answered.

    Must not touch the database, so the driver can wait for a server
    whose database configuration is deliberately invalid.
    """
    return reply(
        {
            "nonce": NONCE,
            "library": django_async_backend.__file__,
            "aliases": sorted(settings.DATABASES),
        }
    )


async def backends(request):
    """How many backends this run has open, excluding the one asking.

    Reports a failure rather than raising, so a broken arm gives the
    driver a diagnosis instead of a 500.
    """
    report = {}
    async with attempt(report):
        report["backends"] = await scalar(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE application_name = %s AND pid <> pg_backend_pid()",
            (NONCE,),
        )
    return reply(report)


urlpatterns = [
    path("ping", ping),
    path("backends", backends),
    path("fanout/bare", fan_out_bare),
    path("fanout/opted-in", fan_out_opted_in),
    path("transaction", transaction),
    path("boundary", boundary_then_new_task),
    path("atomic-boundary", atomic_boundary),
    path("two-aliases", two_aliases),
    path("signal/started", signal_started),
    path("signal/finished", signal_finished),
    path("", one_query),
]

application = get_asgi_application()
