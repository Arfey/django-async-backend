"""Run the ASGI gauntlet across every supported connection setting.

Each arm boots a real ASGI server against a real Postgres and asserts the
invariants that matter at a unit-of-work boundary: a request starts with
an empty connection store, the connection it opens is handed back, and
this run's backend count returns to where it began.

The library's own test suite cannot cover this. AsyncioTransactionTestCase
re-stamps task ownership of every alias before each test method, and no
test goes through a real server, so bugs that live in the request
lifecycle are invisible to it.

Usage::

    python gauntlet/run.py
    python gauntlet/run.py --lib /path/to/a/checkout --only pooled

``--lib`` puts a checkout of ``django_async_backend`` first on the app's
path, so the same gauntlet can be pointed at a branch or at a mutant to
check that it still detects a regression.

Postgres is read from the usual PG* environment variables, defaulting to
the one in docker-compose.yml.
"""

import argparse
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

HERE = pathlib.Path(__file__).parent

# Django's documentation says persistent connections should be disabled
# under ASGI in favour of the backend's own pooling, and this library
# requires ASGI. The conn_max_age arm is therefore a negative one: it
# asserts the setting stays inert, because honouring it would leak a
# backend per request.
ARMS = [
    ("pooled", {"POOL_MAX": "3"}),
    ("unpooled", {}),
    ("multi-alias", {"EXTRA_ALIASES": "1"}),
    ("conn_max_age (unsupported)", {"CONN_MAX_AGE": "60"}),
    ("pool + conn_max_age", {"POOL_MAX": "3", "CONN_MAX_AGE": "60"}),
]

# Arms whose configuration the library is expected to reject. The pool
# property raises lazily, so the server starts and every request fails.
MUST_REFUSE = {"pool + conn_max_age": "ImproperlyConfigured"}

# Arms may be added out of tree for a driver that lives elsewhere; see
# arms_local.py.example.
try:
    import arms_local

    ARMS += arms_local.ARMS
    MUST_REFUSE.update(getattr(arms_local, "MUST_REFUSE", {}))
except ImportError:
    pass

BOOT_TIMEOUT = 30.0
PROBE_TIMEOUT = 2.0
REQUEST_TIMEOUT = 20.0

_base = None


class Failure(Exception):
    """An invariant this gauntlet exists to protect was broken."""


def emit(line):
    sys.stdout.write(f"{line}\n")
    sys.stdout.flush()


def get(path, timeout=REQUEST_TIMEOUT):
    url = f"{_base}{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise Failure(f"{path} returned HTTP {exc.code}") from exc


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def start(env, lib, port, nonce, log):
    path = [str(HERE)]
    if lib:
        path.insert(0, str(lib))
    child_env = {
        **os.environ,
        **env,
        "GAUNTLET_NONCE": nonce,
        "PYTHONPATH": os.pathsep.join(path),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    # A file, not a pipe: a server that writes more than the pipe holds
    # would block forever, which reads as a hang rather than a failure.
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "app:application",
        ],
        cwd=HERE,
        env=child_env,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_until_listening(process, port):
    """Wait for the server to accept connections.

    Readiness is a TCP connect rather than a request, because a bug in
    the release path fails *every* request -- request_started runs before
    the view, so even an endpoint that touches no database returns 500.
    Probing with HTTP would report a server that started perfectly well
    as never having booted.
    """
    deadline = time.monotonic() + BOOT_TIMEOUT
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        try:
            with socket.create_connection(
                ("127.0.0.1", port), timeout=PROBE_TIMEOUT
            ):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def ping_when_ready(seconds=8.0):
    """The first successful /ping, or the last thing that went wrong.

    A connectable port does not mean the app has finished importing, so
    a few failures here are ordinary. Persistent ones are the answer.
    """
    deadline = time.monotonic() + seconds
    problem = None
    while time.monotonic() < deadline:
        try:
            return get("/ping", timeout=PROBE_TIMEOUT), None
        except (Failure, urllib.error.URLError, OSError) as exc:
            problem = exc
            time.sleep(0.25)
    return None, problem


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def tail(log_path, lines=6):
    text = log_path.read_text(errors="replace").strip().splitlines()
    return " | ".join(line[:120] for line in text[-lines:]) or "no output"


def count_backends():
    body = get("/backends")
    if "backends" not in body:
        detail = f"{body.get('error')} {body.get('detail', '')}".strip()
        raise Failure(f"cannot count backends: {detail}")
    return body["backends"]


def settle_backends(timeout=8.0):
    """Sample until two reads agree.

    A pool fills its minimum size in the background, so the first read
    after boot can be one short of the resting count.
    """
    previous = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = count_backends()
        if current == previous:
            return current
        previous = current
        time.sleep(0.25)
    return previous


def check(condition, message):
    if not condition:
        raise Failure(message)


def scenario_sequential(findings, env):
    """Ordinary requests: clean slate in, connection back out.

    The number of distinct backends also says whether pooling is doing
    anything: reusing them when a pool is configured, and not when the
    connection is genuinely closed each request.
    """
    pids = set()
    for i in range(1, 6):
        body = get("/")
        check(body["ok"], f"request {i} failed: {body.get('detail')}")
        check(
            body["store_on_entry"] == [],
            f"request {i} began with {body['store_on_entry']} in the store",
        )
        pids.add(body["pid"])
    findings["backend ids over 5 requests"] = len(pids)
    if env.get("POOL_MAX"):
        check(
            len(pids) < 5,
            f"pooling reused nothing: {len(pids)} backends for 5 requests",
        )
    else:
        check(
            len(pids) > 1,
            "every request got the same backend with no pool configured",
        )


def scenario_signals(findings, env):
    """Each half of the wiring in apps.py, asserted separately."""
    for which in ("started", "finished"):
        body = get(f"/signal/{which}")
        check(body["ok"], f"request_{which} raised: {body.get('detail')}")
        check(
            body["store_after"] == [],
            f"request_{which} left {body['store_after']} in the store",
        )
        check(
            body["connection_closed"],
            f"request_{which} did not hand the connection back",
        )


def scenario_fan_out_bare(findings, env):
    """A child task may not use the connection its parent opened."""
    children = get("/fanout/bare")["children"]
    findings["bare fan-out"] = ",".join(children)
    check(
        "ok" not in children,
        f"a child task shared its parent's connection: {children}",
    )


def scenario_fan_out_opted_in(findings, env):
    """Fan-out through async_new_connection has to work."""
    children = get("/fanout/opted-in")["children"]
    check(children == ["ok", "ok"], f"opted-in fan-out gave {children}")


def scenario_transaction(findings, env):
    """A transaction must not poison the request that follows it."""
    body = get("/transaction")
    check(body["ok"], f"transaction failed: {body.get('detail')}")
    body = get("/")
    check(
        body["store_on_entry"] == [],
        f"request after a transaction began with {body['store_on_entry']}",
    )
    check(body["ok"], f"request after a transaction failed: {body!r}")


def scenario_boundary(findings, env):
    """After a release, the next task in the same context must be able to
    open a connection of its own -- the case a worker or sub-app hits."""
    body = get("/boundary")
    check(
        body["store_after_release"] == [],
        f"release left {body['store_after_release']} in the store",
    )
    detail = f"{body.get('error')} {body.get('detail', '')}".strip()
    check(body["ok"], f"the next task could not query: {detail}")


def scenario_atomic_boundary(findings, env):
    """Releasing inside a transaction must fail loudly afterwards, not
    quietly continue on a connection outside it."""
    body = get("/atomic-boundary")
    check(
        body.get("same_wrapper") is True,
        "a release inside a transaction replaced the connection",
    )
    check(
        body.get("query_after_release_succeeded") is False,
        "a query inside the block succeeded after the release, so the "
        "rest of the transaction ran on another connection",
    )


def scenario_two_aliases(findings, env):
    """Every open async alias is released, and a non-async one is left
    alone rather than being built."""
    if env.get("EXTRA_ALIASES") != "1":
        return
    body = get("/two-aliases")
    check(body["ok"], f"releasing two aliases raised: {body.get('detail')}")
    check(
        body["store_before"] == ["default", "other"],
        f"expected both aliases open, got {body['store_before']}",
    )
    check(
        body["store_after"] == [],
        f"release left {body['store_after']} in the store",
    )


SCENARIOS = [
    ("sequential", scenario_sequential),
    ("request signals", scenario_signals),
    ("bare fan-out", scenario_fan_out_bare),
    ("opted-in fan-out", scenario_fan_out_opted_in),
    ("transaction", scenario_transaction),
    ("non-HTTP boundary", scenario_boundary),
    ("atomic boundary", scenario_atomic_boundary),
    ("two aliases", scenario_two_aliases),
]


def identify(nonce, port, lib, findings, log_path):
    """Prove we are talking to our own child, on the library we chose.

    A stray server on the port, or a --lib that silently did not take,
    would otherwise be tested instead and could report a false pass.
    """
    hello, problem = ping_when_ready()
    if hello is None:
        # Listening but not answering: a broken request path, not a
        # server that failed to start.
        return "FAILED", {"ping": str(problem), "log": tail(log_path, 3)}
    if hello.get("nonce") != nonce:
        return "FAILED", {"port": f"{port} answered by another server"}
    if lib and str(lib.resolve()) not in hello.get("library", ""):
        return "FAILED", {"library": hello.get("library")}
    findings["aliases"] = len(hello.get("aliases", []))
    return None


def refusal(expected):
    body = get("/")
    if body.get("error") == expected:
        return "expected refusal", {"raised": expected}
    return "FAILED", {"expected": expected, "got": repr(body)[:110]}


def run_scenarios(findings, env):
    for label, scenario in SCENARIOS:
        try:
            scenario(findings, env)
        except Failure as exc:
            return "FAILED", {**findings, label: f"FAILED: {exc}"}
    return None


def run_arm(name, env, lib):
    global _base
    findings = {}
    requires = env.pop("REQUIRES", None)
    if requires:
        probe = subprocess.run(
            [sys.executable, "-c", f"import {requires}"],
            capture_output=True,
            check=False,
        )
        if probe.returncode != 0:
            return "skipped", {"missing": requires}

    nonce = f"gauntlet-{uuid.uuid4().hex[:12]}"
    port = free_port()
    _base = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory() as workspace:
        log_path = pathlib.Path(workspace) / "server.log"
        with log_path.open("w") as log:
            process = start(env, lib, port, nonce, log)
        try:
            if not wait_until_listening(process, port):
                return "FAILED TO BOOT", {"log": tail(log_path)}

            wrong = identify(nonce, port, lib, findings, log_path)
            if wrong:
                return wrong

            expected = MUST_REFUSE.get(name)
            if expected:
                return refusal(expected)

            baseline = settle_backends()
            broken = run_scenarios(findings, env)
            if broken:
                return broken

            after = settle_backends()
            findings["backends"] = f"{baseline} -> {after}"
            if after > baseline:
                return "LEAK", findings
            return "ok", findings
        finally:
            stop(process)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--lib",
        type=pathlib.Path,
        default=None,
        help="checkout of django_async_backend to test instead of the "
        "installed one",
    )
    parser.add_argument(
        "--only", default=None, help="run only arms matching this substring"
    )
    args = parser.parse_args()

    if args.lib:
        package = args.lib / "django_async_backend"
        if not package.is_dir():
            emit(f"--lib has no django_async_backend directory: {args.lib}")
            return 2
        emit(f"library under test: {args.lib.resolve()}")

    selected = [
        (name, env) for name, env in ARMS if not args.only or args.only in name
    ]
    if not selected:
        emit(
            f"--only {args.only!r} matched none of: "
            + ", ".join(name for name, _ in ARMS)
        )
        return 2

    failed = 0
    for name, env in selected:
        try:
            verdict, findings = run_arm(name, dict(env), args.lib)
        except Failure as exc:
            verdict, findings = "FAILED", {"error": str(exc)}
        except Exception as exc:  # noqa: BLE001 - report, never crash
            verdict, findings = "ERROR", {type(exc).__name__: str(exc)[:140]}
        detail = "; ".join(f"{k}: {v}" for k, v in findings.items())
        emit(f"{verdict:>18}  {name:<28} {detail}")
        if verdict not in ("ok", "expected refusal", "skipped"):
            failed = 1
    return failed


if __name__ == "__main__":
    sys.exit(main())
