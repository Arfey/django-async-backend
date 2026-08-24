# ASGI gauntlet

A conformance harness for the connection lifecycle. Each arm boots a real
ASGI server against a real Postgres and asserts what has to hold at a
unit-of-work boundary:

- a request starts with an empty connection store;
- the connection a request opens is handed back when it ends;
- this run's backend count returns to where it began;
- both halves of the wiring release — `request_started` and
  `request_finished` are asserted separately;
- a child task may not use the connection its parent opened, and fan-out
  through `async_new_connection()` works;
- releasing while a transaction is open leaves the wrapper in place, so
  the rest of the block fails loudly instead of quietly continuing on a
  connection outside the transaction;
- every open async alias is released, and an ordinary Django alias is
  left alone rather than being built;
- after a release, the next task in the same context can open its own
  connection — the case a worker or a mounted ASGI sub-app hits;
- requests in flight together are served correctly, and a pool queues
  them rather than exceeding its size.

## Why it is not in `tests/`

The Django test suite cannot see this class of bug.
`AsyncioTransactionTestCase` re-stamps task ownership of every alias
before each test method, which is exactly the state a lifecycle bug
corrupts, and no test goes through a real server, so nothing exercises
Django's own request signal dispatch.

## Running it

Start Postgres (`docker compose up postgres -d`), then:

```sh
lets gauntlet
```

or directly:

```sh
poetry run python gauntlet/run.py
```

Connection settings come from the usual `PG*` environment variables and
default to the ones in `docker-compose.yml`. A non-zero exit status means
an invariant broke; each line names which one.

### Latency

Database traffic is delayed by `LATENCY_MS` (2 by default), which widens
the window in which two tasks can race for the same connection — where
the bugs in this layer live. `LATENCY_MS=0` connects straight to
Postgres.

The delay comes from a small TCP proxy (`latency.py`), not from
`tc`/netem: netem needs `NET_ADMIN`, and on loopback it would delay the
driver's own HTTP as much as the database traffic. The proxy delays only
what the application exchanges with Postgres, and needs no privileges.

To point the gauntlet at a checkout other than the installed package —
useful for confirming it still detects a regression you have a fix for:

```sh
poetry run python gauntlet/run.py --lib ../some-other-checkout
poetry run python gauntlet/run.py --only pooled
```

## Arms

| arm | what it covers |
| --- | --- |
| `pooled` | `OPTIONS["pool"]`, the recommended ASGI configuration. Asserts backends are actually reused |
| `unpooled` | no pool: asserts a real connect and disconnect per request |
| `multi-alias` | two async aliases plus an ordinary Django one |
| `conn_max_age (unsupported)` | asserts `CONN_MAX_AGE` stays inert. Django's docs say persistent connections should be disabled under ASGI in favour of pooling, and honouring the setting here would leak a backend per request |
| `pool + conn_max_age` | asserts the combination is still rejected |

## Adding an arm out of tree

A driver that lives outside this repository can be added without touching
the committed matrix. Copy `arms_local.py.example` to `arms_local.py`
(untracked) and list the arm there; it is skipped automatically when the
driver is not importable.

## How it avoids lying to you

A harness that passes while the library is broken is worse than none, so
a few things are deliberate:

- **Backends are counted by `application_name`, not by database.** Each
  run stamps its connections with a unique name and counts only those,
  so another client on the same Postgres cannot mask a leak or invent
  one.
- **Each arm gets an ephemeral port and a nonce.** `/ping` echoes the
  nonce and the path the server imported `django_async_backend` from, and
  the driver refuses to continue if either is not its own. Otherwise a
  stray server left on a fixed port would be tested instead.
- **The server's output goes to a file, not a pipe.** A server that
  writes more than a pipe holds would block forever, which reads as a
  hang rather than a failure.
- **The baseline is sampled until two reads agree**, and only after a
  warm-up burst. A pool grows under load and keeps what it opened, so a
  baseline read at `min_size` would see that growth later and call it a
  leak.
- **`--only` matching nothing, and a `--lib` without a package in it, are
  errors**, not silent successes.
