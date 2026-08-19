# AGENTS.md

Guidance for AI coding agents working in this repository. The
[README](./README.md) is the source of truth; this file just calls out the
things that are easy to get wrong.

## Remember this

- **Async does not mean parallel.** Database queries are not run in parallel by
  default. A task gets one connection per alias and every ORM and cursor call
  in that task takes turns on it, so awaiting queries in sequence costs the sum
  of their times. Fanning them out does not help either: `asyncio.gather()` and
  friends run each coroutine in its own task, which does not own the
  connection, so those calls raise `RuntimeError`. To run queries in parallel
  you must opt in explicitly with `async_new_connection`, which opens a real
  connection per call — use it sparingly, since a wide fan-out can exhaust the
  server's connection limit. Do not assume "django-async-backend" parallelizes
  queries for you.

- **Part of the ORM is generated, not hand-written.** Some ORM modules are
  produced from Django's source by the `codemon` tool and committed to git, so
  they look hand-written but are not. Each carries a `# This file was generated
  automatically. Do not modify it manually.` header. Do not hand-edit those
  files — your changes will be lost on the next regeneration. Edit the config
  under `codemon/config/*.yaml` and run `lets generate` (not a bare
  `python -m codemon`) instead. See the README "Code generation" section.

- **Generated code is committed.** Because the generated modules are checked in,
  a diff can look like ordinary handwritten code. If you touch the ORM layer,
  check whether the file is generated before editing it.
