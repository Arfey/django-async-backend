# AGENTS.md

Guidance for AI coding agents working in this repository. The
[README](./README.md) is the source of truth; this file just calls out the
things that are easy to get wrong.

## Remember this

- **Async does not mean parallel.** Database queries are not run in parallel by
  default. All async ORM and cursor calls within an async context share one
  connection per alias, so concurrent queries — even under `asyncio.gather()` —
  are serialized on that connection. To run queries in parallel you must opt in
  explicitly with `async_connections._independent_connection()` (a concept, not
  production-ready). Do not assume "django-async-backend" parallelizes queries
  for you. See the README "Concurrency model" and "DEP 0009" sections.

- **Part of the ORM is generated, not hand-written.** Some ORM modules are
  produced from Django's source by the `codemon` tool and committed to git, so
  they look hand-written but are not. Each carries a `# This file was generated
  automatically. Do not modify it manually.` header. Do not hand-edit those
  files — your changes will be lost on the next regeneration. Edit the config
  under `codemon/config/*.yaml` and run `lets test_generate` (not a bare
  `python -m codemon`) instead. See the README "Code generation" section.

- **Generated code is committed.** Because the generated modules are checked in,
  a diff can look like ordinary handwritten code. If you touch the ORM layer,
  check whether the file is generated before editing it.
