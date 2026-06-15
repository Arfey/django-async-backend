# Proposal (PROTOTYPE): a coherent async save contract

Status: **draft / for discussion.** This branch is a working prototype meant to
make a design decision concrete, not to merge as-is.

## The problem

Vanilla Django's async write path is incoherent across `asave` and `acreate`,
and it can run a user's override **twice**. This is an acknowledged, still-open
bug — Django ticket **#36888** (`QuerySet.acreate()` ignores `asave()`), and the
attempted fix **PR #20602** was *closed* with the conclusion that a correct fix
needs foundational "async model instantiation" infrastructure Django isn't ready
to build. So upstream has no shipping answer today.

### Proven vanilla Django behaviour (Django 6.0.6)

Each model does `self.foo += 1` in whichever method it overrides; start at
`foo == 0`:

| overrides   | `await obj.asave()` | `await Model.objects.acreate()` |
|-------------|---------------------|---------------------------------|
| nothing     | 0                   | 0                               |
| save only   | 1                   | 1                               |
| asave only  | 1                   | **0** ← `asave` silently dropped |
| both        | **2** ← DOUBLE-run  | **1** ← disagrees with `asave`  |

Why:

- `Model.asave` is `await sync_to_async(self.save)(...)` — it routes through
  `self.save`. So `asave`+`save` both-overridden + the idiomatic
  `await super().asave()` runs the `save` body too → **double `+=1`**.
- `acreate` is `sync_to_async(self.create)` → `create()` → `obj.save(...)`. It
  **never mentions `asave`** — so an `asave` override is silently ignored
  (#36888), and `acreate` is really "run the sync create on a thread."

(Reproducer: pure stock Django, in-memory sqlite — see commit history / the
`docs/` discussion. No async-backend involved.)

## The proposed contract

On async paths, **`asave` is the single source of truth**, and
`acreate` = build + `asave` (so the two finally agree):

- override `asave` → run it; its `super().asave()` does the native async write
  and never calls `save` → **no double-run**.
- override `save` only → honour it via `sync_to_async` (identical to vanilla
  Django; correct, not native).
- override neither → native async write.
- override both → run `asave`; `save` is ignored.

### Resulting behaviour (this prototype)

| overrides   | `await obj.asave()` | `await async_object.acreate()` |
|-------------|---------------------|--------------------------------|
| nothing     | 0                   | 0                              |
| save only   | 1                   | 1                              |
| asave only  | 1                   | **1** (was 0 — #36888 fixed)   |
| both        | **1** (no double)   | 1                              |

Coherent `0 / 1 / 1 / 1` for *both* entry points. `acreate` and `asave` agree,
and the double-run is gone.

## What we propose breaking (vs vanilla Django)

1. **`acreate` honours `asave`** (asave-only: `0 → 1`; both: still `1` but now
   via `asave`, not `save`). This is the #36888 fix direction.
2. **No double-run for both-overriders** (`asave`: `2 → 1`). A both-overrider's
   `save` body no longer runs on async paths.
3. **API is unchanged** (`acreate` exists, same signature). Only *semantics*
   change, and only toward coherence.
4. **`save`-only is NOT broken** — graceful degradation runs it via
   `sync_to_async`, exactly as vanilla Django does. So there is **no silent
   drop** of user code in any cell.

One-line contract for users:

> On async paths, `asave` is the source of truth. `acreate`/`asave` run your
> `asave` override if you have one (native), your `save` override (threaded) if
> that's all you wrote, and a native async save otherwise. If you override both,
> the async path uses `asave` and never your `save`.

## Why this is the right shape

- It's strictly better-or-equal to Django in every cell (the only divergences
  fix an open bug and remove a footgun).
- Because we're the ones who'd take this upstream, the coherent implementation
  *is* the upstream argument: "#36888 is open, here's the one rule that fixes it,
  here's it running with tests."

## Prototype scope / limitations (NOT production)

- Single-table models only (no multi-table inheritance).
- No signals (`pre_save`/`post_save`) on the native path yet.
- No m2m, Postgres only.
- Native write is raw SQL via the async cursor; a production version routes
  through the async `SQLInsertCompiler`/`aupdate` and fires async signals.

## Files

- `django_async_backend/db/models/base.py` — `AsyncSaveModel` (the contract).
- `django_async_backend/db/models/manager.py` — `AsyncManager.acreate`.
- `tests/test_app/models.py` — `SCNeither`/`SCSave`/`SCAsave`/`SCBoth`.
- `tests/db/models/test_async_save_contract.py` — the matrix (asserts the
  coherent table above, and pins the vanilla numbers for contrast).

## Running the tests

```bash
docker compose up postgres -d
cd tests && DJANGO_SETTINGS_MODULE=settings \
  python manage.py test db.models.test_async_save_contract
```
