# Contributing

Contributions are welcome, and they are greatly appreciated. Every little bit
helps, and credit will always be given.

## Types of contributions

### Report bugs

Report bugs at the
[issue tracker](https://github.com/Arfey/django-async-backend/issues/new?template=bug_report.yml).

The bug report form asks for everything needed to reproduce the problem:

- your django-async-backend, Django, Python and PostgreSQL versions;
- whether you are running under ASGI or WSGI, and with or without connection
  pooling;
- a minimal snippet that reproduces the problem;
- the full traceback.


### Fix bugs or implement features

Anything in the [issue tracker](https://github.com/Arfey/django-async-backend/issues)
is open to whoever wants to pick it up — there is no formal triage or
assignment process. If an issue looks interesting, comment on it to say you are
working on it, so nobody duplicates the effort.


### Write documentation

The docs you are reading always need more examples and clarifications. They
live in `docs/source/` and are written in Markdown (MyST).

### Submit feedback

The best way to propose a feature is to
[open a feature request](https://github.com/Arfey/django-async-backend/issues/new?template=feature_request.yml)
that explains how it would work and keeps the scope as narrow as possible.

For usage questions that are not bugs, use
[Discussions](https://github.com/Arfey/django-async-backend/discussions).

## Install `lets`

This project uses [lets](https://lets-cli.org) as its task runner. Every
common task — tests, linting, docs, ORM regeneration — is defined in
`lets.yaml`, and `lets` targets are the canonical way to run them. Install it
first.

::::{tab-set}

:::{tab-item} Shell script
```bash
curl -fsSL https://lets-cli.org/install.sh | bash
```

Installs to `$HOME/.lets/bin/lets` and adds it to your `PATH` automatically.
:::

:::{tab-item} Homebrew
```bash
brew tap lets-cli/tap
brew install lets-cli/tap/lets
```
:::

:::{tab-item} Arch Linux
```bash
yay -S lets-bin
```
:::

:::{tab-item} Go
```bash
go install github.com/lets-cli/lets/cmd/lets@latest
```
:::

::::

Binaries are also published on the
[releases page](https://github.com/lets-cli/lets/releases). See the
[installation docs](https://lets-cli.org/docs/installation/) for CI usage,
pinning a specific version, and installing to a custom directory.

Check it works — this lists every target in `lets.yaml`:

```bash
lets --help
```

```{note}
Each task below also shows the equivalent raw command, so you can work without
`lets` if you prefer.
```

## Get started

Ready to contribute? Here's how to set up `django-async-backend` for local
development.

You will need [Poetry](https://python-poetry.org/docs/#installation), Docker
(the tests run against a real PostgreSQL instance) and Python 3.12+.

**1. Fork the repo** on GitHub, then clone your fork:

```bash
git clone git@github.com:your_name_here/django-async-backend.git
cd django-async-backend
```

**2. Install the dependencies**, including every extra:

```bash
poetry install --with dev,docs --all-extras
```

```{note}
`--all-extras` pulls in `psycopg[binary]` and `psycopg[pool]`. Install them
even if you do not plan to touch pooling: the test suite exercises the pooled
code paths and imports `psycopg_pool` directly, so without the extras those
tests fail outright rather than being skipped. `--with dev,docs` adds the test
tooling and the Sphinx toolchain, so you can build the docs locally too.
```

**3. Create a branch** for your change:

```bash
git checkout -b name-of-your-bugfix-or-feature
```

**4. Make your change**, then check it before pushing:

```bash
lets lint   # linters and formatters
lets test   # the full test suite
```

**5. Commit and open a pull request** against `main`.

## Pull request guidelines

Before you submit, check that:

- the test suite passes and new behaviour is covered by a test;
- `lets lint` is clean — CI runs the same pre-commit hooks;
- the docs are updated if you changed behaviour;
- generated ORM files were produced by `lets generate` rather than edited by
  hand — CI regenerates them and fails if the result differs.

## Running tests

This project uses a comprehensive test suite powered by `unittest`. Integration
tests run locally against a real PostgreSQL instance.

```bash
lets test
```

`lets test` starts the PostgreSQL container if it isn't already running. To run
a single module:

```bash
lets test tests.db.models
```

other options

```bash
lets test --help
```

### Coverage

```bash
lets coverage
```

## Linting

```bash
lets lint
```

Runs all pre-commit hooks over the whole tree.

## Django's own test suite

The `django_async_backend.db.backends.postgresql` backend is fully compatible
with Django's default `django.db.backends.postgresql` backend, as it leverages
the default implementation under the hood. To confirm this compatibility, run
Django's test suite using the custom backend:

```python
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
    "other": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}
```

To execute them:

```bash
lets test_django
```

This builds the image and runs Django's suite in Docker, so it needs Docker
running but no local PostgreSQL.

## Code generation

Large parts of the ORM are **generated, not hand-written**. The async versions
of Django's query classes are produced from Django's own source by `codemon`,
a small codemod tool that lives in `codemon/` and rewrites the sync code into
async using [libcst](https://libcst.readthedocs.io/).

The generated files are committed to git, so the package installs without
running codegen. They look hand-written but are not — each carries a header:

```python
# This file was generated automatically. Do not modify it manually.
```

### How it works

`codemon/config/*.yaml` describes one generated module each. The filename
mirrors the output path, so `db__models__query.yaml` produces
`django_async_backend/db/models/query.py`, and its `pathname` key points at the
matching file in Django's tree:

```yaml
pathname: db/models/query.py

module:
  new_imports:
    - "from django_async_backend.db.transaction import async_atomic"
  classes:
    QuerySet:
      assigns:
        - target:
            value: create
            rename:
              value: acreate
```

For each config, codemon downloads that file from the pinned Django tag on
GitHub — the version lives in `DJANGO_VERSION` in `codemon/__main__.py` — parses
it with libcst, applies the transformations from the YAML, and writes the
result into the package.

Because the transformations are declarative, upgrading to a new Django version
is mostly a matter of bumping `DJANGO_VERSION` and fixing whatever the configs
no longer match.

### Regenerating

Edit the config under `codemon/config/*.yaml` — **never** the generated file —
then run:

```bash
lets generate
```

This restores the Django-derived files to pristine, runs `python -m codemon`,
and reformats the result. Running `codemon` on its own skips the
restore/reformat step and produces large formatting-only diffs.

```{warning}
Regeneration downloads Django's source over the network, so it needs internet
access. Any manual edit to a generated module is lost on the next run — and CI
regenerates them on every PR and fails if the result differs.
```

## Building the docs

```bash
lets docs
```

The rendered site lands in `docs/build/html`. For a live-reloading server on
<http://localhost:8080>:

```bash
lets docs_serve
```

Both need the `docs` dependency group, which
[Get started](#get-started) installs.

## Running the example app

```bash
lets test_app
```

Serves the project in `test_example/` with uvicorn on port 8000.
