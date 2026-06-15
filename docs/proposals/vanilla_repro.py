"""Reproduce vanilla Django's save/asave incoherence -- runnable proof.

PURE stock Django on file-based sqlite. NO django-async-backend involved. This
is the committed evidence behind the "vanilla" column of the proposal table
(docs/proposals/async-save-contract.md): the asave/acreate divergence and the
both-overridden double-run are real, not asserted.

Run:  python docs/proposals/vanilla_repro.py
Needs only Django installed (no postgres).

`save` adds 1, `asave` adds 10, so the printed foo NAMES which method ran:
0=neither, 1=save, 10=asave, 11=both (double-run).

Expected output (Django 6.0):

    overrides    | await obj.asave() | await acreate()
    -------------|-------------------|----------------
    nothing      | 0                 | 0
    save only    | 1                 | 1
    asave only   | 10                | 0   <- asave silently dropped
    both         | 11  <- DOUBLE-RUN | 1   <- ran save, not asave
"""
import asyncio
import tempfile

import django
from django.conf import settings

# File-based sqlite (NOT :memory:): asave runs the save on a worker thread via
# sync_to_async, and an in-memory db is per-connection, so the table would be
# invisible there.
_DB = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False).name

settings.configure(
    INSTALLED_APPS=[],
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": _DB}},
    DEFAULT_AUTO_FIELD="django.db.models.AutoField",
)
django.setup()

from django.db import connection, models  # noqa: E402


class Neither(models.Model):
    foo = models.IntegerField(default=0)

    class Meta:
        app_label = "x"


class SaveOnly(models.Model):
    foo = models.IntegerField(default=0)

    class Meta:
        app_label = "x"

    def save(self, *a, **k):
        self.foo += 1
        super().save(*a, **k)


class AsaveOnly(models.Model):
    foo = models.IntegerField(default=0)

    class Meta:
        app_label = "x"

    async def asave(self, *a, **k):
        self.foo += 10
        await super().asave(*a, **k)


class Both(models.Model):
    foo = models.IntegerField(default=0)

    class Meta:
        app_label = "x"

    def save(self, *a, **k):
        self.foo += 1
        super().save(*a, **k)

    async def asave(self, *a, **k):
        self.foo += 10
        await super().asave(*a, **k)


MODELS = [
    ("nothing", Neither),
    ("save only", SaveOnly),
    ("asave only", AsaveOnly),
    ("both", Both),
]

with connection.schema_editor() as se:
    for _, M in MODELS:
        se.create_model(M)


async def main():
    print("Django version:", django.get_version())
    print(f"{'overrides':<12} | {'await obj.asave()':<18} | await acreate()")
    print("-" * 52)
    for label, M in MODELS:
        obj = M()
        await obj.asave()
        created = await M.objects.acreate()
        note = "  <- DOUBLE-RUN" if obj.foo == 11 else ""
        print(f"{label:<12} | foo={obj.foo:<14}{note} | foo={created.foo}")


asyncio.run(main())
