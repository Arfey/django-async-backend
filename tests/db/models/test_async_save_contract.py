"""PROTOTYPE: the coherent async save contract vs vanilla Django.

`save` adds 1, `asave` adds 10, so the resulting `foo` NAMES which method(s)
ran (this is what makes the proof unambiguous -- a same-sized increment could
not distinguish "asave ran" from "save ran"):

    foo == 0   neither ran
    foo == 1   save ran (only)
    foo == 10  asave ran (only)
    foo == 11  BOTH ran  (i.e. a double-run)

Vanilla Django 6.0 (independently reproduced -- see
docs/proposals/vanilla_repro.py), starting from foo=0:

    overrides    | await obj.asave() | await acreate()
    -------------|-------------------|----------------
    nothing      | 0                 | 0
    save only    | 1                 | 1
    asave only   | 10                | 0   <- asave silently dropped
    both         | 11  <- DOUBLE-RUN | 1   <- ran save, not asave

This contract instead routes both entry points through asave:

    overrides    | await obj.asave() | await acreate()
    -------------|-------------------|----------------
    nothing      | 0                 | 0
    save only    | 1                 | 1
    asave only   | 10                | 10
    both         | 10  <- no double  | 10  <- agrees with asave
"""
from test_app.models import (
    SCAsave,
    SCBoth,
    SCNeither,
    SCSave,
)

from django_async_backend.test import AsyncioTestCase

# model -> expected foo under THIS contract. Same for asave and acreate, and
# the *value itself* identifies the path that ran (see bitmask above).
CONTRACT = [
    (SCNeither, 0),   # 00 neither
    (SCSave, 1),      # 01 save ran (graceful sync_to_async(save))
    (SCAsave, 10),    # 10 asave ran natively
    (SCBoth, 10),     # 10 asave ran, save did NOT (would be 11 if double)
]

# What vanilla Django produces, for contrast (reproduced in vanilla_repro.py).
VANILLA_ASAVE = {SCNeither: 0, SCSave: 1, SCAsave: 10, SCBoth: 11}
VANILLA_ACREATE = {SCNeither: 0, SCSave: 1, SCAsave: 0, SCBoth: 1}


class TestAsaveContract(AsyncioTestCase):
    async def test_asave_each_shape(self):
        for model, expected in CONTRACT:
            with self.subTest(model=model.__name__):
                obj = model()  # foo defaults to 0
                await obj.asave()
                self.assertEqual(obj.foo, expected)

    async def test_acreate_each_shape(self):
        for model, expected in CONTRACT:
            with self.subTest(model=model.__name__):
                obj = await model.async_object.acreate()
                self.assertEqual(obj.foo, expected)

    async def test_acreate_and_asave_agree(self):
        # The headline fix: the two entry points are finally consistent.
        for model, _ in CONTRACT:
            with self.subTest(model=model.__name__):
                via_asave = model()
                await via_asave.asave()
                via_acreate = await model.async_object.acreate()
                self.assertEqual(via_asave.foo, via_acreate.foo)

    async def test_both_overridden_runs_asave_not_save(self):
        # foo == 10 proves asave ran and save did NOT:
        #   11 would mean both ran (the vanilla double-run);
        #    1 would mean save ran and asave was skipped.
        obj = SCBoth()
        await obj.asave()
        self.assertEqual(obj.foo, 10)
        self.assertNotEqual(obj.foo, 11)  # not a double-run
        self.assertNotEqual(obj.foo, 1)   # asave was not skipped for save
        self.assertEqual(VANILLA_ASAVE[SCBoth], 11)  # what we are fixing

    async def test_save_only_runs_save_via_graceful_path(self):
        # foo == 1 proves the user's sync save ran (graceful degradation),
        # not the native path (which would leave foo == 0).
        obj = SCSave()
        await obj.asave()
        self.assertEqual(obj.foo, 1)

    async def test_asave_only_acreate_now_honoured(self):
        # foo == 10 proves acreate ran asave. Vanilla acreate drops it (0).
        obj = await SCAsave.async_object.acreate()
        self.assertEqual(obj.foo, 10)
        self.assertEqual(VANILLA_ACREATE[SCAsave], 0)  # what we are fixing


class TestNativePersistence(AsyncioTestCase):
    """The native path (no override / asave-override) really writes through the
    async connection and is visible to async reads in the same transaction."""

    async def test_native_insert_persists(self):
        obj = await SCNeither.async_object.acreate(foo=7)
        self.assertIsNotNone(obj.pk)
        reloaded = await SCNeither.async_object.aget(pk=obj.pk)
        self.assertEqual(reloaded.foo, 7)

    async def test_native_update_persists(self):
        obj = await SCNeither.async_object.acreate(foo=3)
        obj.foo = 99
        await obj.asave()  # pk set, not force_insert -> UPDATE branch
        reloaded = await SCNeither.async_object.aget(pk=obj.pk)
        self.assertEqual(reloaded.foo, 99)

    async def test_asave_override_persists_natively(self):
        obj = await SCAsave.async_object.acreate(foo=10)  # asave bumps to 20
        self.assertEqual(obj.foo, 20)
        reloaded = await SCAsave.async_object.aget(pk=obj.pk)
        self.assertEqual(reloaded.foo, 20)
