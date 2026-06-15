"""PROTOTYPE: the coherent async save contract vs vanilla Django.

Each model does `self.foo += 1` in whichever method it overrides, so the final
`foo` reveals which path ran and how many times.

Vanilla Django 6.0 (proven empirically, starting from foo=0):

    overrides    | await obj.asave() | await acreate()
    -------------|-------------------|----------------
    nothing      | 0                 | 0
    save only    | 1                 | 1
    asave only   | 1                 | 0   <- asave silently dropped
    both         | 2  <- DOUBLE      | 1   <- disagrees with asave!

This contract (AsyncSaveModel + AsyncManager.acreate) instead yields a
coherent 0 / 1 / 1 / 1 for BOTH entry points: no double-run, and acreate
agrees with asave.
"""
from test_app.models import (
    SCAsave,
    SCBoth,
    SCNeither,
    SCSave,
)

from django_async_backend.test import AsyncioTestCase

# model -> expected foo under THIS contract (same for asave and acreate)
CONTRACT = [
    (SCNeither, 0),
    (SCSave, 1),
    (SCAsave, 1),
    (SCBoth, 1),
]

# What vanilla Django produces, for contrast / documenting the breaks.
VANILLA_ASAVE = {SCNeither: 0, SCSave: 1, SCAsave: 1, SCBoth: 2}
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

    async def test_both_overridden_no_double_run(self):
        # Vanilla Django gives foo=2 here (asave -> super().asave() ->
        # sync_to_async(self.save) -> user save runs too). The contract runs
        # asave once and never touches save.
        obj = SCBoth()
        await obj.asave()
        self.assertEqual(obj.foo, 1)
        self.assertEqual(VANILLA_ASAVE[SCBoth], 2)  # what we are fixing

    async def test_asave_only_acreate_now_honoured(self):
        # Vanilla acreate ignores asave entirely (foo stays 0, #36888).
        obj = await SCAsave.async_object.acreate()
        self.assertEqual(obj.foo, 1)
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
        obj = await SCAsave.async_object.acreate(foo=10)  # asave bumps to 11
        self.assertEqual(obj.foo, 11)
        reloaded = await SCAsave.async_object.aget(pk=obj.pk)
        self.assertEqual(reloaded.foo, 11)
