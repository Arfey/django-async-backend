from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestModelASave(AsyncioTestCase):
    async def test_asave_insert(self):
        obj = TestModel(name="SaveTest", value=42)
        await obj.asave()
        self.assertIsNotNone(obj.pk)
        self.assertFalse(obj._state.adding)

        fetched = await TestModel.async_object.aget(pk=obj.pk)
        self.assertEqual(fetched.name, "SaveTest")
        self.assertEqual(fetched.value, 42)

    async def test_asave_update(self):
        obj = TestModel(name="UpdateTest", value=1)
        await obj.asave()
        original_pk = obj.pk

        obj.value = 99
        await obj.asave()
        self.assertEqual(obj.pk, original_pk)

        fetched = await TestModel.async_object.aget(pk=obj.pk)
        self.assertEqual(fetched.value, 99)

    async def test_asave_force_insert(self):
        obj = TestModel(name="ForceInsert", value=10)
        await obj.asave(force_insert=True)
        self.assertIsNotNone(obj.pk)

        fetched = await TestModel.async_object.aget(name="ForceInsert")
        self.assertEqual(fetched.value, 10)

    async def test_asave_update_fields(self):
        obj = TestModel(name="Partial", value=1)
        await obj.asave()

        obj.name = "Changed"
        obj.value = 999
        await obj.asave(update_fields=["value"])

        fetched = await TestModel.async_object.aget(pk=obj.pk)
        self.assertEqual(fetched.name, "Partial")  # unchanged
        self.assertEqual(fetched.value, 999)  # updated

    async def test_asave_with_fk(self):
        parent = TestModel(name="Parent")
        await parent.asave()

        child = TestModel(name="Child", relative=parent)
        await child.asave()
        self.assertIsNotNone(child.pk)
        self.assertEqual(child.relative_id, parent.pk)


class TestModelADelete(AsyncioTestCase):
    async def test_adelete_instance(self):
        obj = await TestModel.async_object.acreate(name="ToDelete", value=1)
        pk = obj.pk
        count, _per_model = await obj.adelete()
        self.assertEqual(count, 1)
        self.assertIsNone(obj.pk)
        self.assertFalse(await TestModel.async_object.filter(pk=pk).aexists())

    async def test_adelete_no_pk_raises(self):
        obj = TestModel(name="NoPK")
        with self.assertRaises(ValueError):
            await obj.adelete()
