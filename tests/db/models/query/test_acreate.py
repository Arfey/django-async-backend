from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestACreate(AsyncioTestCase):
    async def test_acreate_basic(self):
        obj = await TestModel.async_object.acreate(name="NewItem")
        self.assertIsNotNone(obj.pk)
        self.assertEqual(obj.name, "NewItem")
        self.assertIsNone(obj.value)

    async def test_acreate_with_all_fields(self):
        obj = await TestModel.async_object.acreate(name="Full", value=42)
        self.assertIsNotNone(obj.pk)
        self.assertEqual(obj.name, "Full")
        self.assertEqual(obj.value, 42)

    async def test_acreate_with_fk(self):
        parent = await TestModel.async_object.acreate(name="Parent", value=1)
        child = await TestModel.async_object.acreate(name="Child", value=2, relative=parent)
        self.assertIsNotNone(child.pk)
        self.assertEqual(child.relative_id, parent.pk)

    async def test_acreate_auto_pk_assigned(self):
        obj1 = await TestModel.async_object.acreate(name="First")
        obj2 = await TestModel.async_object.acreate(name="Second")
        self.assertIsNotNone(obj1.pk)
        self.assertIsNotNone(obj2.pk)
        self.assertNotEqual(obj1.pk, obj2.pk)

    async def test_acreate_persists_to_db(self):
        obj = await TestModel.async_object.acreate(name="Persisted", value=7)
        fetched = await TestModel.async_object.aget(pk=obj.pk)
        self.assertEqual(fetched.name, "Persisted")
        self.assertEqual(fetched.value, 7)
