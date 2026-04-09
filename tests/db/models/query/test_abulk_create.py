from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestABulkCreate(AsyncioTestCase):
    async def test_abulk_create_basic(self):
        objs = [
            TestModel(name="Bulk1", value=1),
            TestModel(name="Bulk2", value=2),
            TestModel(name="Bulk3", value=3),
        ]
        result = await TestModel.async_object.abulk_create(objs)
        self.assertEqual(len(result), 3)
        for obj in result:
            self.assertIsNotNone(obj.pk)
        self.assertEqual(await TestModel.async_object.acount(), 3)

    async def test_abulk_create_empty(self):
        result = await TestModel.async_object.abulk_create([])
        self.assertEqual(result, [])
        self.assertEqual(await TestModel.async_object.acount(), 0)

    async def test_abulk_create_batch_size(self):
        objs = [TestModel(name=f"Batch{i}", value=i) for i in range(5)]
        result = await TestModel.async_object.abulk_create(objs, batch_size=2)
        self.assertEqual(len(result), 5)
        self.assertEqual(await TestModel.async_object.acount(), 5)

    async def test_abulk_create_pks_are_unique(self):
        objs = [
            TestModel(name="A", value=1),
            TestModel(name="B", value=2),
        ]
        result = await TestModel.async_object.abulk_create(objs)
        pks = [obj.pk for obj in result]
        self.assertEqual(len(set(pks)), 2)

    async def test_abulk_create_persists_to_db(self):
        objs = [TestModel(name="Persist1", value=10)]
        await TestModel.async_object.abulk_create(objs)
        fetched = await TestModel.async_object.aget(name="Persist1")
        self.assertEqual(fetched.value, 10)
