from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestABulkUpdate(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj1 = await TestModel.async_object.acreate(name="A", value=1)
        self.obj2 = await TestModel.async_object.acreate(name="B", value=2)
        self.obj3 = await TestModel.async_object.acreate(name="C", value=3)

    async def test_abulk_update_basic(self):
        self.obj1.value = 10
        self.obj2.value = 20
        self.obj3.value = 30
        rows = await TestModel.async_object.abulk_update([self.obj1, self.obj2, self.obj3], ["value"])
        self.assertEqual(rows, 3)

        fetched = await TestModel.async_object.aget(pk=self.obj1.pk)
        self.assertEqual(fetched.value, 10)
        fetched = await TestModel.async_object.aget(pk=self.obj2.pk)
        self.assertEqual(fetched.value, 20)

    async def test_abulk_update_empty(self):
        rows = await TestModel.async_object.abulk_update([], ["value"])
        self.assertEqual(rows, 0)

    async def test_abulk_update_batch_size(self):
        self.obj1.value = 100
        self.obj2.value = 200
        self.obj3.value = 300
        rows = await TestModel.async_object.abulk_update([self.obj1, self.obj2, self.obj3], ["value"], batch_size=2)
        self.assertEqual(rows, 3)

    async def test_abulk_update_no_fields_raises(self):
        with self.assertRaises(ValueError):
            await TestModel.async_object.abulk_update([self.obj1], [])
