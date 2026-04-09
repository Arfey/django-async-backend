from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAUpdate(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj1 = await TestModel.async_object.acreate(name="Item1", value=10)
        self.obj2 = await TestModel.async_object.acreate(name="Item2", value=20)
        self.obj3 = await TestModel.async_object.acreate(name="Item3", value=30)

    async def test_aupdate_all(self):
        rows = await TestModel.async_object.aupdate(value=99)
        self.assertEqual(rows, 3)

        results = [obj async for obj in TestModel.async_object.all()]
        for obj in results:
            self.assertEqual(obj.value, 99)

    async def test_aupdate_filtered(self):
        rows = await TestModel.async_object.filter(name="Item1").aupdate(value=42)
        self.assertEqual(rows, 1)

        obj = await TestModel.async_object.aget(name="Item1")
        self.assertEqual(obj.value, 42)

        # Others unchanged
        obj2 = await TestModel.async_object.aget(name="Item2")
        self.assertEqual(obj2.value, 20)

    async def test_aupdate_no_match(self):
        rows = await TestModel.async_object.filter(name="NonExistent").aupdate(value=0)
        self.assertEqual(rows, 0)

    async def test_aupdate_sliced_raises(self):
        with self.assertRaises(TypeError):
            await TestModel.async_object.all()[:1].aupdate(value=0)
