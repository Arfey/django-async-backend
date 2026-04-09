from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestADelete(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=10)
        await TestModel.async_object.acreate(name="Item2", value=20)
        await TestModel.async_object.acreate(name="Item3", value=30)

    async def test_adelete_all(self):
        num_deleted, _deleted_per_model = await TestModel.async_object.all().adelete()
        self.assertEqual(num_deleted, 3)
        self.assertEqual(await TestModel.async_object.acount(), 0)

    async def test_adelete_filtered(self):
        num_deleted, _ = await TestModel.async_object.filter(name="Item1").adelete()
        self.assertEqual(num_deleted, 1)
        self.assertEqual(await TestModel.async_object.acount(), 2)

    async def test_adelete_no_match(self):
        num_deleted, _ = await TestModel.async_object.filter(name="NonExistent").adelete()
        self.assertEqual(num_deleted, 0)
        self.assertEqual(await TestModel.async_object.acount(), 3)

    async def test_adelete_sliced_raises(self):
        with self.assertRaises(TypeError):
            await TestModel.async_object.all()[:1].adelete()

    async def test_adelete_returns_model_label(self):
        _, deleted_per_model = await TestModel.async_object.all().adelete()
        self.assertIn("test_app.TestModel", deleted_per_model)
