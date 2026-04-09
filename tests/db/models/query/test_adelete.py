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

    async def test_adelete_cascade_fk(self):
        """Deleting a parent should cascade-delete its children."""
        parent = await TestModel.async_object.acreate(name="Parent", value=1)
        await TestModel.async_object.acreate(name="Child1", value=2, relative=parent)
        await TestModel.async_object.acreate(name="Child2", value=3, relative=parent)
        self.assertEqual(await TestModel.async_object.acount(), 6)  # 3 from setUp + 3

        # Delete the parent — children should be cascade-deleted
        num_deleted, _ = await TestModel.async_object.filter(pk=parent.pk).adelete()
        self.assertEqual(num_deleted, 3)  # parent + 2 children
        self.assertEqual(await TestModel.async_object.acount(), 3)  # only setUp items

    async def test_adelete_instance_cascade(self):
        """Model.adelete() should cascade too."""
        parent = await TestModel.async_object.acreate(name="Parent2", value=1)
        await TestModel.async_object.acreate(name="Kid1", value=2, relative=parent)
        await TestModel.async_object.acreate(name="Kid2", value=3, relative=parent)

        count, _ = await parent.adelete()
        self.assertEqual(count, 3)
        self.assertIsNone(parent.pk)
