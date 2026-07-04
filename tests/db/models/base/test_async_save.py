from test_app.models import SaveModel

from django_async_backend.test import AsyncioTestCase


class TestAsyncSave(AsyncioTestCase):
    async def test_insert_creates_row(self):
        instance = SaveModel(name="Item1", value=1)

        await instance.async_save()

        stored = await SaveModel.async_objects.aget(name="Item1")
        self.assertEqual(stored.pk, instance.pk)
        self.assertEqual(stored.value, 1)

    async def test_insert_assigns_pk(self):
        instance = SaveModel(name="Item2")

        self.assertIsNone(instance.pk)
        await instance.async_save()

        self.assertIsNotNone(instance.pk)
        self.assertTrue(
            await SaveModel.async_objects.filter(pk=instance.pk).aexists()
        )
