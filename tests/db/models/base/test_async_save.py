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

    async def test_force_insert_with_force_update_raises(self):
        instance = SaveModel(name="Item3", value=1)

        with self.assertRaises(ValueError) as cm:
            await instance.async_save(force_insert=True, force_update=True)

        self.assertEqual(
            str(cm.exception),
            "Cannot force both insert and updating in model saving.",
        )

    async def test_force_insert_with_update_fields_raises(self):
        instance = SaveModel(name="Item4", value=1)

        with self.assertRaises(ValueError) as cm:
            await instance.async_save(
                force_insert=True, update_fields=["value"]
            )

        self.assertEqual(
            str(cm.exception),
            "Cannot force both insert and updating in model saving.",
        )

    async def test_empty_update_fields_is_noop(self):
        instance = SaveModel(name="Item5", value=1)

        await instance.async_save(update_fields=[])

        self.assertIsNone(instance.pk)
        self.assertFalse(
            await SaveModel.async_objects.filter(name="Item5").aexists()
        )

    async def test_unknown_update_fields_raise(self):
        instance = SaveModel(name="Item6", value=1)
        await instance.async_save()

        with self.assertRaises(ValueError):
            await instance.async_save(update_fields=["missing"])
