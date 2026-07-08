from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestNone(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()
        await TestModel(name="Item3", value=3).async_save()

    async def test_success(self):
        self.assertEqual(
            await TestModel.async_objects.none().acount(),
            0,
        )
