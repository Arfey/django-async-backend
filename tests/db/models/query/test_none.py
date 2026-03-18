from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestNone(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=1)
        await TestModel.async_object.acreate(name="Item2", value=2)
        await TestModel.async_object.acreate(name="Item3", value=3)

    async def test_success(self):
        self.assertEqual(
            await TestModel.async_object.none().acount(),
            0,
        )
