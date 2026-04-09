from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestDefer(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=42)

    async def test_defer_field(self):
        results = [obj async for obj in TestModel.async_object.defer("value")]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, "Item1")

    async def test_defer_clear(self):
        results = [obj async for obj in TestModel.async_object.defer(None)]
        self.assertEqual(len(results), 1)


class TestOnly(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=42)

    async def test_only_field(self):
        results = [obj async for obj in TestModel.async_object.only("name")]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, "Item1")

    async def test_only_after_values_raises(self):
        with self.assertRaises(TypeError):
            TestModel.async_object.values("name").only("name")
