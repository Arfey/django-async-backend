from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAContains(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj = await TestModel.async_object.acreate(name="Item1", value=1)

    async def test_acontains_true(self):
        result = await TestModel.async_object.all().acontains(self.obj)
        self.assertTrue(result)

    async def test_acontains_false(self):
        other = TestModel(pk=99999, name="Ghost")
        result = await TestModel.async_object.all().acontains(other)
        self.assertFalse(result)

    async def test_acontains_filtered(self):
        result = await TestModel.async_object.filter(value=1).acontains(self.obj)
        self.assertTrue(result)

        result = await TestModel.async_object.filter(value=999).acontains(self.obj)
        self.assertFalse(result)
