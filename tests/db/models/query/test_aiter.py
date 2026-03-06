from django_async_backend.test import AsyncioTestCase
from test_app.models import TestModel
from django.test import TestCase


class TestMock(TestCase):
    def test_mock(self):
        pass


class TestAIter(AsyncioTestCase):
    async def test_aiter(self):
        await TestModel.async_object.acreate(name="Test1",)
        await TestModel.async_object.acreate(name="Test2")

        results = [obj async for obj in TestModel.async_object.all()]

        self.assertEqual(len(results), 2, "Should iterate over 2 objects")
        self.assertEqual(results[0].name, "Test1", "First object name should be 'Test1'")
        self.assertEqual(results[1].name, "Test2", "Second object name should be 'Test2'")

    async def test_aiter_no_objects(self):
        results = [obj async for obj in TestModel.async_object.all()]
        self.assertEqual(len(results), 0, "Should iterate over 0 objects when none exist")

    async def test_aiter_with_filter(self):
        await TestModel.async_object.acreate(name="Test1",)

        results = [obj async for obj in TestModel.async_object.filter(name='Test1')]

        self.assertEqual(len(results), 1, "Should iterate over 1 object")
        self.assertEqual(results[0].name, "Test1", "First object name should be 'Test1'")
