from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAExists(AsyncioTestCase):
    async def test_has_results(self):
        await TestModel.async_object.acreate(name="Test1")

        self.assertTrue(
            await TestModel.async_object.aexists(),
            "has_results should return True when there are results",
        )

    async def test_has_results_no_objects(self):
        self.assertFalse(
            await TestModel.async_object.aexists(),
            "has_results should return False when there are no results",
        )

    async def test_has_results_from_cache(self):
        await TestModel.async_object.acreate(name="Test1")

        results = [i async for i in TestModel.async_object.all()]
        self.assertEqual(len(results), 1, "Cache should contain 1 object")

        self.assertTrue(
            await TestModel.async_object.aexists(),
            "has_results should return True using the cache",
        )
