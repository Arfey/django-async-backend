from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAExists(AsyncioTestCase):
    async def test_has_results(self):
        await TestModel(name="Test1").async_save()

        self.assertTrue(
            await TestModel.async_objects.aexists(),
            "has_results should return True when there are results",
        )

    async def test_has_results_no_objects(self):
        self.assertFalse(
            await TestModel.async_objects.aexists(),
            "has_results should return False when there are no results",
        )

    async def test_has_results_from_cache(self):
        await TestModel(name="Test1").async_save()

        results = [i async for i in TestModel.async_objects.all()]
        self.assertEqual(len(results), 1, "Cache should contain 1 object")

        self.assertTrue(
            await TestModel.async_objects.aexists(),
            "has_results should return True using the cache",
        )
