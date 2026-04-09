from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAIterator(AsyncioTestCase):
    async def asyncSetUp(self):
        for i in range(5):
            await TestModel.async_object.acreate(name=f"Item{i}", value=i)

    async def test_aiterator_basic(self):
        results = []
        async for obj in TestModel.async_object.all().aiterator():
            results.append(obj)
        self.assertEqual(len(results), 5)

    async def test_aiterator_chunk_size(self):
        results = []
        async for obj in TestModel.async_object.all().aiterator(chunk_size=2):
            results.append(obj)
        self.assertEqual(len(results), 5)

    async def test_aiterator_filtered(self):
        results = []
        async for obj in TestModel.async_object.filter(value__gte=3).aiterator():
            results.append(obj)
        self.assertEqual(len(results), 2)

    async def test_aiterator_no_cache(self):
        qs = TestModel.async_object.all()
        async for _ in qs.aiterator():
            pass
        # aiterator should NOT populate _result_cache
        self.assertIsNone(qs._result_cache)
