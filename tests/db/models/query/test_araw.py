from unittest import IsolatedAsyncioTestCase

from test_app.models import TestModel


class TestARaw(IsolatedAsyncioTestCase):
    """Raw query tests. Uses IsolatedAsyncioTestCase directly since raw queries
    bypass transaction wrapping. Manually cleans up test data."""

    async def asyncSetUp(self):
        await TestModel.async_object.all().adelete()
        await TestModel.async_object.acreate(name="Raw1", value=10)
        await TestModel.async_object.acreate(name="Raw2", value=20)
        await TestModel.async_object.acreate(name="Raw3", value=30)

    async def asyncTearDown(self):
        await TestModel.async_object.all().adelete()

    async def test_araw_basic(self):
        qs = TestModel.async_object.araw("SELECT * FROM test_model")
        results = [obj async for obj in qs]
        self.assertEqual(len(results), 3)
        self.assertIsInstance(results[0], TestModel)

    async def test_araw_with_params(self):
        qs = TestModel.async_object.araw("SELECT * FROM test_model WHERE value > %s", [15])
        results = [obj async for obj in qs]
        self.assertEqual(len(results), 2)
        names = {obj.name for obj in results}
        self.assertEqual(names, {"Raw2", "Raw3"})

    async def test_araw_fields(self):
        qs = TestModel.async_object.araw("SELECT * FROM test_model WHERE name = %s", ["Raw1"])
        results = [obj async for obj in qs]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, "Raw1")
        self.assertEqual(results[0].value, 10)

    async def test_araw_empty_result(self):
        qs = TestModel.async_object.araw("SELECT * FROM test_model WHERE value > %s", [999])
        results = [obj async for obj in qs]
        self.assertEqual(len(results), 0)

    async def test_araw_ordering(self):
        qs = TestModel.async_object.araw("SELECT * FROM test_model ORDER BY value DESC")
        results = [obj async for obj in qs]
        self.assertEqual(results[0].name, "Raw3")
        self.assertEqual(results[-1].name, "Raw1")
