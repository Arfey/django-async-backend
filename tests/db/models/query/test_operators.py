from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestQuerySetOperators(AsyncioTestCase):
    async def asyncSetUp(self):
        self.obj1 = await TestModel.async_object.acreate(name="A", value=1)
        self.obj2 = await TestModel.async_object.acreate(name="B", value=2)
        self.obj3 = await TestModel.async_object.acreate(name="C", value=3)

    async def test_and_operator(self):
        qs1 = TestModel.async_object.filter(value__gte=1)
        qs2 = TestModel.async_object.filter(value__lte=2)
        combined = qs1 & qs2
        results = [obj async for obj in combined]
        names = {obj.name for obj in results}
        self.assertEqual(names, {"A", "B"})

    async def test_or_operator(self):
        qs1 = TestModel.async_object.filter(name="A")
        qs2 = TestModel.async_object.filter(name="C")
        combined = qs1 | qs2
        results = [obj async for obj in combined]
        names = {obj.name for obj in results}
        self.assertEqual(names, {"A", "C"})
