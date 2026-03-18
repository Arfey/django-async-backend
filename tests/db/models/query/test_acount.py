from django_async_backend.test import AsyncioTestCase
from test_app.models import TestModel
from django.test import TestCase


class TestMock(TestCase):
    def test_mock(self):
        pass


class TestACount(AsyncioTestCase):
    async def test_acount(self):
        await TestModel.async_object.acreate(1)
        await TestModel.async_object.acreate(2)

        self.assertEqual(
            await TestModel.async_object.acount(),
            2,
            "Count should be 2",
        )

    async def test_acount_no_objects(self):
        self.assertEqual(
            await TestModel.async_object.acount(),
            0,
            "Count should be 0 when no objects exist",
        )
