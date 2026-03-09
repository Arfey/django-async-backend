from test_app.models import (
    GetLatestByModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestALatest(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1")
        await TestModel.async_object.acreate(name="Item2")
        await TestModel.async_object.acreate(name="Item3")

    async def test_alatest_with_results(self):
        latest_item = await TestModel.async_object.alatest("name")
        self.assertIsNotNone(
            latest_item, "alatest should return the latest object"
        )
        self.assertEqual(
            latest_item.name, "Item3", "The latest object should be 'Item3'"
        )

    async def test_alatest_no_results(self):
        with self.assertRaises(TestModel.DoesNotExist):
            await TestModel.async_object.filter(name="some").alatest("name")

    async def test_alatest_with_custom_field(self):
        latest_item = await TestModel.async_object.alatest("name")
        self.assertIsNotNone(
            latest_item,
            "alatest should return the latest object by custom field",
        )
        self.assertEqual(
            latest_item.name,
            "Item3",
            "The latest object should be 'Item3' by name",
        )

    async def test_alatest_no_get_latest_by(self):
        with self.assertRaises(ValueError):
            await TestModel.async_object.alatest()

    async def test_alatest_get_latest_by(self):
        await GetLatestByModel.async_object.acreate(name="Item1")
        await GetLatestByModel.async_object.acreate(name="Item2")

        latest_item = await GetLatestByModel.async_object.alatest()
        self.assertIsNotNone(
            latest_item, "alatest should return the latest object"
        )
        self.assertEqual(
            latest_item.name, "Item2", "The latest object should be 'Item2'"
        )

    async def test_alatest_with_sliced_query(self):
        sliced_queryset = TestModel.async_object.all()[:1]
        with self.assertRaises(TypeError):
            await sliced_queryset.alatest("name")
