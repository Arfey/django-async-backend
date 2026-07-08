from test_app.models import (
    GetLatestByModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestALatest(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1").async_save()
        await TestModel(name="Item2").async_save()
        await TestModel(name="Item3").async_save()

    async def test_alatest_with_results(self):
        latest_item = await TestModel.async_objects.alatest("name")
        self.assertIsNotNone(
            latest_item, "alatest should return the latest object"
        )
        self.assertEqual(
            latest_item.name, "Item3", "The latest object should be 'Item3'"
        )

    async def test_alatest_no_results(self):
        with self.assertRaises(TestModel.DoesNotExist):
            await TestModel.async_objects.filter(name="some").alatest("name")

    async def test_alatest_with_custom_field(self):
        latest_item = await TestModel.async_objects.alatest("name")
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
            await TestModel.async_objects.alatest()

    async def test_alatest_get_latest_by(self):
        await GetLatestByModel(name="Item1").async_save()
        await GetLatestByModel(name="Item2").async_save()

        latest_item = await GetLatestByModel.async_objects.alatest()
        self.assertIsNotNone(
            latest_item, "alatest should return the latest object"
        )
        self.assertEqual(
            latest_item.name, "Item2", "The latest object should be 'Item2'"
        )

    async def test_alatest_with_sliced_query(self):
        sliced_queryset = TestModel.async_objects.all()[:1]
        with self.assertRaises(TypeError):
            await sliced_queryset.alatest("name")
