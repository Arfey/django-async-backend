from test_app.models import (
    GetLatestByModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestAEarliest(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1").async_save()
        await TestModel(name="Item2").async_save()
        await TestModel(name="Item3").async_save()

    async def test_aearliest_with_results(self):
        earliest_item = await TestModel.async_objects.aearliest("name")
        self.assertIsNotNone(
            earliest_item, "aearliest should return the earliest object"
        )
        self.assertEqual(
            earliest_item.name,
            "Item1",
            "The earliest object should be 'Item1'",
        )

    async def test_aearliest_no_results(self):
        with self.assertRaises(TestModel.DoesNotExist):
            await TestModel.async_objects.filter(name="some").aearliest("name")

    async def test_aearliest_with_custom_field(self):
        earliest_item = await TestModel.async_objects.aearliest("name")
        self.assertIsNotNone(
            earliest_item,
            "aearliest should return the earliest object by custom field",
        )
        self.assertEqual(
            earliest_item.name,
            "Item1",
            "The earliest object should be 'Item1' by name",
        )

    async def test_aearliest_no_get_latest_by(self):
        with self.assertRaises(ValueError):
            await TestModel.async_objects.aearliest()

    async def test_aearliest_get_latest_by(self):
        await GetLatestByModel(name="Item1").async_save()
        await GetLatestByModel(name="Item2").async_save()

        earliest_item = await GetLatestByModel.async_objects.aearliest()
        self.assertIsNotNone(
            earliest_item, "aearliest should return the earliest object"
        )
        self.assertEqual(
            earliest_item.name,
            "Item1",
            "The earliest object should be 'Item1'",
        )

    async def test_aearliest_with_sliced_query(self):
        sliced_queryset = TestModel.async_objects.all()[:1]
        with self.assertRaises(TypeError):
            await sliced_queryset.aearliest("name")
