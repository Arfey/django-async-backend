from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAUpdateOrCreate(AsyncioTestCase):
    async def test_creates_when_not_exists(self):
        obj, created = await TestModel.async_object.aupdate_or_create(name="New", defaults={"value": 42})
        self.assertTrue(created)
        self.assertEqual(obj.name, "New")
        self.assertEqual(obj.value, 42)

    async def test_updates_when_exists(self):
        await TestModel.async_object.acreate(name="Exists", value=1)
        obj, created = await TestModel.async_object.aupdate_or_create(name="Exists", defaults={"value": 99})
        self.assertFalse(created)
        self.assertEqual(obj.value, 99)

        # Verify persisted
        fetched = await TestModel.async_object.aget(name="Exists")
        self.assertEqual(fetched.value, 99)

    async def test_create_defaults(self):
        obj, created = await TestModel.async_object.aupdate_or_create(
            name="New",
            defaults={"value": 10},
            create_defaults={"value": 50},
        )
        self.assertTrue(created)
        self.assertEqual(obj.value, 50)
