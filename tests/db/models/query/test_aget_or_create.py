from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAGetOrCreate(AsyncioTestCase):
    async def test_creates_when_not_exists(self):
        obj, created = await TestModel.async_object.aget_or_create(name="New", defaults={"value": 42})
        self.assertTrue(created)
        self.assertEqual(obj.name, "New")
        self.assertEqual(obj.value, 42)
        self.assertIsNotNone(obj.pk)

    async def test_gets_when_exists(self):
        existing = await TestModel.async_object.acreate(name="Exists", value=1)
        obj, created = await TestModel.async_object.aget_or_create(name="Exists", defaults={"value": 99})
        self.assertFalse(created)
        self.assertEqual(obj.pk, existing.pk)
        self.assertEqual(obj.value, 1)  # original value, not default

    async def test_no_defaults(self):
        obj, created = await TestModel.async_object.aget_or_create(name="NoDefaults")
        self.assertTrue(created)
        self.assertEqual(obj.name, "NoDefaults")
