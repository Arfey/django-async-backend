from django.core.exceptions import FieldError
from django.db import IntegrityError
from test_app.models import (
    GetOrCreateModel,
    SaveModel,
)

from django_async_backend.test import AsyncioTestCase


class TestAGetOrCreate(AsyncioTestCase):
    async def test_returns_existing_object(self):
        existing = await SaveModel.async_objects.acreate(
            name="Existing", value=1
        )

        obj, created = await SaveModel.async_objects.aget_or_create(
            name="Existing"
        )

        self.assertFalse(created)
        self.assertEqual(obj.pk, existing.pk)

    async def test_creates_when_missing(self):
        obj, created = await SaveModel.async_objects.aget_or_create(
            name="Fresh", defaults={"value": 7}
        )

        self.assertTrue(created)
        self.assertIsNotNone(obj.pk)
        self.assertEqual(obj.value, 7)
        self.assertTrue(
            await SaveModel.async_objects.filter(name="Fresh").aexists()
        )

    async def test_defaults_callable_is_resolved(self):
        obj, created = await SaveModel.async_objects.aget_or_create(
            name="Callable", defaults={"value": lambda: 42}
        )

        self.assertTrue(created)
        self.assertEqual(obj.value, 42)

    async def test_invalid_field_raises_field_error(self):
        with self.assertRaises(FieldError):
            await SaveModel.async_objects.aget_or_create(
                name="Bad", defaults={"nonexistent": 1}
            )

    async def test_integrity_error_is_reraised(self):
        await GetOrCreateModel.async_objects.acreate(name="A", code="dup")

        with self.assertRaises(IntegrityError):
            await GetOrCreateModel.async_objects.aget_or_create(
                name="B", defaults={"code": "dup"}
            )
