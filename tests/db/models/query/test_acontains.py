from django.db import NotSupportedError
from test_app.models import (
    SaveModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase


class TestAContains(AsyncioTestCase):
    async def test_contains_object(self):
        obj = TestModel(name="Test1")
        await obj.async_save()

        self.assertTrue(
            await TestModel.async_objects.acontains(obj),
            "acontains should return True when the object is in the queryset",
        )

    async def test_does_not_contain_object(self):
        obj = TestModel(name="Test1")
        await obj.async_save()
        await TestModel.async_objects.filter(pk=obj.pk).adelete()

        self.assertFalse(
            await TestModel.async_objects.acontains(obj),
            "acontains should return False when the object is "
            "not in the queryset",
        )

    async def test_contains_object_from_cache(self):
        obj = TestModel(name="Test1")
        await obj.async_save()

        queryset = TestModel.async_objects.all()
        results = [i async for i in queryset]
        self.assertEqual(len(results), 1, "Cache should contain 1 object")

        self.assertTrue(
            await queryset.acontains(obj),
            "acontains should return True using the cache",
        )

    async def test_does_not_contain_object_from_cache(self):
        obj = TestModel(name="Test1")
        await obj.async_save()
        other = TestModel(name="Test2")
        await other.async_save()

        queryset = TestModel.async_objects.filter(name="Test1")
        results = [i async for i in queryset]
        self.assertEqual(len(results), 1, "Cache should contain 1 object")

        self.assertFalse(
            await queryset.acontains(other),
            "acontains should return False using the cache",
        )

    async def test_other_model(self):
        obj = SaveModel(name="Test1")
        await obj.async_save()

        self.assertFalse(
            await TestModel.async_objects.acontains(obj),
            "acontains should return False for another model",
        )

    async def test_not_a_model_instance(self):
        with self.assertRaisesRegex(
            TypeError, r"'obj' must be a model instance\."
        ):
            await TestModel.async_objects.acontains(object())

    async def test_unsaved_object(self):
        with self.assertRaisesRegex(
            ValueError,
            r"QuerySet\.contains\(\) cannot be used on unsaved objects\.",
        ):
            await TestModel.async_objects.acontains(TestModel(name="Test1"))

    async def test_after_values(self):
        obj = TestModel(name="Test1")
        await obj.async_save()

        with self.assertRaisesRegex(
            TypeError,
            r"Cannot call QuerySet\.contains\(\) after \.values\(\) or "
            r"\.values_list\(\)\.",
        ):
            await TestModel.async_objects.values("name").acontains(obj)

    async def test_after_union(self):
        obj = TestModel(name="Test1")
        await obj.async_save()

        queryset = TestModel.async_objects.filter(name="Test1").union(
            TestModel.async_objects.filter(name="Test2")
        )

        with self.assertRaisesRegex(
            NotSupportedError,
            r"Calling QuerySet\.contains\(\) after union\(\) is not "
            r"supported\.",
        ):
            await queryset.acontains(obj)
