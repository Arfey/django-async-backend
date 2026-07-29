from test_app.models import (
    GenericFkModel,
    SaveModel,
    SaveParentModel,
    TestModel,
)

from django_async_backend.test import AsyncioTestCase
from django_async_backend.utils.contenttypes import aget_for_model


class TestACreate(AsyncioTestCase):
    async def test_acreate_creates_and_returns_object(self):
        obj = await TestModel.async_objects.acreate(name="Created", value=5)

        self.assertIsNotNone(obj.pk)
        self.assertEqual(obj.name, "Created")
        stored = await TestModel.async_objects.aget(pk=obj.pk)
        self.assertEqual(stored.value, 5)

    async def test_acreate_on_queryset(self):
        obj = await TestModel.async_objects.all().acreate(name="QsCreated")

        self.assertIsNotNone(obj.pk)
        self.assertTrue(
            await TestModel.async_objects.filter(name="QsCreated").aexists()
        )

    async def test_acreate_reverse_one_to_one_field_raises(self):
        with self.assertRaises(ValueError) as cm:
            await SaveParentModel.async_objects.acreate(savechildmodel=None)

        self.assertEqual(
            str(cm.exception),
            "The following fields do not exist in this model: savechildmodel",
        )

    async def test_acreate_with_generic_foreign_key(self):
        target = await SaveModel.async_objects.acreate(name="GfkTarget")
        content_type = await aget_for_model(SaveModel)

        obj = await GenericFkModel.async_objects.acreate(
            name="Gfk", content_object=target
        )

        self.assertEqual(obj.object_id, target.pk)
        self.assertEqual(obj.content_type_id, content_type.pk)
