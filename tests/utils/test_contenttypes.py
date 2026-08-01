from django.contrib.contenttypes.models import ContentType
from test_app.models import (
    GenericFkModel,
    SaveModel,
    SaveProxyModel,
)

from django_async_backend.test import AsyncioTestCase
from django_async_backend.utils.contenttypes import aget_for_model


class TestAGetForModel(AsyncioTestCase):
    def setUp(self):
        ContentType.objects.clear_cache()
        self.addCleanup(ContentType.objects.clear_cache)

    async def test_returns_content_type_for_model(self):
        content_type = await aget_for_model(SaveModel)

        self.assertEqual(content_type.app_label, "test_app")
        self.assertEqual(content_type.model, "savemodel")

    async def test_creates_missing_content_type(self):
        await ContentType.async_objects.filter(
            app_label="test_app", model="savemodel"
        ).adelete()

        content_type = await aget_for_model(SaveModel)

        self.assertTrue(
            await ContentType.async_objects.filter(
                pk=content_type.pk
            ).aexists()
        )

    async def test_second_call_is_served_from_the_cache(self):
        content_type = await aget_for_model(SaveModel)
        await ContentType.async_objects.filter(pk=content_type.pk).adelete()

        again = await aget_for_model(SaveModel)

        self.assertEqual(again.pk, content_type.pk)

    async def test_resolves_proxy_model_to_its_concrete_model(self):
        content_type = await aget_for_model(SaveProxyModel)

        self.assertEqual(content_type.model, "savemodel")

    async def test_keeps_proxy_model_when_not_for_concrete_model(self):
        content_type = await aget_for_model(
            SaveProxyModel, for_concrete_model=False
        )

        self.assertEqual(content_type.model, "saveproxymodel")

    async def test_using_selects_the_database(self):
        content_type = await aget_for_model(SaveModel, using="other")

        self.assertEqual(content_type._state.db, "other")

    async def test_caches_per_database(self):
        await aget_for_model(SaveModel, using="other")

        content_type = await aget_for_model(SaveModel)

        self.assertEqual(content_type._state.db, "default")

    async def test_warms_cache_for_generic_foreign_key_assignment(self):
        target = await SaveModel.async_objects.acreate(name="GfkTarget")

        await aget_for_model(SaveModel)

        obj = await GenericFkModel.async_objects.acreate(
            name="Gfk", content_object=target
        )
        self.assertEqual(obj.object_id, target.pk)
