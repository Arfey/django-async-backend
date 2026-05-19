from django.db.models import F
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestAUpdate(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Item1", value=1)
        await TestModel.async_object.acreate(name="Item2", value=2)
        await TestModel.async_object.acreate(name="Item3", value=3)

    async def test_returns_row_count(self):
        rows = await TestModel.async_object.all().aupdate(value=42)
        self.assertEqual(rows, 3)
        values = sorted(
            [obj.value async for obj in TestModel.async_object.all()]
        )
        self.assertEqual(values, [42, 42, 42])

    async def test_filtered_update(self):
        rows = await TestModel.async_object.filter(name="Item1").aupdate(
            value=99
        )
        self.assertEqual(rows, 1)
        item = await TestModel.async_object.aget(name="Item1")
        self.assertEqual(item.value, 99)

    async def test_f_expression(self):
        await TestModel.async_object.all().aupdate(value=F("value") + 10)
        values = sorted(
            [obj.value async for obj in TestModel.async_object.all()]
        )
        self.assertEqual(values, [11, 12, 13])

    async def test_no_match_returns_zero(self):
        rows = await TestModel.async_object.filter(name="Nope").aupdate(
            value=0
        )
        self.assertEqual(rows, 0)

    async def test_sliced_raises(self):
        qs = TestModel.async_object.all()[:1]
        with self.assertRaises(TypeError):
            await qs.aupdate(value=1)
