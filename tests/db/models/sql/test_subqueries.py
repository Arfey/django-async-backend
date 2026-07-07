from unittest import mock

from django.db import DEFAULT_DB_ALIAS
from django.db.models import F
from test_app.models import TestModel

from django_async_backend.db.models import sql as async_sql
from django_async_backend.test import AsyncioTestCase


class TestUpdateBatch(AsyncioTestCase):
    async def asyncSetUp(self):
        self.item1 = TestModel(name="Item1", value=1)
        await self.item1.async_save()
        self.item2 = TestModel(name="Item2", value=2)
        await self.item2.async_save()
        self.item3 = TestModel(name="Item3", value=3)
        await self.item3.async_save()

    async def _values_by_name(self):
        return {
            obj.name: obj.value async for obj in TestModel.async_objects.all()
        }

    async def test_updates_only_listed_pks(self):
        query = async_sql.UpdateQuery(TestModel)
        await query.update_batch(
            [self.item1.pk, self.item3.pk],
            {"value": 99},
            DEFAULT_DB_ALIAS,
        )
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 99, "Item2": 2, "Item3": 99},
        )

    async def test_updates_all_listed_pks(self):
        query = async_sql.UpdateQuery(TestModel)
        await query.update_batch(
            [self.item1.pk, self.item2.pk, self.item3.pk],
            {"value": 7},
            DEFAULT_DB_ALIAS,
        )
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 7, "Item2": 7, "Item3": 7},
        )

    async def test_empty_pk_list_is_noop(self):
        query = async_sql.UpdateQuery(TestModel)
        await query.update_batch([], {"value": 0}, DEFAULT_DB_ALIAS)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 1, "Item2": 2, "Item3": 3},
        )

    async def test_f_expression(self):
        query = async_sql.UpdateQuery(TestModel)
        await query.update_batch(
            [self.item1.pk, self.item2.pk, self.item3.pk],
            {"value": F("value") + 10},
            DEFAULT_DB_ALIAS,
        )
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 11, "Item2": 12, "Item3": 13},
        )

    async def test_batches_across_chunk_boundary(self):
        # Force more than one iteration of the chunking loop so the per-batch
        # clear_where()/add_filter() path is exercised for every pk.
        pk_list = [self.item1.pk, self.item2.pk, self.item3.pk]
        query = async_sql.UpdateQuery(TestModel)
        with mock.patch(
            "django_async_backend.db.models.sql.subqueries."
            "GET_ITERATOR_CHUNK_SIZE",
            1,
        ):
            await query.update_batch(pk_list, {"value": 5}, DEFAULT_DB_ALIAS)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 5, "Item2": 5, "Item3": 5},
        )
