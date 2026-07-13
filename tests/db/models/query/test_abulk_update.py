from django.db import (
    DEFAULT_DB_ALIAS,
    IntegrityError,
)
from django.db.models import F
from test_app.models import (
    ChildModel,
    TestModel,
)

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncioTestCase,
    AsyncioTransactionTestCase,
)


class TestABulkUpdate(AsyncioTestCase):
    async def asyncSetUp(self):
        self.item1 = await TestModel.async_objects.acreate(
            name="Item1", value=1
        )
        self.item2 = await TestModel.async_objects.acreate(
            name="Item2", value=2
        )
        self.item3 = await TestModel.async_objects.acreate(
            name="Item3", value=3
        )

    async def _values_by_name(self):
        return {
            obj.name: obj.value async for obj in TestModel.async_objects.all()
        }

    async def test_returns_row_count(self):
        self.item1.value = 10
        self.item2.value = 20
        self.item3.value = 30
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item1, self.item2, self.item3], ["value"]
        )
        self.assertEqual(rows, 3)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 10, "Item2": 20, "Item3": 30},
        )

    async def test_multiple_fields(self):
        self.item1.name = "New1"
        self.item1.value = 100
        self.item2.name = "New2"
        self.item2.value = 200
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item1, self.item2], ["name", "value"]
        )
        self.assertEqual(rows, 2)
        self.assertEqual(
            await self._values_by_name(),
            {"New1": 100, "New2": 200, "Item3": 3},
        )

    async def test_batch_size_forces_multiple_batches(self):
        # batch_size=1 produces one UPDATE (one awaited aupdate) per object,
        # exercising the per-batch loop inside `async with async_atomic`.
        self.item1.value = 7
        self.item2.value = 7
        self.item3.value = 7
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item1, self.item2, self.item3], ["value"], batch_size=1
        )
        self.assertEqual(rows, 3)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 7, "Item2": 7, "Item3": 7},
        )

    async def test_uneven_batches(self):
        # batch_size=2 over 3 objects exercises the uneven final batch.
        self.item1.value = 9
        self.item2.value = 9
        self.item3.value = 9
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item1, self.item2, self.item3], ["value"], batch_size=2
        )
        self.assertEqual(rows, 3)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 9, "Item2": 9, "Item3": 9},
        )

    async def test_f_expression_values(self):
        # An attribute set to a resolvable expression (F) must pass through
        # unwrapped into the CASE/WHEN rather than being treated as a literal.
        self.item1.value = F("value") + 100
        self.item2.value = F("value") + 100
        self.item3.value = F("value") + 100
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item1, self.item2, self.item3], ["value"]
        )
        self.assertEqual(rows, 3)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 101, "Item2": 102, "Item3": 103},
        )

    async def test_empty_objs_returns_zero(self):
        rows = await TestModel.async_objects.all().abulk_update([], ["value"])
        self.assertEqual(rows, 0)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 1, "Item2": 2, "Item3": 3},
        )

    async def test_manager_level_exposure(self):
        # abulk_update is surfaced on the manager, not only on querysets.
        self.item1.value = 50
        rows = await TestModel.async_objects.abulk_update(
            [self.item1], ["value"]
        )
        self.assertEqual(rows, 1)
        item = await TestModel.async_objects.aget(name="Item1")
        self.assertEqual(item.value, 50)

    async def test_duplicate_objects_count_once_per_batch(self):
        # Duplicates within one batch collapse into a single row match (the
        # first When wins), so the same pk counts once toward the row count.
        self.item1.value = 42
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item1, self.item1], ["value"]
        )
        self.assertEqual(rows, 1)
        item = await TestModel.async_objects.aget(name="Item1")
        self.assertEqual(item.value, 42)

    async def test_foreign_key_field(self):
        # A saved related object passes _prepare_related_fields_for_save and
        # updates the FK column.
        self.item2.relative = self.item1
        rows = await TestModel.async_objects.all().abulk_update(
            [self.item2], ["relative"]
        )
        self.assertEqual(rows, 1)
        item = await TestModel.async_objects.aget(name="Item2")
        self.assertEqual(item.relative_id, self.item1.pk)

    async def test_unsaved_related_object_raises(self):
        self.item2.relative = TestModel(name="Unsaved", value=0)
        with self.assertRaises(ValueError):
            await TestModel.async_objects.all().abulk_update(
                [self.item2], ["relative"]
            )

    async def test_only_matching_queryset_rows_updated(self):
        # The batched UPDATE runs through the queryset's own filters, so
        # objects outside the queryset are not touched.
        self.item1.value = 111
        self.item2.value = 222
        rows = await TestModel.async_objects.filter(
            name="Item1"
        ).abulk_update([self.item1, self.item2], ["value"])
        self.assertEqual(rows, 1)
        self.assertEqual(
            await self._values_by_name(),
            {"Item1": 111, "Item2": 2, "Item3": 3},
        )


class TestABulkUpdateValidation(AsyncioTestCase):
    async def asyncSetUp(self):
        self.item1 = await TestModel.async_objects.acreate(
            name="Item1", value=1
        )

    async def test_no_fields_raises(self):
        with self.assertRaises(ValueError):
            await TestModel.async_objects.all().abulk_update([self.item1], [])

    async def test_non_positive_batch_size_raises(self):
        for batch_size in (0, -1):
            with self.assertRaises(ValueError):
                await TestModel.async_objects.all().abulk_update(
                    [self.item1], ["value"], batch_size=batch_size
                )

    async def test_object_without_pk_raises(self):
        unsaved = TestModel(name="Unsaved", value=1)
        with self.assertRaises(ValueError):
            await TestModel.async_objects.all().abulk_update(
                [unsaved], ["value"]
            )

    async def test_pk_field_raises(self):
        with self.assertRaises(ValueError):
            await TestModel.async_objects.all().abulk_update(
                [self.item1], ["id"]
            )

    async def test_non_concrete_field_raises(self):
        with self.assertRaises(ValueError):
            await TestModel.async_objects.all().abulk_update(
                [self.item1], ["relatives"]
            )

    async def test_parent_pk_field_raises(self):
        child = await ChildModel.async_objects.acreate(child_value=1)
        with self.assertRaises(ValueError):
            await ChildModel.async_objects.all().abulk_update([child], ["id"])


class TestABulkUpdateRollback(AsyncioTransactionTestCase):
    # AsyncioTransactionTestCase commits for real (it does not wrap the test
    # in a rolled-back transaction), so the atomic block abulk_update opens
    # is the outermost transaction and its rollback is observable.

    async def asyncSetUp(self):
        self.item1 = await TestModel.async_objects.acreate(
            name="Item1", value=1
        )
        self.item2 = await TestModel.async_objects.acreate(
            name="Item2", value=2
        )
        self.item3 = await TestModel.async_objects.acreate(
            name="Item3", value=3
        )

    async def asyncTearDown(self):
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            await cursor.execute("DELETE FROM test_model;")

    async def test_failed_batch_rolls_back_earlier_batches(self):
        # batch_size=1 turns each object into its own UPDATE inside one
        # atomic block. The third rename collides with the first one's new
        # unique name, so its batch raises -- the atomic must then roll back
        # the two already-applied batches.
        self.item1.name = "rollback-A"
        self.item2.name = "rollback-B"
        self.item3.name = "rollback-A"
        with self.assertRaises(IntegrityError):
            await TestModel.async_objects.all().abulk_update(
                [self.item1, self.item2, self.item3], ["name"], batch_size=1
            )
        names = sorted(
            [obj.name async for obj in TestModel.async_objects.all()]
        )
        self.assertEqual(names, ["Item1", "Item2", "Item3"])
