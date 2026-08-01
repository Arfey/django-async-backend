from django.core.exceptions import SynchronousOnlyOperation
from django.db import (
    DEFAULT_DB_ALIAS,
    NotSupportedError,
)
from django.db.models import (
    FilteredRelation,
    Q,
)
from test_app.models import TestModel

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)


class TestOnly(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel(name="Item1", value=1).async_save()
        await TestModel(name="Item2", value=2).async_save()

    async def test_not_exposed_on_manager(self):
        """_only() is private, so BaseManager.from_queryset() does not copy it
        onto AsyncManager. It is reachable through a queryset only.
        """
        self.assertFalse(hasattr(TestModel.async_objects, "_only"))
        self.assertTrue(hasattr(TestModel.async_objects.all(), "_only"))

    async def test_selects_only_requested_columns(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            [
                obj
                async for obj in TestModel.async_objects.all()
                ._only("id", "name")
                .order_by("name")
            ]

        sql = ctx.captured_queries[-1]["sql"]

        self.assertIn('"name"', sql, "Requested column should be selected")
        self.assertNotIn(
            '"value"', sql, "Unrequested column should not be selected"
        )
        self.assertNotIn(
            '"relative_id"',
            sql,
            "Unrequested column should not be selected",
        )

    async def test_unrequested_fields_are_deferred(self):
        obj = [
            o
            async for o in TestModel.async_objects.all()
            ._only("id", "name")
            .filter(name="Item1")
        ][0]

        self.assertEqual(
            obj.get_deferred_fields(),
            {"value", "relative_id"},
            "Fields outside _only() should be deferred",
        )

    async def test_returns_clone_without_touching_source(self):
        source = TestModel.async_objects.all()

        clone = source._only("id", "name")

        self.assertIsNot(clone, source, "_only() should return a clone")
        self.assertEqual(
            source.query.deferred_loading,
            (frozenset(), True),
            "Source queryset should be left untouched",
        )
        self.assertEqual(
            clone.query.deferred_loading, (frozenset({"id", "name"}), False)
        )

    async def test_chaining_matches_sync_only(self):
        clone = TestModel.async_objects.all()._only("id", "name")._only("id")
        expected = TestModel.objects.only("id", "name").only("id")

        self.assertEqual(
            clone.query.deferred_loading,
            expected.query.deferred_loading,
            "_only() should chain exactly like Django's only()",
        )

    async def test_after_values_raises_type_error(self):
        with self.assertRaises(TypeError):
            TestModel.async_objects.values("name")._only("name")

    async def test_after_values_list_raises_type_error(self):
        with self.assertRaises(TypeError):
            TestModel.async_objects.values_list("name")._only("name")

    async def test_none_argument_raises_type_error(self):
        with self.assertRaises(TypeError):
            TestModel.async_objects.all()._only(None)

    async def test_filtered_relation_raises_value_error(self):
        qs = TestModel.async_objects.annotate(
            rel=FilteredRelation(
                "relatives", condition=Q(relatives__name="Item2")
            )
        )

        with self.assertRaises(ValueError):
            qs._only("rel")

    async def test_combined_queryset_raises_not_supported(self):
        combined = TestModel.async_objects.filter(name="Item1").union(
            TestModel.async_objects.filter(name="Item2")
        )

        with self.assertRaises(NotSupportedError):
            combined._only("name")

    async def test_deferred_field_access_is_sync_unsafe(self):
        """Reading a deferred field falls back to Django's sync
        refresh_from_db, which is why _only() is private rather than a public
        only().
        """
        obj = [
            o
            async for o in TestModel.async_objects.all()
            ._only("id", "name")
            .filter(name="Item1")
        ][0]

        with self.assertRaises(SynchronousOnlyOperation):
            obj.value
