from contextlib import ExitStack
from inspect import iscoroutinefunction
from unittest import mock

from django.db import DEFAULT_DB_ALIAS
from django.db.models import deletion as django_deletion
from django.db.models.deletion import DatabaseOnDelete
from test_app.models import (
    CascadeChildModel,
    DeleteModel,
    FastDeleteModel,
)

from django_async_backend.db.models.deletion import (
    _CASCADE,
    _DB_CASCADE,
    _DB_SET_DEFAULT,
    _DB_SET_NULL,
    _DO_NOTHING,
    _SKIP_COLLECTION,
    _SYNC_SKIP_COLLECTION,
    Collector,
    _get_candidate_relations_to_delete,
    _resolve_async_on_delete,
)
from django_async_backend.test import AsyncioTestCase


class TestDatabaseOnDeleteSentinels(AsyncioTestCase):
    """The DB-level sentinels are our own instances of Django's class, so the
    forced collector they hand back is awaitable."""

    async def test_sentinels_are_django_class_instances(self):
        for sentinel in (_DB_CASCADE, _DB_SET_DEFAULT, _DB_SET_NULL):
            with self.subTest(sentinel=str(sentinel)):
                self.assertIsInstance(sentinel, DatabaseOnDelete)

    async def test_db_cascade_forced_collector_is_async(self):
        # The bug this guards: Django's DB_CASCADE carries the *sync* CASCADE,
        # which collect() would await.
        self.assertIs(_DB_CASCADE.forced_collector, _CASCADE)
        self.assertTrue(iscoroutinefunction(_DB_CASCADE.forced_collector))
        self.assertFalse(
            iscoroutinefunction(django_deletion.DB_CASCADE.forced_collector)
        )

    async def test_only_db_cascade_has_a_forced_collector(self):
        self.assertIsNone(_DB_SET_DEFAULT.forced_collector)
        self.assertIsNone(_DB_SET_NULL.forced_collector)

    async def test_sentinels_keep_their_names(self):
        self.assertEqual(str(_DB_CASCADE), "DB_CASCADE")
        self.assertEqual(str(_DB_SET_DEFAULT), "DB_SET_DEFAULT")
        self.assertEqual(str(_DB_SET_NULL), "DB_SET_NULL")


class TestSkipCollectionSets(AsyncioTestCase):
    """Two sets, because the two call sites compare at different stages."""

    async def test_skip_collection_holds_resolved_handlers(self):
        self.assertEqual(
            _SKIP_COLLECTION,
            frozenset(
                [_DO_NOTHING, _DB_CASCADE, _DB_SET_DEFAULT, _DB_SET_NULL]
            ),
        )

    async def test_sync_skip_collection_is_djangos_own_set(self):
        # can_fast_delete() compares the raw on_delete, so it must use the
        # objects a user model actually declares.
        self.assertIs(_SYNC_SKIP_COLLECTION, django_deletion.SKIP_COLLECTION)
        for sentinel in (
            django_deletion.DB_CASCADE,
            django_deletion.DB_SET_DEFAULT,
            django_deletion.DB_SET_NULL,
            django_deletion.DO_NOTHING,
        ):
            with self.subTest(sentinel=str(sentinel)):
                self.assertIn(sentinel, _SYNC_SKIP_COLLECTION)

    async def test_cascade_is_not_skipped(self):
        self.assertNotIn(django_deletion.CASCADE, _SYNC_SKIP_COLLECTION)
        self.assertNotIn(_CASCADE, _SKIP_COLLECTION)


class TestResolveDatabaseOnDelete(AsyncioTestCase):
    async def test_sync_db_sentinels_map_to_async_ones(self):
        for sync, expected in (
            (django_deletion.DB_CASCADE, _DB_CASCADE),
            (django_deletion.DB_SET_DEFAULT, _DB_SET_DEFAULT),
            (django_deletion.DB_SET_NULL, _DB_SET_NULL),
        ):
            with self.subTest(sentinel=str(sync)):
                resolved = _resolve_async_on_delete(sync)

                self.assertIs(resolved, expected)
                self.assertIn(resolved, _SKIP_COLLECTION)


class TestForceCollection(AsyncioTestCase):
    async def asyncSetUp(self):
        await FastDeleteModel(name="Item1").async_save()

    async def test_force_collection_disables_fast_delete(self):
        forced = Collector(using=DEFAULT_DB_ALIAS, force_collection=True)
        default = Collector(using=DEFAULT_DB_ALIAS)

        self.assertFalse(forced.can_fast_delete(FastDeleteModel.objects.all()))
        self.assertTrue(default.can_fast_delete(FastDeleteModel.objects.all()))

    async def test_defaults_to_false(self):
        self.assertFalse(Collector(using=DEFAULT_DB_ALIAS).force_collection)

    async def test_db_sentinel_children_stay_fast_deletable(self):
        # can_fast_delete() compares the *raw* on_delete, so it must test
        # against Django's sentinels. Using our resolved set here silently
        # disables the optimisation instead of failing loudly.
        #
        # Fast delete requires *every* reverse relation to be skippable, so
        # all of them are pointed at the sentinel under test.
        relations = list(_get_candidate_relations_to_delete(DeleteModel._meta))
        collector = Collector(using=DEFAULT_DB_ALIAS)

        for sentinel in (
            django_deletion.DB_CASCADE,
            django_deletion.DB_SET_NULL,
            django_deletion.DB_SET_DEFAULT,
        ):
            with self.subTest(sentinel=str(sentinel)):
                with ExitStack() as stack:
                    for relation in relations:
                        stack.enter_context(
                            mock.patch.object(
                                relation.field.remote_field,
                                "on_delete",
                                sentinel,
                            )
                        )

                    self.assertTrue(
                        collector.can_fast_delete(DeleteModel.objects.all())
                    )


class TestCollectDatabaseOnDelete(AsyncioTestCase):
    """collect() against a DB_CASCADE field, with and without force."""

    async def asyncSetUp(self):
        self.parent = DeleteModel(name="Parent")
        await self.parent.async_save()
        await CascadeChildModel(name="Child", parent=self.parent).async_save()
        self.field = CascadeChildModel._meta.get_field("parent")

    def _as_db_sentinel(self, sentinel):
        return mock.patch.object(
            self.field.remote_field, "on_delete", sentinel
        )

    async def test_db_cascade_is_skipped_by_default(self):
        collector = Collector(using=DEFAULT_DB_ALIAS)

        with self._as_db_sentinel(django_deletion.DB_CASCADE):
            await collector.collect([self.parent])

        self.assertNotIn(CascadeChildModel, collector.data)

    async def test_db_cascade_collects_when_forced(self):
        collector = Collector(using=DEFAULT_DB_ALIAS, force_collection=True)

        with self._as_db_sentinel(django_deletion.DB_CASCADE):
            await collector.collect([self.parent])

        # The forced collector ran, and it was the async _CASCADE -- awaiting
        # Django's sync CASCADE would have raised instead.
        self.assertIn(CascadeChildModel, collector.data)
        self.assertEqual(len(collector.data[CascadeChildModel]), 1)

    async def test_sentinels_without_forced_collector_stay_skipped(self):
        for sentinel in (
            django_deletion.DB_SET_NULL,
            django_deletion.DB_SET_DEFAULT,
        ):
            with self.subTest(sentinel=str(sentinel)):
                collector = Collector(
                    using=DEFAULT_DB_ALIAS, force_collection=True
                )

                with self._as_db_sentinel(sentinel):
                    await collector.collect([self.parent])

                self.assertNotIn(CascadeChildModel, collector.data)
