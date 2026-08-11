from django.db import DEFAULT_DB_ALIAS
from django.db.models import F
from django.test import TestCase
from test_app.models import TestModel

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)


class TestMock(TestCase):
    def test_mock(self):
        pass


class TestAIter(AsyncioTestCase):
    async def test_aiter(self):
        await TestModel(
            name="Test1",
        ).async_save()
        await TestModel(name="Test2").async_save()

        results = [obj async for obj in TestModel.async_objects.all()]

        self.assertEqual(len(results), 2, "Should iterate over 2 objects")
        self.assertEqual(
            results[0].name, "Test1", "First object name should be 'Test1'"
        )
        self.assertEqual(
            results[1].name,
            "Test2",
            "Second object name should be 'Test2'",
        )

    async def test_aiter_no_objects(self):
        results = [obj async for obj in TestModel.async_objects.all()]
        self.assertEqual(
            len(results),
            0,
            "Should iterate over 0 objects when none exist",
        )

    async def test_aiter_with_filter(self):
        await TestModel(
            name="Test1",
        ).async_save()

        results = [
            obj async for obj in TestModel.async_objects.filter(name="Test1")
        ]

        self.assertEqual(len(results), 1, "Should iterate over 1 object")
        self.assertEqual(
            results[0].name, "Test1", "First object name should be 'Test1'"
        )


class TestAIterKnownRelatedObjects(AsyncioTestCase):
    """Iterating populates the fields listed in _known_related_objects.

    There is no async related manager, so _known_related_objects is seeded the
    way a related manager would seed it on the sync side.
    """

    async def asyncSetUp(self):
        self.relative = TestModel(name="Relative", value=0)
        await self.relative.async_save()
        self.child = TestModel(name="Child", value=1, relative=self.relative)
        await self.child.async_save()
        self.field = TestModel._meta.get_field("relative")

    def seed(self, queryset, *objs):
        queryset._known_related_objects = {
            self.field: {obj.pk: obj for obj in objs}
        }
        return queryset

    async def test_populates_known_related_object(self):
        queryset = self.seed(
            TestModel.async_objects.filter(name="Child"), self.relative
        )

        obj = [o async for o in queryset][0]

        self.assertIs(
            obj.relative,
            self.relative,
            "The known related object should be attached to the instance",
        )

    async def test_populated_relation_costs_no_extra_query(self):
        queryset = self.seed(
            TestModel.async_objects.filter(name="Child"), self.relative
        )

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [o async for o in queryset][0]
            _ = obj.relative

        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "Reading the populated relation should not query the database",
        )

    async def test_deferred_fk_attname_is_not_populated(self):
        """When the FK attname is deferred, iteration skips the object instead
        of triggering a query to load relative_id.
        """
        queryset = self.seed(
            TestModel.async_objects.filter(name="Child")._only("id", "name"),
            self.relative,
        )

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [o async for o in queryset][0]

        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "Skipping a deferred FK should not add a query",
        )
        self.assertNotIn(
            "relative",
            obj._state.fields_cache,
            "A deferred FK attname should leave the relation unpopulated",
        )
        self.assertIn(
            "relative_id",
            obj.get_deferred_fields(),
            "relative_id should still be deferred, not loaded",
        )

    async def test_select_related_object_is_not_overwritten(self):
        """A relation already loaded by select_related() wins over the known
        related object.
        """
        decoy = TestModel(name="Decoy", value=99)
        await decoy.async_save()

        queryset = self.seed(
            TestModel.async_objects.filter(name="Child").select_related(
                "relative"
            ),
            decoy,
        )

        obj = [o async for o in queryset][0]

        self.assertIsNot(
            obj.relative,
            decoy,
            "select_related() should not be overwritten",
        )
        self.assertEqual(obj.relative.pk, self.relative.pk)

    async def test_missing_known_related_object_is_ignored(self):
        """A pk that is absent from the mapping is skipped, as happens in
        qs1 | qs2 scenarios.
        """
        other = TestModel(name="Other", value=2)
        await other.async_save()

        queryset = self.seed(
            TestModel.async_objects.filter(name="Child"), other
        )

        obj = [o async for o in queryset][0]

        self.assertNotIn(
            "relative",
            obj._state.fields_cache,
            "An unmatched pk should leave the relation unpopulated",
        )

    async def test_annotations_are_set_alongside_known_related_objects(self):
        queryset = self.seed(
            TestModel.async_objects.filter(name="Child").annotate(
                doubled=F("value") * 2
            ),
            self.relative,
        )

        obj = [o async for o in queryset][0]

        self.assertEqual(
            obj.doubled, 2, "Annotations should be set on the row"
        )
        self.assertIs(
            obj.relative,
            self.relative,
            "Annotations should not disturb known related objects",
        )

    async def test_null_fk_is_not_populated(self):
        queryset = self.seed(
            TestModel.async_objects.filter(name="Relative"), self.relative
        )

        obj = [o async for o in queryset][0]

        self.assertNotIn(
            "relative",
            obj._state.fields_cache,
            "A NULL FK should not match any known related object",
        )
