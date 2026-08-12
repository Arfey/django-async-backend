from test_app.models import TestModel

from django_async_backend.db.models.query import PreventQuerySetCloning
from django_async_backend.test import AsyncioTestCase


class TestQuerySetCloning(AsyncioTestCase):
    """_disable_cloning() opts a QuerySet into in-place mutation, so _chain()
    returns the same instance instead of a clone.
    """

    async def asyncSetUp(self):
        for name in ("first", "second", "third", "fourth"):
            await TestModel(name=name).async_save()

    async def test_cloning_is_enabled_by_default(self):
        queryset = TestModel.async_objects.all()

        self.assertIs(
            queryset._cloning_enabled,
            True,
            "A new QuerySet should clone on every operation",
        )
        self.assertIsNot(
            queryset.filter(name="first"),
            queryset,
            "filter() should return a new QuerySet by default",
        )

    async def test_context_manager(self):
        """_avoid_cloning() makes modifications apply to the original
        QuerySet.
        """
        queryset = TestModel.async_objects.all()

        with queryset._avoid_cloning():
            queryset2 = queryset.filter(name__in={"first", "second"}).exclude(
                name="second"
            )

        self.assertIs(queryset2, queryset)

        queryset3 = queryset2.exclude(name__in={"third", "fourth"})

        # queryset3 is not a mutation of queryset2 (which is actually also
        # queryset) but a new instance entirely.
        self.assertIsNot(queryset3, queryset)
        self.assertIsNot(queryset3, queryset2)

    async def test_context_manager_returns_the_queryset(self):
        queryset = TestModel.async_objects.all()

        with queryset._avoid_cloning() as entered:
            self.assertIs(
                entered,
                queryset,
                "__enter__() should return the QuerySet itself",
            )

    async def test_avoid_cloning_returns_context_manager(self):
        queryset = TestModel.async_objects.all()

        self.assertIsInstance(
            queryset._avoid_cloning(),
            PreventQuerySetCloning,
            "_avoid_cloning() should not be a generator based manager",
        )

    async def test_cloning_restored_on_exception(self):
        queryset = TestModel.async_objects.all()

        with self.assertRaises(RuntimeError):
            with queryset._avoid_cloning():
                raise RuntimeError("boom")

        self.assertIs(
            queryset._cloning_enabled,
            True,
            "__exit__() should re-enable cloning even when the body raises",
        )

    async def test_explicit_toggling(self):
        queryset = TestModel.async_objects.filter(name__in={"first", "second"})
        queryset2 = queryset._disable_cloning()

        # The _disable_cloning() method doesn't return a new QuerySet, but
        # toggles the value on the current instance. queryset2 can be ignored.
        self.assertIs(queryset2, queryset)

        queryset3 = queryset.filter(name__in={"first", "second"})
        queryset3 = queryset3.exclude(name="second")
        returned = queryset3._enable_cloning()

        # These are still both references to the same QuerySet, despite
        # re-binding as if they were normal chained operations providing new
        # QuerySet instances.
        self.assertIs(queryset3, queryset)
        self.assertIs(
            returned,
            queryset,
            "_enable_cloning() should return the same instance too",
        )

        queryset3 = queryset3.filter(name="second")

        # Cloning has been re-enabled so subsequent operations yield a new
        # QuerySet. queryset3 is now all of the filters applied to queryset
        # plus an additional filter.
        self.assertIsNot(queryset3, queryset)

    async def test_in_place_mutation_accumulates_filters(self):
        queryset = TestModel.async_objects.all()

        with queryset._avoid_cloning():
            queryset.filter(name__in={"first", "second"}).exclude(
                name="second"
            )

        results = [obj async for obj in queryset]

        self.assertEqual(
            [obj.name for obj in results],
            ["first"],
            "Both filters should have been applied to the original QuerySet",
        )

    async def test_sticky_filter_is_applied_without_cloning(self):
        """_chain() still consumes a sticky filter when it returns the same
        instance rather than a clone.
        """
        queryset = TestModel.async_objects.all()._next_is_sticky()

        with queryset._avoid_cloning():
            queryset2 = queryset.filter(name="first")

        self.assertIs(queryset2, queryset)
        self.assertIs(
            queryset2.query.filter_is_sticky,
            True,
            "The sticky filter should have been transferred to the query",
        )
        self.assertIs(
            queryset2._sticky_filter,
            False,
            "The sticky flag should be consumed by _chain()",
        )
