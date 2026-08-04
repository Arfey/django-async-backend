from django.db.models import (
    Exists,
    OuterRef,
    Subquery,
)
from test_app.models import TestModel

from django_async_backend.db.models.sql.query import Query as AsyncQuery
from django_async_backend.test import AsyncioTestCase


class TestQuerySetResolveExpression(AsyncioTestCase):
    """`resolve_expression` is what lets a queryset be used as a subquery."""

    async def test_returns_async_query(self):
        """The resolved subquery must stay on the async Query class."""
        inner = TestModel.async_objects.filter(name="Item1")
        outer = TestModel.async_objects.all()

        resolved = inner.resolve_expression(outer.query)

        self.assertIsInstance(resolved, AsyncQuery)
        self.assertTrue(resolved.subquery)

    async def test_preserves_db(self):
        inner = TestModel.async_objects.using("default").filter(name="Item1")
        outer = TestModel.async_objects.all()

        resolved = inner.resolve_expression(outer.query)

        self.assertEqual(resolved._db, "default")

    async def test_queryset_only(self):
        """It stays off the manager: Manager.from_queryset() skips it."""
        self.assertTrue(
            TestModel.async_objects.all().resolve_expression.queryset_only
        )
        self.assertFalse(
            hasattr(TestModel.async_objects, "resolve_expression")
        )

    async def test_in_subquery(self):
        await TestModel.async_objects.acreate(name="Item1", value=1)
        await TestModel.async_objects.acreate(name="Item2", value=2)

        inner = TestModel.async_objects.filter(value=1).values("pk")
        qs = TestModel.async_objects.filter(pk__in=inner)

        self.assertEqual([obj.name async for obj in qs], ["Item1"])

    async def test_exists_subquery(self):
        await TestModel.async_objects.acreate(name="Item1", value=1)
        await TestModel.async_objects.acreate(name="Item2", value=2)

        inner = TestModel.async_objects.filter(name=OuterRef("name"), value=1)
        qs = TestModel.async_objects.annotate(has=Exists(inner)).filter(
            has=True
        )

        self.assertEqual([obj.name async for obj in qs], ["Item1"])

    async def test_subquery_annotation(self):
        await TestModel.async_objects.acreate(name="Item1", value=1)

        inner = TestModel.async_objects.filter(name=OuterRef("name")).values(
            "value"
        )
        qs = TestModel.async_objects.annotate(v=Subquery(inner[:1]))

        self.assertEqual(
            [(obj.name, obj.v) async for obj in qs], [("Item1", 1)]
        )

    async def test_resolve_does_not_query(self):
        await TestModel.async_objects.acreate(name="Item1", value=1)
        inner = TestModel.async_objects.filter(value=1)
        outer = TestModel.async_objects.all()

        inner.resolve_expression(outer.query)

        self.assertIsNone(inner._result_cache)
