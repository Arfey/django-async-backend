from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestQuerySetRepr(AsyncioTestCase):

    async def test_repr_does_not_query(self):
        """repr() shows the query, it never fetches results."""
        qs = TestModel.async_objects.all()

        self.assertEqual(repr(qs), "<QuerySet [%s]>" % qs.query)
        self.assertIsNone(qs._result_cache)

    async def test_repr_includes_filters(self):
        qs = TestModel.async_objects.filter(name="Item1")

        self.assertIn("QuerySet", repr(qs))
        self.assertIn("Item1", repr(qs))

    async def test_repr_on_queryset_with_results(self):
        """Even with a populated cache, repr() shows the query."""
        await TestModel.async_objects.acreate(name="Item1")
        qs = TestModel.async_objects.all()
        await qs._fetch_all()

        self.assertEqual(repr(qs), "<QuerySet [%s]>" % qs.query)
