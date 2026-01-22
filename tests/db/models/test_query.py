from unittest import TestCase

from django_async_backend.db.models.query import AsyncQuerySet


class TestAsyncQuerySet(TestCase):
    def test_not_allowed_sync_methods(self):
        qs = AsyncQuerySet()
        for method in [
            "get",
            "create",
            "bulk_create",
            "bulk_update",
            "get_or_create",
            "update_or_create",
            "aggregate",
            "count",
            "exists",
            "first",
            "last",
            "earliest",
            "latest",
            "in_bulk",
            "delete",
            "update",
            "iterator",
            "__iter__",
            "__len__",
            "__bool__",
            "__getitem__",
            "explain",
        ]:
            with self.subTest(f"test for {method} method"):
                with self.assertRaises(NotImplementedError):
                    getattr(qs, method)()

    def test_not_allowed_async_methods(self):
        qs = AsyncQuerySet()
        for method in [
            # Asynchronous methods
            "aget",
            "acreate",
            "abulk_create",
            "abulk_update",
            "aget_or_create",
            "aupdate_or_create",
            "aaggregate",
            "acount",
            "aexists",
            "afirst",
            "alast",
            "aearliest",
            "alatest",
            "ain_bulk",
            "adelete",
            "aupdate",
            "aiterator",
            "aexplain",
        ]:
            with self.subTest(f"test for {method} method"):
                with self.assertRaises(NotImplementedError):
                    getattr(qs, method)()
