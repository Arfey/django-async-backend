from django.db import DEFAULT_DB_ALIAS
from django.db.models.deletion import ProtectedError, RestrictedError
from django.db.models.signals import post_delete, pre_delete
from test_app.models import (
    ChildModel,
    DeleteAuthorModel,
    DeleteBookModel,
    DeleteCascadeRestrictModel,
    DeleteProtectedModel,
    DeleteRestrictedModel,
    DeleteReviewModel,
    DeleteSetDefaultModel,
    DeleteSetNullModel,
    DeleteTaggedModel,
    GetOrCreateModel,
    TestModel,
)

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
    AsyncioTransactionTestCase,
)


class TestADelete(AsyncioTestCase):
    async def test_deletes_all(self):
        await DeleteAuthorModel.async_objects.acreate(name="A")
        await DeleteAuthorModel.async_objects.acreate(name="B")

        total, per_model = await DeleteAuthorModel.async_objects.all().adelete()

        self.assertEqual(total, 2)
        self.assertEqual(per_model, {"test_app.DeleteAuthorModel": 2})
        self.assertFalse(await DeleteAuthorModel.async_objects.aexists())

    async def test_filtered_delete(self):
        await DeleteAuthorModel.async_objects.acreate(name="Keep")
        await DeleteAuthorModel.async_objects.acreate(name="Drop")

        total, _ = await DeleteAuthorModel.async_objects.filter(
            name="Drop"
        ).adelete()

        self.assertEqual(total, 1)
        self.assertTrue(
            await DeleteAuthorModel.async_objects.filter(name="Keep").aexists()
        )

    async def test_empty_queryset(self):
        total, per_model = await DeleteAuthorModel.async_objects.filter(
            name="Missing"
        ).adelete()

        self.assertEqual(total, 0)
        self.assertEqual(per_model, {})

    async def test_sliced_queryset_raises(self):
        with self.assertRaises(TypeError):
            await DeleteAuthorModel.async_objects.all()[:1].adelete()

    async def test_alters_data(self):
        self.assertIs(
            DeleteAuthorModel.async_objects.all().adelete.alters_data, True
        )

    async def test_cascade(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        book1 = await DeleteBookModel.async_objects.acreate(
            name="B1", author=author
        )
        await DeleteBookModel.async_objects.acreate(name="B2", author=author)
        await DeleteReviewModel.async_objects.acreate(name="R1", book=book1)

        total, per_model = await DeleteAuthorModel.async_objects.filter(
            pk=author.pk
        ).adelete()

        self.assertEqual(total, 4)
        self.assertEqual(per_model["test_app.DeleteAuthorModel"], 1)
        self.assertEqual(per_model["test_app.DeleteBookModel"], 2)
        self.assertEqual(per_model["test_app.DeleteReviewModel"], 1)
        self.assertFalse(await DeleteBookModel.async_objects.aexists())
        self.assertFalse(await DeleteReviewModel.async_objects.aexists())

    async def test_cascade_runs_on_async_connection(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        await DeleteBookModel.async_objects.acreate(name="B", author=author)

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            await DeleteAuthorModel.async_objects.filter(
                pk=author.pk
            ).adelete()

        sqls = [query["sql"] for query in ctx.captured_queries]
        self.assertTrue(
            any("delete_book_model" in sql and sql.startswith("SELECT") for sql in sqls)
        )
        self.assertTrue(
            any(sql.startswith("DELETE") and "delete_book_model" in sql for sql in sqls)
        )
        self.assertTrue(
            any(sql.startswith("DELETE") and "delete_author_model" in sql for sql in sqls)
        )

    async def test_self_referential_cascade(self):
        parent = await TestModel.async_objects.acreate(name="parent")
        await TestModel.async_objects.acreate(name="child", relative=parent)

        total, _ = await TestModel.async_objects.filter(
            pk=parent.pk
        ).adelete()

        self.assertEqual(total, 2)
        self.assertFalse(await TestModel.async_objects.aexists())

    async def test_protect_raises(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        await DeleteProtectedModel.async_objects.acreate(author=author)

        with self.assertRaises(ProtectedError):
            await DeleteAuthorModel.async_objects.filter(
                pk=author.pk
            ).adelete()

    async def test_restrict_raises(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        await DeleteRestrictedModel.async_objects.acreate(author=author)

        with self.assertRaises(RestrictedError):
            await DeleteAuthorModel.async_objects.filter(
                pk=author.pk
            ).adelete()

    async def test_restrict_waived_when_cascade_collects(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        await DeleteCascadeRestrictModel.async_objects.acreate(
            cascade_author=author, restrict_author=author
        )

        total, per_model = await DeleteAuthorModel.async_objects.filter(
            pk=author.pk
        ).adelete()

        self.assertEqual(total, 2)
        self.assertEqual(per_model["test_app.DeleteCascadeRestrictModel"], 1)

    async def test_set_null(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        ref = await DeleteSetNullModel.async_objects.acreate(author=author)

        await DeleteAuthorModel.async_objects.filter(pk=author.pk).adelete()

        ref = await DeleteSetNullModel.async_objects.aget(pk=ref.pk)
        self.assertIsNone(ref.author_id)

    async def test_set_default(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        ref = await DeleteSetDefaultModel.async_objects.acreate(author=author)

        await DeleteAuthorModel.async_objects.filter(pk=author.pk).adelete()

        ref = await DeleteSetDefaultModel.async_objects.aget(pk=ref.pk)
        self.assertIsNone(ref.author_id)

    async def test_fast_delete_uses_single_query(self):
        # No reverse relations and no signal receivers: the whole
        # queryset is deleted with one DELETE, no SELECT.
        await GetOrCreateModel.async_objects.acreate(name="A", code="a")
        await GetOrCreateModel.async_objects.acreate(name="B", code="b")

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            total, _ = await GetOrCreateModel.async_objects.all().adelete()

        self.assertEqual(total, 2)
        self.assertEqual(len(ctx.captured_queries), 1)
        self.assertTrue(ctx.captured_queries[0]["sql"].startswith("DELETE"))

    async def test_pre_and_post_delete_signals(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        seen = []

        def pre_receiver(sender, instance, origin, **kwargs):
            seen.append(("pre", instance.pk, origin))

        def post_receiver(sender, instance, origin, **kwargs):
            seen.append(("post", instance.pk, origin))

        pre_delete.connect(pre_receiver, sender=DeleteAuthorModel)
        post_delete.connect(post_receiver, sender=DeleteAuthorModel)
        self.addCleanup(
            pre_delete.disconnect, pre_receiver, sender=DeleteAuthorModel
        )
        self.addCleanup(
            post_delete.disconnect, post_receiver, sender=DeleteAuthorModel
        )

        queryset = DeleteAuthorModel.async_objects.filter(pk=author.pk)
        await queryset.adelete()

        self.assertEqual(
            [(kind, pk) for kind, pk, _ in seen],
            [("pre", author.pk), ("post", author.pk)],
        )
        for _, _, origin in seen:
            self.assertIs(origin, queryset)

    async def test_signal_receivers_disable_fast_delete(self):
        # With a receiver connected, GetOrCreateModel can't be
        # fast-deleted and goes through the collect/signals path.
        await GetOrCreateModel.async_objects.acreate(name="A", code="a")
        seen = []

        def receiver(sender, instance, **kwargs):
            seen.append(instance.pk)

        post_delete.connect(receiver, sender=GetOrCreateModel)
        self.addCleanup(
            post_delete.disconnect, receiver, sender=GetOrCreateModel
        )

        total, _ = await GetOrCreateModel.async_objects.all().adelete()

        self.assertEqual(total, 1)
        self.assertEqual(len(seen), 1)

    async def test_multi_table_inheritance_raises(self):
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            await cursor.execute(
                "INSERT INTO parent_model (parent_value) VALUES (1) "
                "RETURNING id"
            )
            parent_id = (await cursor.fetchone())[0]
            await cursor.execute(
                "INSERT INTO child_model (parentmodel_ptr_id, child_value) "
                "VALUES (%s, 2)",
                [parent_id],
            )

        with self.assertRaises(NotImplementedError):
            await ChildModel.async_objects.all().adelete()

    async def test_generic_relation_raises(self):
        await DeleteTaggedModel.async_objects.acreate(name="A")

        with self.assertRaises(NotImplementedError):
            await DeleteTaggedModel.async_objects.all().adelete()


class TestADeleteOutsideTransaction(AsyncioTransactionTestCase):
    # AsyncioTransactionTestCase runs in autocommit; adelete must open
    # (and commit) its own transaction around the cascade.

    async def asyncTearDown(self):
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            await cursor.execute("DELETE FROM delete_review_model;")
            await cursor.execute("DELETE FROM delete_book_model;")
            await cursor.execute("DELETE FROM delete_author_model;")
            await cursor.execute("DELETE FROM get_or_create_model;")

    async def test_cascade_delete_commits(self):
        author = await DeleteAuthorModel.async_objects.acreate(name="A")
        await DeleteBookModel.async_objects.acreate(name="B", author=author)

        total, _ = await DeleteAuthorModel.async_objects.filter(
            pk=author.pk
        ).adelete()

        self.assertEqual(total, 2)
        self.assertFalse(await DeleteAuthorModel.async_objects.aexists())
        self.assertFalse(await DeleteBookModel.async_objects.aexists())

    async def test_fast_delete_commits(self):
        await GetOrCreateModel.async_objects.acreate(name="A", code="a")

        total, _ = await GetOrCreateModel.async_objects.all().adelete()

        self.assertEqual(total, 1)
        self.assertFalse(await GetOrCreateModel.async_objects.aexists())
