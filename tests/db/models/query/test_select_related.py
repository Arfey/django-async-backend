import gc
import unittest
import warnings

from django.core.exceptions import FieldError
from django.db import (
    DEFAULT_DB_ALIAS,
    NotSupportedError,
)
from django.db.models import (
    FilteredRelation,
    Q,
)
from django.db.models.functions import Length
from test_app.models import (
    DepthLevel0Model,
    DepthLevel1Model,
    DepthLevel2Model,
    DepthLevel3Model,
    DepthLevel4Model,
    DepthLevel5Model,
    DepthLevel6Model,
    DepthLevel7Model,
    SelectRelatedAuthorModel,
    SelectRelatedBookModel,
    SelectRelatedMtiChildModel,
    SelectRelatedMtiParentModel,
    SelectRelatedProfileModel,
    SelectRelatedPublisherModel,
    TestModel,
)

from django_async_backend.db import async_connections
from django_async_backend.db.models.query import ModelIterable
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)

DEPTH_MODELS = [
    DepthLevel1Model,
    DepthLevel2Model,
    DepthLevel3Model,
    DepthLevel4Model,
    DepthLevel5Model,
    DepthLevel6Model,
    DepthLevel7Model,
]


class SelectRelatedTestCase(AsyncioTestCase):
    """Shared fixture: two authors with profiles, two publishers, and three
    books, one of which has no publisher so the NULL related object branch of
    RelatedPopulator is reachable.
    """

    async def asyncSetUp(self):
        self.author1 = SelectRelatedAuthorModel(name="Author1")
        await self.author1.async_save()
        self.author2 = SelectRelatedAuthorModel(name="Author2")
        await self.author2.async_save()

        self.profile1 = SelectRelatedProfileModel(
            author=self.author1, bio="Bio1"
        )
        await self.profile1.async_save()

        self.publisher1 = SelectRelatedPublisherModel(name="Publisher1")
        await self.publisher1.async_save()

        self.book1 = SelectRelatedBookModel(
            title="Book1", author=self.author1, publisher=self.publisher1
        )
        await self.book1.async_save()
        self.book2 = SelectRelatedBookModel(
            title="Book2", author=self.author2, publisher=None
        )
        await self.book2.async_save()

    async def books(self, queryset):
        return [obj async for obj in queryset.order_by("title")]


class TestSelectRelatedQueryConstruction(SelectRelatedTestCase):
    """select_related() only mutates the query; nothing here touches the DB."""

    async def test_returns_clone_without_touching_source(self):
        source = SelectRelatedBookModel.async_objects.all()

        clone = source.select_related("author")

        self.assertIsNot(
            clone, source, "select_related() should return a clone"
        )
        self.assertFalse(
            source.query.select_related,
            "Source queryset should be left untouched",
        )
        self.assertEqual(clone.query.select_related, {"author": {}})

    async def test_no_arguments_follows_all_relations(self):
        qs = SelectRelatedBookModel.async_objects.all().select_related()

        self.assertIs(
            qs.query.select_related,
            True,
            "select_related() without fields should follow every relation",
        )

    async def test_none_argument_clears_previous_call(self):
        qs = SelectRelatedBookModel.async_objects.all().select_related(
            "author"
        )

        cleared = qs.select_related(None)

        self.assertIs(
            cleared.query.select_related,
            False,
            "select_related(None) should clear the relation list",
        )

    async def test_multiple_fields_are_merged(self):
        qs = SelectRelatedBookModel.async_objects.all().select_related(
            "author", "publisher"
        )

        self.assertEqual(
            qs.query.select_related, {"author": {}, "publisher": {}}
        )

    async def test_chaining_accumulates_relations(self):
        qs = (
            SelectRelatedBookModel.async_objects.all()
            .select_related("author")
            .select_related("publisher")
        )

        self.assertEqual(
            qs.query.select_related,
            {"author": {}, "publisher": {}},
            "Chained calls should accumulate rather than replace",
        )

    async def test_nested_lookup_builds_nested_dict(self):
        qs = TestModel.async_objects.all().select_related("relative__relative")

        self.assertEqual(
            qs.query.select_related, {"relative": {"relative": {}}}
        )

    async def test_matches_sync_select_related(self):
        clone = SelectRelatedBookModel.async_objects.all().select_related(
            "author", "publisher"
        )
        expected = SelectRelatedBookModel.objects.select_related(
            "author", "publisher"
        )

        self.assertEqual(
            clone.query.select_related,
            expected.query.select_related,
            "select_related() should build the same structure as Django's",
        )

    async def test_after_values_raises_type_error(self):
        with self.assertRaises(TypeError):
            SelectRelatedBookModel.async_objects.values(
                "title"
            ).select_related("author")

    async def test_after_values_list_raises_type_error(self):
        with self.assertRaises(TypeError):
            SelectRelatedBookModel.async_objects.values_list(
                "title"
            ).select_related("author")

    async def test_combined_queryset_raises_not_supported(self):
        combined = SelectRelatedBookModel.async_objects.filter(
            title="Book1"
        ).union(SelectRelatedBookModel.async_objects.filter(title="Book2"))

        with self.assertRaises(NotSupportedError):
            combined.select_related("author")


class TestSelectRelatedForwardRelations(SelectRelatedTestCase):

    async def test_forward_fk_is_fetched_in_one_query(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.select_related("author")
            )
            authors = [book.author.name for book in books]

        self.assertEqual(authors, ["Author1", "Author2"])
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "select_related() should join instead of issuing extra queries",
        )

    async def test_related_object_is_cached(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.select_related("author")
        )

        for book in books:
            self.assertTrue(
                SelectRelatedBookModel._meta.get_field("author").is_cached(
                    book
                ),
                "select_related() should populate the FK cache",
            )

    async def test_join_appears_in_sql(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            await self.books(
                SelectRelatedBookModel.async_objects.select_related("author")
            )

        sql = ctx.captured_queries[-1]["sql"]

        self.assertIn("JOIN", sql.upper())
        self.assertIn("select_related_author_model", sql)

    async def test_without_select_related_no_join_is_emitted(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            await self.books(SelectRelatedBookModel.async_objects.all())

        sql = ctx.captured_queries[-1]["sql"]

        self.assertNotIn(
            "select_related_author_model",
            sql,
            "Plain queryset should not join the related table",
        )

    async def test_multiple_relations_in_one_query(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.select_related(
                    "author", "publisher"
                )
            )
            values = [
                (
                    book.author.name,
                    book.publisher.name if book.publisher else None,
                )
                for book in books
            ]

        self.assertEqual(
            values, [("Author1", "Publisher1"), ("Author2", None)]
        )
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_null_related_object_stays_none(self):
        """A NULL foreign key hits the ``obj_data[self.pk_idx] is None`` branch
        of RelatedPopulator.populate().
        """
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.filter(
                    title="Book2"
                ).select_related("publisher")
            )

        self.assertIsNone(books[0].publisher)
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "A NULL relation should not trigger a lazy fetch",
        )

    async def test_no_argument_form_populates_relations(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.select_related()
            )
            values = [book.author.name for book in books]

        self.assertEqual(values, ["Author1", "Author2"])
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_nested_relation_is_followed(self):
        """``author__profile`` descends a second level, so the recursive call
        in get_related_selections() builds a nested RelatedPopulator.
        """
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.filter(
                    title="Book1"
                ).select_related("author__profile")
            )
            bio = books[0].author.profile.bio

        self.assertEqual(bio, "Bio1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_unrestricted_form_skips_nullable_relation(self):
        """The no-argument form only descends non-null relations, so the
        nullable self-FK is left alone and the max_depth guard in
        get_related_selections() is what ends the walk.
        """
        previous = None
        for index in range(3):
            obj = TestModel(
                name="Chain%d" % index, value=index, relative=previous
            )
            await obj.async_save()
            previous = obj

        deepest = [
            obj
            async for obj in TestModel.async_objects.filter(
                name="Chain2"
            ).select_related()
        ][0]

        self.assertFalse(
            TestModel._meta.get_field("relative").is_cached(deepest),
            "select_related() should not follow a nullable relation",
        )

    async def test_unrestricted_form_stops_at_max_depth(self):
        """A chain of non-nullable foreign keys deeper than max_depth: the
        no-argument form descends exactly max_depth levels and then takes the
        ``cur_depth > max_depth`` bail-out instead of joining the rest.
        """
        level0 = DepthLevel0Model(name="L0")
        await level0.async_save()
        parent = level0
        for index, model in enumerate(DEPTH_MODELS, start=1):
            obj = model(name="L%d" % index, parent=parent)
            await obj.async_save()
            parent = obj

        max_depth = DepthLevel7Model.async_objects.all().query.max_depth

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            deepest = [
                obj
                async for obj in DepthLevel7Model.async_objects.filter(
                    name="L7"
                ).select_related()
            ][0]

            current = deepest
            for _ in range(max_depth):
                current = current.parent

        self.assertEqual(
            current.name,
            "L%d" % (7 - max_depth),
            "Exactly max_depth levels should be joined",
        )
        self.assertFalse(
            current._meta.get_field("parent").is_cached(current),
            "The level past max_depth should not be joined",
        )
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_explicit_nesting_descends_past_max_depth(self):
        """An explicit lookup puts the query in restricted mode, where
        max_depth no longer applies, so a six-deep chain is fully joined.
        """
        previous = None
        for index in range(7):
            obj = TestModel(
                name="Chain%d" % index, value=index, relative=previous
            )
            await obj.async_save()
            previous = obj

        depth = 6
        lookup = "__".join(["relative"] * depth)

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            deepest = [
                obj
                async for obj in TestModel.async_objects.filter(
                    name="Chain6"
                ).select_related(lookup)
            ][0]

            current = deepest
            names = []
            for _ in range(depth):
                current = current.relative
                names.append(current.name)

        self.assertGreater(
            depth,
            TestModel.async_objects.all().query.max_depth,
            "The chain must be deeper than max_depth for this to mean anything",
        )
        self.assertEqual(
            names, ["Chain5", "Chain4", "Chain3", "Chain2", "Chain1", "Chain0"]
        )
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "The whole chain should arrive in a single query",
        )

    async def test_self_referential_relation(self):
        parent = TestModel(name="Parent", value=1)
        await parent.async_save()
        child = TestModel(name="Child", value=2, relative=parent)
        await child.async_save()

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            objs = [
                obj
                async for obj in TestModel.async_objects.filter(
                    name="Child"
                ).select_related("relative")
            ]
            relative_name = objs[0].relative.name

        self.assertEqual(relative_name, "Parent")
        self.assertEqual(len(ctx.captured_queries), 1)


class TestSelectRelatedReverseRelations(SelectRelatedTestCase):
    """Reverse one-to-one goes through the ``restricted`` branch of
    get_related_selections(), which uses a different setter pair.
    """

    async def test_reverse_one_to_one_is_fetched_in_one_query(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            authors = [
                obj
                async for obj in SelectRelatedAuthorModel.async_objects.filter(
                    name="Author1"
                ).select_related("profile")
            ]
            bio = authors[0].profile.bio

        self.assertEqual(bio, "Bio1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_missing_reverse_one_to_one_raises_does_not_exist(self):
        """Author2 has no profile, so the LEFT JOIN yields NULL columns and
        RelatedPopulator caches the absence rather than lazily refetching.
        """
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            authors = [
                obj
                async for obj in SelectRelatedAuthorModel.async_objects.filter(
                    name="Author2"
                ).select_related("profile")
            ]

            with self.assertRaises(SelectRelatedProfileModel.DoesNotExist):
                authors[0].profile

        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "A missing reverse relation should not trigger a lazy fetch",
        )

    async def test_remote_setter_populates_back_reference(self):
        """The reverse branch sets the related object's own FK cache, so
        walking back from the profile costs no extra query.
        """
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            authors = [
                obj
                async for obj in SelectRelatedAuthorModel.async_objects.filter(
                    name="Author1"
                ).select_related("profile")
            ]
            back_reference = authors[0].profile.author.name

        self.assertEqual(back_reference, "Author1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_forward_one_to_one_sets_reverse_cache(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            profiles = [
                obj
                async for obj in (
                    SelectRelatedProfileModel.async_objects.select_related(
                        "author"
                    )
                )
            ]
            name = profiles[0].author.name

        self.assertEqual(name, "Author1")
        self.assertEqual(len(ctx.captured_queries), 1)


class TestSelectRelatedMultiTableInheritance(SelectRelatedTestCase):

    async def test_parent_link_uses_reorder_for_init(self):
        """Selecting the MTI child from the parent side sets from_parent=True,
        so RelatedPopulator reorders the already-selected parent columns into
        the order Model.__init__ expects.
        """
        child = SelectRelatedMtiChildModel(name="Child1", child_value=7)
        await child.async_save()

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            parents = [
                obj
                async for obj in (
                    SelectRelatedMtiParentModel.async_objects.filter(
                        name="Child1"
                    ).select_related("selectrelatedmtichildmodel")
                )
            ]
            selected = parents[0].selectrelatedmtichildmodel

        self.assertEqual(selected.child_value, 7)
        self.assertEqual(selected.name, "Child1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_child_queryset_selects_parent_fields(self):
        child = SelectRelatedMtiChildModel(name="Child2", child_value=9)
        await child.async_save()

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            qs = SelectRelatedMtiChildModel.async_objects.filter(
                name="Child2"
            ).select_related()
            children = [obj async for obj in qs]

        self.assertEqual(children[0].name, "Child2")
        self.assertEqual(children[0].child_value, 9)
        self.assertEqual(len(ctx.captured_queries), 1)


class TestSelectRelatedWithAnnotationsAndKnownObjects(SelectRelatedTestCase):
    """ModelIterable applies annotations and known related objects in the same
    loop that runs the related populators, so they have to coexist.
    """

    async def test_annotation_is_applied_alongside_select_related(self):
        qs = (
            SelectRelatedBookModel.async_objects.select_related("author")
            .annotate(title_length=Length("title"))
            .order_by("title")
        )

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = [obj async for obj in qs]

        self.assertEqual([book.title_length for book in books], [5, 5])
        self.assertEqual(
            [book.author.name for book in books],
            [
                "Author1",
                "Author2",
            ],
        )
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_known_related_objects_are_applied_without_select_related(
        self,
    ):
        """_known_related_objects is normally filled by a related manager,
        which is sync-only here, so set it directly. Without select_related()
        the FK is not cached, so ModelIterable assigns the known object.
        """
        qs = SelectRelatedBookModel.async_objects.filter(
            author=self.author1
        ).order_by("title")
        author_field = SelectRelatedBookModel._meta.get_field("author")
        qs._known_related_objects = {
            author_field: {self.author1.pk: self.author1}
        }

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            books = [obj async for obj in qs]
            name = books[0].author.name

        self.assertIs(
            books[0].author,
            self.author1,
            "The known related object should be attached to the row",
        )
        self.assertEqual(name, "Author1")
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "The known related object should avoid a lazy fetch",
        )

    async def test_known_related_objects_do_not_override_select_related(self):
        """select_related() caches the FK first, so the ``is_cached`` guard
        makes ModelIterable skip the known related object entirely.
        """
        qs = (
            SelectRelatedBookModel.async_objects.filter(author=self.author1)
            .select_related("author")
            .order_by("title")
        )
        author_field = SelectRelatedBookModel._meta.get_field("author")
        decoy = SelectRelatedAuthorModel(pk=self.author1.pk, name="Decoy")
        qs._known_related_objects = {author_field: {self.author1.pk: decoy}}

        books = [obj async for obj in qs]

        self.assertIsNot(
            books[0].author,
            decoy,
            "select_related() should win over the known related object",
        )
        self.assertEqual(books[0].author.name, "Author1")

    async def test_known_related_object_miss_is_ignored(self):
        """A pk that is absent from the known map hits the KeyError branch and
        leaves the row untouched.
        """
        qs = SelectRelatedBookModel.async_objects.filter(
            author=self.author1
        ).order_by("title")
        author_field = SelectRelatedBookModel._meta.get_field("author")
        qs._known_related_objects = {author_field: {-1: self.author2}}

        books = [obj async for obj in qs]

        self.assertFalse(
            author_field.is_cached(books[0]),
            "A missing key should leave the FK uncached",
        )


class TestSelectRelatedErrors(SelectRelatedTestCase):

    async def test_invalid_field_name_raises_field_error(self):
        qs = SelectRelatedBookModel.async_objects.select_related("nonexistent")

        with self.assertRaises(FieldError):
            await self.books(qs)

    async def test_non_relational_field_raises_field_error(self):
        qs = SelectRelatedBookModel.async_objects.select_related("title")

        with self.assertRaises(FieldError):
            await self.books(qs)

    async def test_filtered_relation_is_selected(self):
        qs = SelectRelatedAuthorModel.async_objects.annotate(
            book=FilteredRelation("books", condition=Q(books__title="Book1"))
        ).select_related("book")

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            authors = [
                obj async for obj in qs.filter(name="Author1").order_by("name")
            ]

        self.assertEqual(authors[0].book.title, "Book1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_filtered_relation_with_no_match_is_none(self):
        """When the FilteredRelation matches nothing the joined columns are
        NULL, so local_setter() takes its falsy ``from_obj`` branch and caches
        no back reference.
        """
        qs = SelectRelatedAuthorModel.async_objects.annotate(
            book=FilteredRelation(
                "books", condition=Q(books__title="Nonexistent")
            )
        ).select_related("book")

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            authors = [
                obj async for obj in qs.filter(name="Author1").order_by("name")
            ]

        # The row is still returned, but RelatedPopulator builds no object, so
        # remote_setter() never runs and the annotation attribute is absent.
        self.assertEqual(authors[0].name, "Author1")
        self.assertFalse(
            hasattr(authors[0], "book"),
            "An unmatched FilteredRelation should not set the attribute",
        )
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_deep_filtered_relation_uses_noop_local_setter(self):
        """A FilteredRelation spanning more than two joins cannot cache a back
        reference, so get_related_selections() installs local_setter_noop.
        """
        qs = SelectRelatedPublisherModel.async_objects.annotate(
            author=FilteredRelation(
                "books__author",
                condition=Q(books__author__name="Author1"),
            )
        ).select_related("author")

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            publishers = [obj async for obj in qs.filter(name="Publisher1")]

        self.assertEqual(publishers[0].author.name, "Author1")
        self.assertEqual(len(ctx.captured_queries), 1)


class TestSelectRelatedInteractions(SelectRelatedTestCase):
    """select_related() combined with the other queryset entry points, each of
    which builds its own compiler and could drop the related populators.
    """

    async def test_with_aget(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            book = await SelectRelatedBookModel.async_objects.select_related(
                "author"
            ).aget(title="Book1")
            name = book.author.name

        self.assertEqual(name, "Author1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_with_afirst(self):
        book = (
            await SelectRelatedBookModel.async_objects.select_related("author")
            .order_by("title")
            .afirst()
        )

        self.assertEqual(book.author.name, "Author1")

    async def test_with_alast(self):
        book = (
            await SelectRelatedBookModel.async_objects.select_related("author")
            .order_by("title")
            .alast()
        )

        self.assertEqual(book.author.name, "Author2")

    async def test_with_filter_on_related_field(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.select_related(
                "author"
            ).filter(author__name="Author1")
        )

        self.assertEqual([book.title for book in books], ["Book1"])
        self.assertEqual(books[0].author.name, "Author1")

    async def test_with_order_by_related_field(self):
        qs = SelectRelatedBookModel.async_objects.select_related(
            "author"
        ).order_by("-author__name")
        books = [obj async for obj in qs]

        self.assertEqual(
            [book.author.name for book in books],
            ["Author2", "Author1"],
        )

    async def test_model_iterable_applies_related_populators(self):
        """ModelIterable is the only place related populators are applied, so
        drive it directly rather than through the queryset.
        """
        qs = SelectRelatedBookModel.async_objects.select_related(
            "author"
        ).order_by("title")

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            names = [book.author.name async for book in ModelIterable(qs)]

        self.assertEqual(names, ["Author1", "Author2"])
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "ModelIterable should keep the join rather than refetch relations",
        )

    @unittest.expectedFailure
    async def test_with_chunked_fetch(self):
        """Server-side cursors are broken independently of select_related():
        execute_sql(chunked_fetch=True) returns the async generator built by
        cursor_iter(), but results_iter() feeds it to itertools.chain, which
        only accepts sync iterables. Kept as an expected failure so the
        select_related() side of this path is covered once that is fixed.
        """
        qs = SelectRelatedBookModel.async_objects.select_related(
            "author"
        ).order_by("title")

        names = [
            book.author.name
            async for book in ModelIterable(
                qs, chunked_fetch=True, chunk_size=1
            )
        ]

        self.assertEqual(names, ["Author1", "Author2"])

    async def test_with_slicing(self):
        qs = SelectRelatedBookModel.async_objects.select_related(
            "author"
        ).order_by("title")[:1]
        books = [obj async for obj in qs]

        self.assertEqual(books[0].author.name, "Author1")

    async def test_with_acount_ignores_join(self):
        count = await SelectRelatedBookModel.async_objects.select_related(
            "author"
        ).acount()

        self.assertEqual(count, 2)

    async def test_no_unawaited_coroutine_across_select_related_paths(self):
        """The whole point of the generated ORM is that select_related() stays
        synchronous once the rows are in memory: RelatedPopulator.populate()
        and the setters it calls must never leave a coroutine behind. An
        unawaited coroutine only surfaces as a RuntimeWarning at collection
        time, which does not fail a test on its own, so capture warnings and
        force a collection while they are still being recorded.
        """
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            books = await self.books(
                SelectRelatedBookModel.async_objects.select_related(
                    "author", "publisher"
                )
            )
            for book in books:
                book.author.name
                if book.publisher is not None:
                    book.publisher.name

            nested = await self.books(
                SelectRelatedBookModel.async_objects.filter(
                    title="Book1"
                ).select_related("author__profile")
            )
            nested[0].author.profile.bio
            nested[0].author.profile.author.name

            reverse = [
                obj
                async for obj in SelectRelatedAuthorModel.async_objects.filter(
                    name="Author1"
                ).select_related("profile")
            ]
            reverse[0].profile.bio

            await SelectRelatedBookModel.async_objects.select_related(
                "author"
            ).aget(title="Book1")

            del books, nested, reverse
            gc.collect()

        # Report only the first line of each warning; the rest is the
        # creation traceback, which makes the diff unreadable.
        unawaited = sorted(
            {
                str(w.message).splitlines()[0]
                for w in caught
                if "never awaited" in str(w.message)
            }
        )

        self.assertEqual(
            unawaited,
            [],
            "select_related() left a coroutine unawaited: %s" % unawaited,
        )

    async def test_values_after_select_related_drops_relations(self):
        """.values() rebuilds the iterable class, so the related populators
        must not leak into the dict rows.
        """
        qs = (
            SelectRelatedBookModel.async_objects.select_related("author")
            .values("title")
            .order_by("title")
        )
        rows = [row async for row in qs]

        self.assertEqual(rows, [{"title": "Book1"}, {"title": "Book2"}])
