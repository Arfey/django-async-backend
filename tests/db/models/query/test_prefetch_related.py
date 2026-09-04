import gc
import warnings
from collections import deque
from unittest import mock

from django.core.exceptions import SynchronousOnlyOperation
from django.db import (
    DEFAULT_DB_ALIAS,
    NotSupportedError,
)
from django.db.models import Prefetch as DjangoPrefetch
from django.db.models.fetch_modes import (
    FETCH_PEERS,
    FETCH_RAISE,
)
from test_app.models import (
    ChildModel,
    GenericFkModel,
    GenericRelationModel,
    M2MOwnerModel,
    M2MTagModel,
    ParentModel,
    SelectRelatedAuthorModel,
    SelectRelatedBookModel,
    SelectRelatedProfileModel,
    SelectRelatedPublisherModel,
)

from django_async_backend.db import async_connections
from django_async_backend.db.models.query import (
    Prefetch,
    aprefetch_related_objects,
)
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)


class PrefetchRelatedTestCase(AsyncioTestCase):
    """Shared fixture.

    ``Author1`` owns both books and a profile, ``Author2`` owns neither, so
    the empty side of every relation is covered as well. ``Book2`` has no
    publisher, which is the NULL forward foreign key.
    """

    async def asyncSetUp(self):
        self.author1 = await SelectRelatedAuthorModel.async_objects.acreate(
            name="Author1"
        )
        self.author2 = await SelectRelatedAuthorModel.async_objects.acreate(
            name="Author2"
        )
        self.profile1 = await SelectRelatedProfileModel.async_objects.acreate(
            author=self.author1, bio="Bio1"
        )
        self.publisher1 = (
            await SelectRelatedPublisherModel.async_objects.acreate(
                name="Publisher1"
            )
        )
        self.book1 = await SelectRelatedBookModel.async_objects.acreate(
            title="Book1", author=self.author1, publisher=self.publisher1
        )
        self.book2 = await SelectRelatedBookModel.async_objects.acreate(
            title="Book2", author=self.author1, publisher=None
        )

    def capture(self, alias=DEFAULT_DB_ALIAS):
        return AsyncCaptureQueriesContext(async_connections[alias])

    async def evaluate(self, queryset):
        return [obj async for obj in queryset]

    async def books(self, queryset):
        return await self.evaluate(queryset.order_by("title"))

    async def authors(self, queryset):
        return await self.evaluate(queryset.order_by("name"))


class TestPrefetchRelatedQueryConstruction(PrefetchRelatedTestCase):
    """prefetch_related() only records lookups; nothing here touches the DB."""

    async def test_returns_clone_without_touching_source(self):
        source = SelectRelatedBookModel.async_objects.all()

        clone = source.prefetch_related("author")

        self.assertIsNot(clone, source)
        self.assertEqual(source._prefetch_related_lookups, ())
        self.assertEqual(clone._prefetch_related_lookups, ("author",))

    async def test_chaining_accumulates_lookups(self):
        queryset = SelectRelatedBookModel.async_objects.prefetch_related(
            "author"
        ).prefetch_related("publisher")

        self.assertEqual(
            queryset._prefetch_related_lookups, ("author", "publisher")
        )

    async def test_none_clears_lookups(self):
        queryset = SelectRelatedBookModel.async_objects.prefetch_related(
            "author"
        ).prefetch_related(None)

        self.assertEqual(queryset._prefetch_related_lookups, ())

    async def test_lookups_survive_cloning(self):
        queryset = SelectRelatedBookModel.async_objects.prefetch_related(
            "author"
        ).filter(title="Book1")

        self.assertEqual(queryset._prefetch_related_lookups, ("author",))

    async def test_combined_queries_are_rejected(self):
        combined = SelectRelatedBookModel.async_objects.filter(
            title="Book1"
        ).union(SelectRelatedBookModel.async_objects.filter(title="Book2"))

        with self.assertRaises(NotSupportedError):
            combined.prefetch_related("author")

    async def test_prefetch_rejects_sync_queryset(self):
        with self.assertRaises(ValueError):
            Prefetch("books", queryset=SelectRelatedBookModel.objects.all())

    async def test_rejects_django_prefetch_with_clear_error(self):
        msg = (
            "prefetch_related() lookups must be strings or "
            "django_async_backend.db.models.query.Prefetch instances."
        )

        with self.assertRaises(TypeError) as ctx:
            SelectRelatedAuthorModel.async_objects.prefetch_related(
                DjangoPrefetch("books")
            )
        self.assertEqual(str(ctx.exception), msg)

    async def test_prefetch_equality_and_hash_use_destination(self):
        prefetch = Prefetch(
            "books",
            queryset=SelectRelatedBookModel.async_objects.all(),
        )
        same_destination = Prefetch("books")
        different_destination = Prefetch("books", to_attr="library")

        self.assertEqual(prefetch, prefetch)
        self.assertEqual(prefetch, mock.ANY)
        self.assertEqual(prefetch, same_destination)
        self.assertEqual(hash(prefetch), hash(same_destination))
        self.assertNotEqual(prefetch, different_destination)

    async def test_prefetch_rejects_non_model_iterables(self):
        msg = (
            "Prefetch querysets cannot use raw(), values(), and values_list()."
        )

        with self.assertRaises(ValueError) as ctx:
            Prefetch(
                "books",
                queryset=SelectRelatedBookModel.async_objects.values("pk"),
            )
        self.assertEqual(str(ctx.exception), msg)
        with self.assertRaises(ValueError) as ctx:
            Prefetch(
                "books",
                queryset=SelectRelatedBookModel.async_objects.values_list(
                    "pk"
                ),
            )
        self.assertEqual(str(ctx.exception), msg)


class TestPrefetchRelatedForwardRelations(PrefetchRelatedTestCase):

    async def test_forward_fk_uses_two_queries(self):
        async with self.capture() as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.prefetch_related("author")
            )
            names = [book.author.name for book in books]

        self.assertEqual(names, ["Author1", "Author1"])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_forward_fk_is_cached(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.prefetch_related("author")
        )
        field = SelectRelatedBookModel._meta.get_field("author")

        async with self.capture() as ctx:
            for book in books:
                self.assertTrue(field.is_cached(book))
                book.author.name

        self.assertEqual(len(ctx.captured_queries), 0)

    async def test_null_forward_fk_yields_none(self):
        async with self.capture() as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.prefetch_related(
                    "publisher"
                )
            )
            publishers = [book.publisher for book in books]

        self.assertEqual(publishers[0].name, "Publisher1")
        self.assertIsNone(publishers[1])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_forward_one_to_one(self):
        async with self.capture() as ctx:
            profiles = await self.evaluate(
                SelectRelatedProfileModel.async_objects.prefetch_related(
                    "author"
                )
            )
            names = [profile.author.name for profile in profiles]

        self.assertEqual(names, ["Author1"])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_forward_relation_copies_fetch_mode(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.fetch_mode(
                FETCH_PEERS
            ).prefetch_related("author")
        )

        self.assertEqual(books[0]._state.fetch_mode, FETCH_PEERS)
        self.assertEqual(books[0].author._state.fetch_mode, FETCH_PEERS)

    async def test_forward_relation_is_available_in_fetch_raise_mode(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.fetch_mode(
                FETCH_RAISE
            ).prefetch_related("author")
        )

        self.assertEqual(books[0].author.name, "Author1")

    async def test_forward_custom_queryset_can_filter_out_relation(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.filter(
                title="Book1"
            ).prefetch_related(
                Prefetch(
                    "author",
                    queryset=SelectRelatedAuthorModel.async_objects.none(),
                    to_attr="prefetched_author",
                )
            )
        )

        self.assertIsNone(books[0].prefetched_author)

    async def test_forward_prefetch_clears_custom_ordering(self):
        authors = SelectRelatedAuthorModel.async_objects.order_by("-name")

        async with self.capture() as ctx:
            await self.books(
                SelectRelatedBookModel.async_objects.prefetch_related(
                    Prefetch("author", queryset=authors)
                )
            )

        self.assertNotIn("ORDER BY", ctx.captured_queries[1]["sql"])


class TestPrefetchRelatedReverseRelations(PrefetchRelatedTestCase):

    async def test_reverse_fk_uses_two_queries(self):
        async with self.capture() as ctx:
            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    "books"
                )
            )
            titles = [
                sorted(book.title for book in author.books.all())
                for author in authors
            ]

        self.assertEqual(titles, [["Book1", "Book2"], []])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_reverse_fk_caches_the_back_reference(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.prefetch_related("books")
        )
        field = SelectRelatedBookModel._meta.get_field("author")

        async with self.capture() as ctx:
            for book in authors[0].books.all():
                self.assertTrue(field.is_cached(book))
                self.assertEqual(book.author.name, "Author1")

        self.assertEqual(len(ctx.captured_queries), 0)

    async def test_reverse_one_to_one(self):
        async with self.capture() as ctx:
            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    "profile"
                )
            )
            bio = authors[0].profile.bio

        self.assertEqual(bio, "Bio1")
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_missing_reverse_one_to_one_raises_without_a_query(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.prefetch_related("profile")
        )

        async with self.capture() as ctx:
            with self.assertRaises(
                SelectRelatedAuthorModel.profile.RelatedObjectDoesNotExist
            ):
                authors[1].profile

        self.assertEqual(len(ctx.captured_queries), 0)

    async def test_reverse_many_relation_preserves_custom_ordering(self):
        books = SelectRelatedBookModel.async_objects.order_by("-title")

        async with self.capture() as ctx:
            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    Prefetch("books", queryset=books)
                )
            )

        related_books = await self.evaluate(authors[0].books.all())
        self.assertEqual(
            [book.title for book in related_books], ["Book2", "Book1"]
        )
        self.assertIn("ORDER BY", ctx.captured_queries[1]["sql"])

    async def test_reverse_one_to_one_custom_queryset_to_attr(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.prefetch_related(
                Prefetch(
                    "profile",
                    queryset=SelectRelatedProfileModel.async_objects.none(),
                    to_attr="prefetched_profile",
                )
            )
        )

        self.assertIsNone(authors[0].prefetched_profile)
        self.assertIsNone(authors[1].prefetched_profile)


class TestPrefetchRelatedManyToMany(PrefetchRelatedTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        through = M2MOwnerModel.tags.through
        self.owner1 = await M2MOwnerModel.async_objects.acreate(name="Owner1")
        self.owner2 = await M2MOwnerModel.async_objects.acreate(name="Owner2")
        self.tag1 = await M2MTagModel.async_objects.acreate(name="Tag1")
        self.tag2 = await M2MTagModel.async_objects.acreate(name="Tag2")
        await through.async_objects.acreate(
            m2mownermodel=self.owner1, m2mtagmodel=self.tag1
        )
        await through.async_objects.acreate(
            m2mownermodel=self.owner1, m2mtagmodel=self.tag2
        )

    async def test_forward_many_to_many(self):
        async with self.capture() as ctx:
            owners = await self.evaluate(
                M2MOwnerModel.async_objects.prefetch_related("tags").order_by(
                    "name"
                )
            )
            tags = [
                sorted(tag.name for tag in owner.tags.all())
                for owner in owners
            ]

        self.assertEqual(tags, [["Tag1", "Tag2"], []])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_reverse_many_to_many(self):
        async with self.capture() as ctx:
            tags = await self.evaluate(
                M2MTagModel.async_objects.prefetch_related("owners").order_by(
                    "name"
                )
            )
            owners = [
                sorted(owner.name for owner in tag.owners.all())
                for tag in tags
            ]

        self.assertEqual(owners, [["Owner1"], ["Owner1"]])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_forward_many_to_many_to_attr_conflicts_with_field(self):
        queryset = M2MOwnerModel.async_objects.prefetch_related(
            Prefetch(
                "tags",
                queryset=M2MTagModel.async_objects.all(),
                to_attr="tags",
            )
        )

        with self.assertRaises(ValueError) as ctx:
            await self.evaluate(queryset)
        self.assertEqual(
            str(ctx.exception),
            "to_attr=tags conflicts with a field on the M2MOwnerModel model.",
        )

    async def test_reverse_many_to_many_to_attr_conflicts_with_field(self):
        queryset = M2MTagModel.async_objects.prefetch_related(
            Prefetch(
                "owners",
                queryset=M2MOwnerModel.async_objects.all(),
                to_attr="owners",
            )
        )

        with self.assertRaises(ValueError) as ctx:
            await self.evaluate(queryset)
        self.assertEqual(
            str(ctx.exception),
            "to_attr=owners conflicts with a field on the M2MTagModel model.",
        )

    async def test_forward_many_to_many_sliced_queryset(self):
        owners = await self.evaluate(
            M2MOwnerModel.async_objects.order_by("name").prefetch_related(
                Prefetch(
                    "tags",
                    queryset=M2MTagModel.async_objects.order_by("name")[:1],
                    to_attr="first_tag",
                )
            )
        )

        self.assertEqual([tag.name for tag in owners[0].first_tag], ["Tag1"])
        self.assertEqual(owners[1].first_tag, [])

    async def test_reverse_many_to_many_sliced_queryset(self):
        tags = await self.evaluate(
            M2MTagModel.async_objects.order_by("name").prefetch_related(
                Prefetch(
                    "owners",
                    queryset=M2MOwnerModel.async_objects.order_by("name")[:1],
                    to_attr="first_owner",
                )
            )
        )

        self.assertEqual(
            [[owner.name for owner in tag.first_owner] for tag in tags],
            [["Owner1"], ["Owner1"]],
        )


class TestPrefetchRelatedNestedLookups(PrefetchRelatedTestCase):

    async def test_nested_lookup_uses_three_queries(self):
        async with self.capture() as ctx:
            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    "books__publisher"
                )
            )
            publishers = sorted(
                "" if book.publisher is None else book.publisher.name
                for book in authors[0].books.all()
            )

        self.assertEqual(publishers, ["", "Publisher1"])
        self.assertEqual(len(ctx.captured_queries), 3)

    async def test_overlapping_lookups_are_deduplicated(self):
        async with self.capture() as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.prefetch_related(
                    "author", "author__profile"
                )
            )
            bios = [book.author.profile.bio for book in books]

        self.assertEqual(bios, ["Bio1", "Bio1"])
        self.assertEqual(len(ctx.captured_queries), 3)

    async def test_select_related_relation_is_not_prefetched_again(self):
        async with self.capture() as ctx:
            books = await self.books(
                SelectRelatedBookModel.async_objects.select_related(
                    "author"
                ).prefetch_related("author__profile")
            )
            bios = [book.author.profile.bio for book in books]

        self.assertEqual(bios, ["Bio1", "Bio1"])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_to_attr_can_be_traversed_by_later_lookup(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.prefetch_related(
                Prefetch("books", to_attr="library"),
                "library__publisher",
            )
        )

        self.assertEqual(
            [
                book.publisher.name if book.publisher else None
                for book in authors[0].library
            ],
            ["Publisher1", None],
        )

    async def test_nested_prefetch_on_custom_queryset_is_not_overwritten(self):
        books = SelectRelatedBookModel.async_objects.prefetch_related(
            "publisher"
        )
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.prefetch_related(
                Prefetch("books", queryset=books)
            )
        )

        async with self.capture() as ctx:
            related_books = await self.evaluate(authors[0].books.all())
            publishers = [book.publisher for book in related_books]

        self.assertEqual(publishers, [self.publisher1, None])
        self.assertEqual(len(ctx.captured_queries), 0)

    async def test_nullable_relation_can_be_traversed(self):
        books = await self.books(
            SelectRelatedBookModel.async_objects.prefetch_related(
                "publisher__books"
            )
        )

        self.assertEqual(
            [book.title for book in books[0].publisher.books.all()],
            ["Book1"],
        )
        self.assertIsNone(books[1].publisher)


class TestPrefetchObject(PrefetchRelatedTestCase):

    async def test_prefetch_with_custom_queryset(self):
        async with self.capture() as ctx:
            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    Prefetch(
                        "books",
                        queryset=SelectRelatedBookModel.async_objects.filter(
                            title="Book1"
                        ),
                    )
                )
            )
            books = authors[0].books.all()
            titles = list(books)

        self.assertEqual([book.title for book in titles], ["Book1"])
        self.assertEqual(len(books), 1)
        self.assertTrue(books)
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_prefetch_to_attr(self):
        async with self.capture() as ctx:
            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    Prefetch(
                        "books",
                        queryset=SelectRelatedBookModel.async_objects.filter(
                            title="Book2"
                        ),
                        to_attr="cheap_books",
                    )
                )
            )

        self.assertIsInstance(authors[0].cheap_books, list)
        self.assertEqual(
            [book.title for book in authors[0].cheap_books], ["Book2"]
        )
        self.assertEqual(authors[1].cheap_books, [])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_same_lookup_with_two_querysets_is_rejected(self):
        queryset = SelectRelatedAuthorModel.async_objects.prefetch_related(
            "books",
            Prefetch(
                "books",
                queryset=SelectRelatedBookModel.async_objects.all(),
            ),
        )

        with self.assertRaises(ValueError):
            await self.authors(queryset)

    async def test_custom_queryset_can_be_sliced(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.prefetch_related(
                Prefetch(
                    "books",
                    queryset=SelectRelatedBookModel.async_objects.order_by(
                        "title"
                    )[1:],
                    to_attr="later_books",
                )
            )
        )

        self.assertEqual(
            [book.title for book in authors[0].later_books], ["Book2"]
        )
        self.assertEqual(authors[1].later_books, [])


class TestPrefetchRelatedEvaluationPaths(PrefetchRelatedTestCase):

    async def test_afirst_applies_the_prefetch(self):
        async with self.capture() as ctx:
            book = (
                await SelectRelatedBookModel.async_objects.order_by("title")
                .prefetch_related("author")
                .afirst()
            )
            name = book.author.name

        self.assertEqual(name, "Author1")
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_aget_applies_the_prefetch(self):
        async with self.capture() as ctx:
            author = (
                await SelectRelatedAuthorModel.async_objects.filter(
                    name="Author1"
                )
                .prefetch_related("books")
                .aget()
            )
            titles = sorted(book.title for book in author.books.all())

        self.assertEqual(titles, ["Book1", "Book2"])
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_slicing_applies_the_prefetch(self):
        async with self.capture() as ctx:
            books = await self.evaluate(
                SelectRelatedBookModel.async_objects.order_by(
                    "title"
                ).prefetch_related("author")[:1]
            )
            name = books[0].author.name

        self.assertEqual(name, "Author1")
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_acount_does_not_trigger_the_prefetch(self):
        async with self.capture() as ctx:
            count = (
                await SelectRelatedBookModel.async_objects.prefetch_related(
                    "author"
                ).acount()
            )

        self.assertEqual(count, 2)
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_prefetch_runs_only_once_per_queryset(self):
        queryset = SelectRelatedBookModel.async_objects.prefetch_related(
            "author"
        )
        await self.evaluate(queryset)

        async with self.capture() as ctx:
            await self.evaluate(queryset)

        self.assertEqual(len(ctx.captured_queries), 0)


class TestAprefetchRelatedObjects(PrefetchRelatedTestCase):

    async def test_prefetches_already_loaded_instances(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )

        async with self.capture() as ctx:
            await aprefetch_related_objects(authors, "books")
            titles = sorted(book.title for book in authors[0].books.all())

        self.assertEqual(titles, ["Book1", "Book2"])
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_second_call_is_a_no_op(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )
        await aprefetch_related_objects(authors, "books")

        async with self.capture() as ctx:
            await aprefetch_related_objects(authors, "books")

        self.assertEqual(len(ctx.captured_queries), 0)

    async def test_empty_instance_list_is_a_no_op(self):
        async with self.capture() as ctx:
            await aprefetch_related_objects([], "books")

        self.assertEqual(len(ctx.captured_queries), 0)

    async def test_accepts_various_iterables(self):
        author = await SelectRelatedAuthorModel.async_objects.filter(
            name="Author1"
        ).aget()

        class AuthorIterable:
            def __iter__(self):
                yield author

        cases = (
            (author,),
            deque([author]),
            {author},
            frozenset([author]),
            {"author": author}.values(),
            AuthorIterable(),
        )
        for instances in cases:
            with self.subTest(iterable=type(instances).__name__):
                author._prefetched_objects_cache = {}
                async with self.capture() as ctx:
                    await aprefetch_related_objects(instances, "books")
                self.assertEqual(len(ctx.captured_queries), 1)
                self.assertEqual(
                    [book.title for book in author.books.all()],
                    ["Book1", "Book2"],
                )

    async def test_second_call_fetches_new_instances(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )
        await aprefetch_related_objects(authors[:1], "books")

        async with self.capture() as ctx:
            await aprefetch_related_objects(authors, "books")

        self.assertEqual(len(ctx.captured_queries), 1)
        self.assertEqual(list(authors[1].books.all()), [])

    async def test_custom_filtered_queryset(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )

        await aprefetch_related_objects(
            authors,
            Prefetch(
                "books",
                queryset=SelectRelatedBookModel.async_objects.filter(
                    title="Book1"
                ),
            ),
        )

        books = list(authors[0].books.all())
        self.assertEqual([book.title for book in books], ["Book1"])

    async def test_nested_lookup_after_custom_queryset_prefetch(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )
        await aprefetch_related_objects(
            authors,
            Prefetch(
                "books",
                queryset=SelectRelatedBookModel.async_objects.all(),
            ),
        )

        async with self.capture() as ctx:
            await aprefetch_related_objects(authors, "books__publisher")

        books = list(authors[0].books.all())
        self.assertEqual([book.title for book in books], ["Book1", "Book2"])
        self.assertEqual(books[0].publisher, self.publisher1)
        self.assertIsNone(books[1].publisher)
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_rejects_django_prefetch_with_clear_error(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )
        msg = (
            "Prefetch lookups must be strings. Use "
            "django_async_backend.db.models.query.Prefetch instead of "
            "django.db.models.Prefetch."
        )

        with self.assertRaises(TypeError) as ctx:
            await aprefetch_related_objects(authors, DjangoPrefetch("books"))
        self.assertEqual(str(ctx.exception), msg)

    async def test_forward_foreign_key(self):
        books = await self.books(SelectRelatedBookModel.async_objects.all())

        async with self.capture() as ctx:
            await aprefetch_related_objects(books, "author")
            names = [book.author.name for book in books]

        self.assertEqual(names, ["Author1", "Author1"])
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_reverse_one_to_one(self):
        authors = await self.authors(
            SelectRelatedAuthorModel.async_objects.all()
        )

        async with self.capture() as ctx:
            await aprefetch_related_objects(authors, "profile")
            bio = authors[0].profile.bio

        self.assertEqual(bio, "Bio1")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_many_to_many(self):
        through = M2MOwnerModel.tags.through
        owner = await M2MOwnerModel.async_objects.acreate(
            name="StandaloneOwner"
        )
        tag = await M2MTagModel.async_objects.acreate(name="StandaloneTag")
        await through.async_objects.acreate(
            m2mownermodel=owner, m2mtagmodel=tag
        )
        owners = await self.evaluate(
            M2MOwnerModel.async_objects.filter(pk=owner.pk)
        )

        async with self.capture() as ctx:
            await aprefetch_related_objects(owners, "tags")
            names = [related.name for related in owners[0].tags.all()]

        self.assertEqual(names, ["StandaloneTag"])
        self.assertEqual(len(ctx.captured_queries), 1)


class TestPrefetchRelatedMultiDatabase(PrefetchRelatedTestCase):

    async def test_using_is_honored_for_primary_and_related_queries(self):
        author = await SelectRelatedAuthorModel.async_objects.using(
            "other"
        ).acreate(name="OtherAuthor")
        await SelectRelatedBookModel.async_objects.using("other").acreate(
            title="OtherBook", author=author
        )

        async with self.capture("other") as other_ctx:
            authors = await self.evaluate(
                SelectRelatedAuthorModel.async_objects.using("other")
                .filter(name="OtherAuthor")
                .prefetch_related("books")
            )

        self.assertEqual(
            [book.title for book in authors[0].books.all()], ["OtherBook"]
        )
        self.assertEqual(len(other_ctx.captured_queries), 2)

    async def test_custom_queryset_can_select_a_different_database(self):
        author = await SelectRelatedAuthorModel.async_objects.using(
            "other"
        ).acreate(name="OtherAuthor")
        await SelectRelatedBookModel.async_objects.using("other").acreate(
            title="OtherBook", author=author
        )
        other_authors = await self.evaluate(
            SelectRelatedAuthorModel.async_objects.using("other").filter(
                name="OtherAuthor"
            )
        )

        async with self.capture() as default_ctx:
            await aprefetch_related_objects(
                other_authors,
                Prefetch(
                    "books",
                    queryset=SelectRelatedBookModel.async_objects.using(
                        "default"
                    ),
                ),
            )

        self.assertEqual(await self.evaluate(other_authors[0].books.all()), [])
        self.assertEqual(len(default_ctx.captured_queries), 1)


class TestPrefetchRelatedMultiTableInheritance(PrefetchRelatedTestCase):

    async def test_parent_link_can_be_prefetched(self):
        child = await ChildModel.async_objects.acreate(
            parent_value=1, child_value=2
        )

        async with self.capture() as ctx:
            children = await self.evaluate(
                ChildModel.async_objects.filter(pk=child.pk).prefetch_related(
                    "parentmodel_ptr"
                )
            )
            parent = children[0].parentmodel_ptr

        self.assertEqual(parent.parent_value, 1)
        self.assertEqual(len(ctx.captured_queries), 2)

    async def test_child_link_can_be_prefetched(self):
        child = await ChildModel.async_objects.acreate(
            parent_value=1, child_value=2
        )

        async with self.capture() as ctx:
            parents = await self.evaluate(
                ParentModel.async_objects.filter(pk=child.pk).prefetch_related(
                    "childmodel"
                )
            )
            fetched_child = parents[0].childmodel

        self.assertEqual(fetched_child.child_value, 2)
        self.assertEqual(len(ctx.captured_queries), 2)


class TestPrefetchRelatedUnsupported(PrefetchRelatedTestCase):

    async def test_generic_foreign_key_is_not_supported(self):
        await GenericFkModel.async_objects.acreate(name="Generic")
        queryset = GenericFkModel.async_objects.prefetch_related(
            "content_object"
        )

        with self.assertRaises(NotSupportedError) as ctx:
            await self.evaluate(queryset)

        self.assertIn("content_object", str(ctx.exception))

    async def test_generic_relation_is_not_supported(self):
        """A GenericRelation never even reaches the prefetch dispatch: simply
        building its related manager looks the ContentType up synchronously.
        """
        await GenericRelationModel.async_objects.acreate(name="Generic")
        queryset = GenericRelationModel.async_objects.prefetch_related(
            "children"
        )

        with self.assertRaises(SynchronousOnlyOperation):
            await self.evaluate(queryset)

    async def test_unknown_lookup_is_rejected(self):
        queryset = SelectRelatedBookModel.async_objects.prefetch_related(
            "does_not_exist"
        )

        with self.assertRaises(AttributeError) as ctx:
            await self.books(queryset)

        self.assertIn("does_not_exist", str(ctx.exception))

    async def test_non_relational_lookup_is_rejected(self):
        queryset = SelectRelatedBookModel.async_objects.prefetch_related(
            "title"
        )

        with self.assertRaises(ValueError):
            await self.books(queryset)


class TestPrefetchRelatedLeavesNoCoroutineUnawaited(PrefetchRelatedTestCase):

    async def test_no_unawaited_coroutine_warning(self):
        """An unawaited coroutine only surfaces as a RuntimeWarning at
        collection time, which does not fail a test on its own, so capture
        warnings and force a collection while they are still recorded.
        """
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            books = await self.books(
                SelectRelatedBookModel.async_objects.prefetch_related(
                    "author", "publisher"
                )
            )
            for book in books:
                book.author.name

            authors = await self.authors(
                SelectRelatedAuthorModel.async_objects.prefetch_related(
                    "books__publisher", "profile"
                )
            )
            for author in authors:
                list(author.books.all())

            owners = await self.evaluate(
                M2MOwnerModel.async_objects.prefetch_related("tags")
            )
            for owner in owners:
                list(owner.tags.all())

            await aprefetch_related_objects(authors, "books")

            del books, authors, owners
            gc.collect()

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
            "prefetch_related() left a coroutine unawaited: %s" % unawaited,
        )
