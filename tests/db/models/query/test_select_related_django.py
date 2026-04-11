"""
Ported from Django's select_related tests (tests/select_related/tests.py).

Adapted for our async backend using the Author->Book->Review chain instead of
Django's Domain->Kingdom->Phylum->Klass->Order->Family->Genus->Species chain.
Mapping:
  Author  ~ Domain/Kingdom (top level)
  Book    ~ Genus (middle, 2-FK-deep from Review)
  Review  ~ Species (leaf)
  EditorNote ~ model with nullable FK to Book

Tests requiring contenttypes/GenericForeignKey are skipped.
"""

import re

import pytest
from django.core.exceptions import FieldError
from test_app.models import Author, Book, EditorNote, Review

# ---------------------------------------------------------------------------
# SelectRelatedTests equivalents
# ---------------------------------------------------------------------------


async def test_access_fks_without_select_related(async_db):
    """Accessing FKs without select_related() does not pre-fetch related objects."""
    author = await Author.async_object.acreate(name="AuthorA")
    book = await Book.async_object.acreate(title="BookA", author=author)
    await Review.async_object.acreate(text="ReviewA", book=book)

    # We just confirm the data is accessible (no query-count assertion here since
    # we don't have assertNumQueries in async pytest style, but correctness matters).
    reviews = [r async for r in Review.async_object.all()]
    assert len(reviews) == 1
    # Accessing .book and then .author each issue a lazy query, but the result must be correct.
    review = reviews[0]
    fetched_author = await Book.async_object.aget(pk=review.book_id)
    assert fetched_author.author_id == author.pk


async def test_access_fks_with_select_related(async_db):
    """select_related() pre-fetches related objects so no extra queries are needed."""
    author = await Author.async_object.acreate(name="AuthorSR")
    book = await Book.async_object.acreate(title="BookSR", author=author)
    await Review.async_object.acreate(text="ReviewSR", book=book)

    review = await Review.async_object.select_related("book__author").aget(text="ReviewSR")
    # Both levels should be cached; no additional DB hit needed.
    assert review.book.title == "BookSR"
    assert review.book.author.name == "AuthorSR"


async def test_list_without_select_related(async_db):
    """Iterating a queryset without select_related works but requires extra queries."""
    author1 = await Author.async_object.acreate(name="L1")
    author2 = await Author.async_object.acreate(name="L2")
    book1 = await Book.async_object.acreate(title="LB1", author=author1)
    book2 = await Book.async_object.acreate(title="LB2", author=author2)
    await Review.async_object.acreate(text="LR1", book=book1)
    await Review.async_object.acreate(text="LR2", book=book2)

    reviews = [r async for r in Review.async_object.all()]
    assert len(reviews) == 2
    # Check we can still access book_id (no lazy load, it's a column)
    book_ids = {r.book_id for r in reviews}
    assert book_ids == {book1.pk, book2.pk}


async def test_list_with_select_related(async_db):
    """select_related() applies to entire lists, not just items."""
    author1 = await Author.async_object.acreate(name="SRL1")
    author2 = await Author.async_object.acreate(name="SRL2")
    book1 = await Book.async_object.acreate(title="SRLB1", author=author1)
    book2 = await Book.async_object.acreate(title="SRLB2", author=author2)
    await Review.async_object.acreate(text="SRLR1", book=book1)
    await Review.async_object.acreate(text="SRLR2", book=book2)

    reviews = [r async for r in Review.async_object.select_related("book__author")]
    assert len(reviews) == 2
    author_names = sorted(r.book.author.name for r in reviews)
    assert author_names == ["SRL1", "SRL2"]


async def test_list_with_depth(async_db):
    """
    Selecting only a partial depth ('book') stops traversal at Book.
    The author field on book is not pre-fetched, but accessing book itself works.
    """
    author = await Author.async_object.acreate(name="DepthAuthor")
    book = await Book.async_object.acreate(title="DepthBook", author=author)
    await Review.async_object.acreate(text="DepthReview", book=book)

    reviews = [r async for r in Review.async_object.select_related("book")]
    assert len(reviews) == 1
    # book is pre-fetched
    assert reviews[0].book.title == "DepthBook"
    # author_id is a column on book, still accessible without a query
    assert reviews[0].book.author_id == author.pk


async def test_certain_fields(async_db):
    """
    select_related('book__author') fetches exactly those two hops.
    Equivalent to Django's test_certain_fields (genus__family).
    """
    author1 = await Author.async_object.acreate(name="CF1")
    author2 = await Author.async_object.acreate(name="CF2")
    book1 = await Book.async_object.acreate(title="CFB1", author=author1)
    book2 = await Book.async_object.acreate(title="CFB2", author=author2)
    await Review.async_object.acreate(text="CFR1", book=book1)
    await Review.async_object.acreate(text="CFR2", book=book2)

    reviews = [r async for r in Review.async_object.select_related("book__author")]
    author_names = sorted(r.book.author.name for r in reviews)
    assert author_names == ["CF1", "CF2"]


async def test_more_certain_fields(async_db):
    """
    select_related with filter: only matching rows are returned but the
    selected relation is still pre-fetched.
    Equivalent to Django's test_more_certain_fields.
    """
    author1 = await Author.async_object.acreate(name="MCF1")
    author2 = await Author.async_object.acreate(name="MCF2")
    book1 = await Book.async_object.acreate(title="MCFB1", author=author1)
    book2 = await Book.async_object.acreate(title="MCFB2", author=author2)
    await Review.async_object.acreate(text="MCFR1", book=book1)
    await Review.async_object.acreate(text="MCFR2", book=book2)

    reviews = [r async for r in Review.async_object.filter(book__author__name="MCF1").select_related("book__author")]
    assert len(reviews) == 1
    assert reviews[0].book.author.name == "MCF1"


async def test_field_traversal(async_db):
    """
    select_related with ordering and slicing still pre-fetches related fields.
    Equivalent to Django's test_field_traversal.
    """
    author = await Author.async_object.acreate(name="FTAuthor")
    book = await Book.async_object.acreate(title="FTBook", author=author)
    await Review.async_object.acreate(text="FTReview", book=book)

    review = await Review.async_object.all().select_related("book__author").order_by("id")[0:1].aget()
    assert review.book.author.name == "FTAuthor"


async def test_none_clears_list(async_db):
    """
    select_related(None) clears a previously set select_related.
    Equivalent to Django's test_none_clears_list.
    """
    qs = Review.async_object.select_related("book").select_related(None)
    assert qs.query.select_related is False


async def test_chaining(async_db):
    """
    select_related() calls accumulate: each call adds to the set of
    pre-fetched relations.
    Equivalent to Django's test_chaining (HybridSpecies with two FKs).
    We use EditorNote which has book FK, and Book which has author FK. Two
    separate select_related calls on the same queryset.
    """
    author = await Author.async_object.acreate(name="ChainAuthor")
    book = await Book.async_object.acreate(title="ChainBook", author=author)
    await Review.async_object.acreate(text="ChainReview", book=book)

    # Chain two select_related calls; both must be honoured.
    qs = Review.async_object.select_related("book").select_related("book__author")
    reviews = [r async for r in qs]
    assert len(reviews) == 1
    assert reviews[0].book.title == "ChainBook"
    assert reviews[0].book.author.name == "ChainAuthor"


async def test_reverse_relation_not_cached(async_db):
    """
    select_related('book') caches the book on the review, but the reverse
    relation (review_set) on book is NOT cached.
    Equivalent to Django's test_reverse_relation_caching.
    """
    author = await Author.async_object.acreate(name="RRAuthor")
    book = await Book.async_object.acreate(title="RRBook", author=author)
    await Review.async_object.acreate(text="RRReview", book=book)

    review = await Review.async_object.select_related("book").filter(text="RRReview").afirst()
    assert review.book.title == "RRBook"
    # The reverse relation (reviews) should not be in the book's fields_cache
    assert "reviews" not in review.book._state.fields_cache


async def test_select_related_after_values_raises(async_db):
    """
    Calling select_related() after values() raises TypeError.
    Equivalent to Django's test_select_related_after_values.
    """
    msg = re.escape("Cannot call select_related() after .values() or .values_list()")
    with pytest.raises(TypeError, match=msg):
        list(Review.async_object.values("text").select_related("book"))


async def test_select_related_after_values_list_raises(async_db):
    """
    Calling select_related() after values_list() raises TypeError.
    Equivalent to Django's test_select_related_after_values_list.
    """
    msg = re.escape("Cannot call select_related() after .values() or .values_list()")
    with pytest.raises(TypeError, match=msg):
        list(Review.async_object.values_list("text").select_related("book"))


# ---------------------------------------------------------------------------
# SelectRelatedValidationTests equivalents (no DB needed, but async_db is
# harmless; we include it so the fixture machinery is consistent)
# ---------------------------------------------------------------------------


async def test_non_relational_field(async_db):
    """
    Passing a non-relational field to select_related() raises FieldError.
    Equivalent to Django's test_non_relational_field.
    """
    non_relational_error = "Non-relational field given in select_related: '%s'. Choices are: %s"
    # 'text' is a CharField on Review, not a relation
    with pytest.raises(FieldError, match="Non-relational field given in select_related"):
        [r async for r in Review.async_object.select_related("text__something")]

    with pytest.raises(FieldError, match="Non-relational field given in select_related"):
        [r async for r in Review.async_object.select_related("text")]

    # Author has no FK fields at all, so choices would be "(none)"
    with pytest.raises(FieldError, match="Non-relational field given in select_related"):
        [a async for a in Author.async_object.select_related("name")]


async def test_non_relational_field_nested(async_db):
    """
    Passing a non-relational nested field raises FieldError.
    Equivalent to Django's test_non_relational_field_nested.
    """
    with pytest.raises(FieldError, match="Non-relational field given in select_related"):
        [r async for r in Review.async_object.select_related("book__title")]


async def test_invalid_field(async_db):
    """
    Passing an invalid field name to select_related() raises FieldError.
    Equivalent to Django's test_invalid_field.
    """
    with pytest.raises(FieldError, match="Invalid field name"):
        [r async for r in Review.async_object.select_related("invalid_field")]

    with pytest.raises(FieldError, match="Invalid field name"):
        [r async for r in Review.async_object.select_related("book__related_invalid")]

    with pytest.raises(FieldError, match="Invalid field name"):
        [a async for a in Author.async_object.select_related("invalid_field")]


# ---------------------------------------------------------------------------
# Additional async-specific tests not in Django's suite
# ---------------------------------------------------------------------------


async def test_select_related_multiple_fields(async_db):
    """
    select_related can accept multiple field paths at once.
    """
    author = await Author.async_object.acreate(name="MFAuthor")
    book = await Book.async_object.acreate(title="MFBook", author=author)
    await Review.async_object.acreate(text="MFReview", book=book)
    await EditorNote.async_object.acreate(note="MFNote", book=book)

    # Fetch EditorNote with both book and book__author in one query
    notes = [n async for n in EditorNote.async_object.select_related("book", "book__author")]
    assert len(notes) == 1
    assert notes[0].book.title == "MFBook"
    assert notes[0].book.author.name == "MFAuthor"


async def test_select_related_nullable_fk_is_none(async_db):
    """
    select_related with a nullable FK returns None for the relation when NULL.
    """
    await EditorNote.async_object.acreate(note="NoBook", book=None)

    notes = [n async for n in EditorNote.async_object.select_related("book")]
    assert len(notes) == 1
    assert notes[0].book is None


async def test_select_related_nullable_fk_present(async_db):
    """
    select_related with a nullable FK pre-fetches the related object when set.
    """
    author = await Author.async_object.acreate(name="NullableAuthor")
    book = await Book.async_object.acreate(title="NullableBook", author=author)
    await EditorNote.async_object.acreate(note="HasBook", book=book)

    notes = [n async for n in EditorNote.async_object.select_related("book")]
    assert len(notes) == 1
    assert notes[0].book.title == "NullableBook"


async def test_select_related_order_by_related_field(async_db):
    """
    Ordering by a related field while using select_related works correctly.
    """
    a1 = await Author.async_object.acreate(name="ZZZ")
    a2 = await Author.async_object.acreate(name="AAA")
    book1 = await Book.async_object.acreate(title="ZBook", author=a1)
    book2 = await Book.async_object.acreate(title="ABook", author=a2)
    await Review.async_object.acreate(text="ZReview", book=book1)
    await Review.async_object.acreate(text="AReview", book=book2)

    reviews = [r async for r in Review.async_object.select_related("book__author").order_by("book__author__name")]
    assert len(reviews) == 2
    assert reviews[0].book.author.name == "AAA"
    assert reviews[1].book.author.name == "ZZZ"


async def test_select_related_three_levels_deep(async_db):
    """
    Full three-level chain: Review -> Book -> Author all pre-fetched in one query.
    """
    author = await Author.async_object.acreate(name="DeepA")
    book = await Book.async_object.acreate(title="DeepB", author=author)
    await Review.async_object.acreate(text="DeepR", book=book)

    reviews = [r async for r in Review.async_object.select_related("book__author")]
    assert len(reviews) == 1
    assert reviews[0].book.title == "DeepB"
    assert reviews[0].book.author.name == "DeepA"


async def test_select_related_with_filter_and_exclusion(async_db):
    """
    select_related combined with filter and exclude.
    """
    a1 = await Author.async_object.acreate(name="FilterA")
    a2 = await Author.async_object.acreate(name="ExcludeA")
    book1 = await Book.async_object.acreate(title="FB1", author=a1)
    book2 = await Book.async_object.acreate(title="FB2", author=a2)
    await Review.async_object.acreate(text="FR1", book=book1)
    await Review.async_object.acreate(text="FR2", book=book2)

    reviews = [
        r
        async for r in Review.async_object.select_related("book__author")
        .filter(book__author__name="FilterA")
        .exclude(text="FR2")
    ]
    assert len(reviews) == 1
    assert reviews[0].book.author.name == "FilterA"
