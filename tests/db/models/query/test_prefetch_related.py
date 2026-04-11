import pytest
from django.db.models import Prefetch

from test_app.models import Author, Book, Review


async def test_prefetch_related_none_clears(async_db):
    """prefetch_related(None) clears previous prefetches."""
    qs = Author.async_object.prefetch_related("books").prefetch_related(None)
    assert not qs._prefetch_related_lookups


async def test_prefetch_related_chaining(async_db):
    """Multiple prefetch_related calls accumulate lookups."""
    qs = Author.async_object.prefetch_related("books").prefetch_related(
        "books__reviews"
    )
    assert qs._prefetch_related_lookups == ("books", "books__reviews")


async def test_prefetch_related_with_prefetch_object_chaining(async_db):
    """Prefetch objects can be passed and accumulate."""
    prefetch = Prefetch("books")
    qs = Author.async_object.prefetch_related(prefetch)
    assert len(qs._prefetch_related_lookups) == 1
    assert isinstance(qs._prefetch_related_lookups[0], Prefetch)


@pytest.mark.xfail(
    reason="prefetch_related_objects has a bug: _result_cache contains "
    "unawaited coroutines from __getitem__, causing 'coroutine' object "
    "has no attribute '_state'",
    strict=True,
)
async def test_prefetch_related_basic(async_db):
    """Basic prefetch_related fetches related objects."""
    author = await Author.async_object.acreate(name="Author1")
    await Book.async_object.acreate(title="Book1", author=author)
    await Book.async_object.acreate(title="Book2", author=author)

    authors = [a async for a in Author.async_object.prefetch_related("books")]
    assert len(authors) == 1
    assert len(authors[0].books.all()) == 2


@pytest.mark.xfail(
    reason="prefetch_related_objects has a bug: _result_cache contains "
    "unawaited coroutines from __getitem__",
    strict=True,
)
async def test_prefetch_related_chained_execution(async_db):
    """Chained prefetch through multiple relations."""
    author = await Author.async_object.acreate(name="A")
    book = await Book.async_object.acreate(title="B", author=author)
    await Review.async_object.acreate(text="Great", book=book)

    authors = [
        a
        async for a in Author.async_object.prefetch_related("books", "books__reviews")
    ]
    assert len(authors) == 1
