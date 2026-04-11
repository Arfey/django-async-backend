from django.db.models import Prefetch
from test_app.models import Author, Book


async def test_prefetch_related_none_clears(async_db):
    """prefetch_related(None) clears previous prefetches."""
    qs = Author.async_object.prefetch_related("books").prefetch_related(None)
    assert not qs._prefetch_related_lookups


async def test_prefetch_related_chaining(async_db):
    """Multiple prefetch_related calls accumulate lookups."""
    qs = Author.async_object.prefetch_related("books").prefetch_related("books__reviews")
    assert qs._prefetch_related_lookups == ("books", "books__reviews")


async def test_prefetch_related_with_prefetch_object_chaining(async_db):
    """Prefetch objects can be passed and accumulate."""
    prefetch = Prefetch("books")
    qs = Author.async_object.prefetch_related(prefetch)
    assert len(qs._prefetch_related_lookups) == 1
    assert isinstance(qs._prefetch_related_lookups[0], Prefetch)


async def test_prefetch_related_basic(async_db):
    """Basic prefetch_related fetches related objects."""
    author = await Author.async_object.acreate(name="Author1")
    await Book.async_object.acreate(title="Book1", author=author)
    await Book.async_object.acreate(title="Book2", author=author)

    authors = [a async for a in Author.async_object.prefetch_related("books")]
    assert len(authors) == 1
    # The prefetch cache should be populated
    cached_books = authors[0]._prefetched_objects_cache["books"]
    assert len(cached_books._result_cache) == 2


async def test_prefetch_related_empty(async_db):
    """Prefetch on objects with no related items returns empty cache."""
    await Author.async_object.acreate(name="Lonely")
    authors = [a async for a in Author.async_object.prefetch_related("books")]
    assert len(authors) == 1
    cached_books = authors[0]._prefetched_objects_cache["books"]
    assert len(cached_books._result_cache) == 0


async def test_prefetch_related_multiple_parents(async_db):
    """Prefetch works correctly across multiple parent objects."""
    a1 = await Author.async_object.acreate(name="Author1")
    a2 = await Author.async_object.acreate(name="Author2")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a2)

    authors = [a async for a in Author.async_object.prefetch_related("books").order_by("name")]
    assert len(authors) == 2
    # Collect book counts per author from prefetch cache
    book_counts = {a.name: len(a._prefetched_objects_cache["books"]._result_cache) for a in authors}
    assert book_counts == {"Author1": 2, "Author2": 1}
