from django.db.models import Count, OuterRef, Subquery
from test_app.models import Author, Book, Review


async def test_filter_with_subquery(async_db):
    """Filter using a Subquery."""
    a1 = await Author.async_object.acreate(name="WithBooks")
    a2 = await Author.async_object.acreate(name="NoBooks")
    await Book.async_object.acreate(title="B1", author=a1)

    # Authors who have at least one book
    authors_with_books = Author.async_object.filter(pk__in=Subquery(Book.async_object.values("author_id")))
    results = [a async for a in authors_with_books]
    assert len(results) == 1
    assert results[0].name == "WithBooks"


async def test_exclude_with_subquery(async_db):
    """Exclude using a Subquery."""
    a1 = await Author.async_object.acreate(name="WithBooks")
    a2 = await Author.async_object.acreate(name="NoBooks")
    await Book.async_object.acreate(title="B1", author=a1)

    authors_without_books = Author.async_object.exclude(pk__in=Subquery(Book.async_object.values("author_id")))
    results = [a async for a in authors_without_books]
    assert len(results) == 1
    assert results[0].name == "NoBooks"


async def test_annotate_with_subquery(async_db):
    """Annotate with a correlated Subquery."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    await Book.async_object.acreate(title="First", author=a1)
    await Book.async_object.acreate(title="Second", author=a1)
    await Book.async_object.acreate(title="Only", author=a2)

    first_book_subquery = Subquery(
        Book.async_object.filter(author=OuterRef("pk")).order_by("title").values("title")[:1]
    )
    authors = [a async for a in Author.async_object.annotate(first_book=first_book_subquery).order_by("name")]
    assert authors[0].first_book == "First"
    assert authors[1].first_book == "Only"


async def test_filter_exists_pattern(async_db):
    """Filter using __in with related queryset (EXISTS-like)."""
    a1 = await Author.async_object.acreate(name="Reviewed")
    a2 = await Author.async_object.acreate(name="Unreviewed")
    b1 = await Book.async_object.acreate(title="B1", author=a1)
    b2 = await Book.async_object.acreate(title="B2", author=a2)
    await Review.async_object.acreate(text="R1", book=b1)

    reviewed_authors = Author.async_object.filter(
        books__in=Subquery(
            Book.async_object.filter(pk__in=Subquery(Review.async_object.values("book_id"))).values("pk")
        )
    )
    results = [a async for a in reviewed_authors]
    assert len(results) == 1
    assert results[0].name == "Reviewed"


async def test_subquery_in_values(async_db):
    """Subquery used in values()."""
    a = await Author.async_object.acreate(name="A")
    await Book.async_object.acreate(title="T1", author=a)
    await Book.async_object.acreate(title="T2", author=a)

    book_count = Subquery(
        Book.async_object.filter(author=OuterRef("pk")).values("author").annotate(c=Count("id")).values("c")[:1]
    )
    results = [r async for r in Author.async_object.annotate(num_books=book_count).values("name", "num_books")]
    assert len(results) == 1
    assert results[0]["num_books"] == 2
