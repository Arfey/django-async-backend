from django.db.models import Count, F, Value
from django.db.models.functions import Concat, Length, Lower, Upper
from shared.models import Author, Book, Review, TestModel


async def test_annotate_count_related(async_db):
    """Annotate with Count on a reverse relation."""
    a1 = await Author.async_object.acreate(name="Prolific")
    a2 = await Author.async_object.acreate(name="Quiet")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a1)
    await Book.async_object.acreate(title="B4", author=a2)

    authors = [a async for a in Author.async_object.annotate(book_count=Count("books")).order_by("name")]
    assert authors[0].name == "Prolific"
    assert authors[0].book_count == 3
    assert authors[1].name == "Quiet"
    assert authors[1].book_count == 1


async def test_annotate_sum(async_db):
    """Annotate with Sum."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
            TestModel(name="C", value=30),
        ]
    )
    result = [r async for r in TestModel.async_object.values("value").annotate(double=F("value") * 2).order_by("value")]
    assert result[0]["double"] == 20
    assert result[1]["double"] == 40
    assert result[2]["double"] == 60


async def test_annotate_f_expression(async_db):
    """Annotate using F expressions."""
    await TestModel.async_object.acreate(name="Test", value=5)
    results = [obj async for obj in TestModel.async_object.annotate(doubled=F("value") * 2)]
    assert len(results) == 1
    assert results[0].doubled == 10


async def test_annotate_with_q_filter(async_db):
    """Annotate with Count filtered by Q object."""
    a = await Author.async_object.acreate(name="A")
    b1 = await Book.async_object.acreate(title="Python", author=a)
    b2 = await Book.async_object.acreate(title="Java", author=a)
    await Review.async_object.acreate(text="Great", book=b1)
    await Review.async_object.acreate(text="OK", book=b1)
    await Review.async_object.acreate(text="Meh", book=b2)

    books = [b async for b in Book.async_object.annotate(review_count=Count("reviews")).order_by("-review_count")]
    assert books[0].title == "Python"
    assert books[0].review_count == 2
    assert books[1].title == "Java"
    assert books[1].review_count == 1


async def test_annotate_string_functions(async_db):
    """Annotate with string functions."""
    await TestModel.async_object.acreate(name="Hello", value=1)
    results = [
        obj
        async for obj in TestModel.async_object.annotate(
            upper_name=Upper("name"),
            lower_name=Lower("name"),
            name_length=Length("name"),
        )
    ]
    assert results[0].upper_name == "HELLO"
    assert results[0].lower_name == "hello"
    assert results[0].name_length == 5


async def test_annotate_concat(async_db):
    """Annotate with Concat."""
    await TestModel.async_object.acreate(name="World", value=1)
    results = [obj async for obj in TestModel.async_object.annotate(greeting=Concat(Value("Hello "), "name"))]
    assert results[0].greeting == "Hello World"


async def test_annotate_multiple_aggregates_with_values(async_db):
    """Multiple aggregates grouped by values."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    for i in range(3):
        await Book.async_object.acreate(title=f"Book{i}", author=a1)
    await Book.async_object.acreate(title="Single", author=a2)

    results = [
        r
        async for r in Author.async_object.values("name")
        .annotate(
            book_count=Count("books"),
        )
        .order_by("name")
    ]
    assert results[0]["name"] == "A1"
    assert results[0]["book_count"] == 3
    assert results[1]["name"] == "A2"
    assert results[1]["book_count"] == 1


async def test_annotate_chained(async_db):
    """Chained annotate calls."""
    await TestModel.async_object.acreate(name="X", value=5)
    results = [
        obj async for obj in TestModel.async_object.annotate(doubled=F("value") * 2).annotate(tripled=F("value") * 3)
    ]
    assert results[0].doubled == 10
    assert results[0].tripled == 15


async def test_annotate_filter_on_annotation(async_db):
    """Filter using an annotated field."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a2)

    prolific = [a async for a in Author.async_object.annotate(book_count=Count("books")).filter(book_count__gte=2)]
    assert len(prolific) == 1
    assert prolific[0].name == "A1"
