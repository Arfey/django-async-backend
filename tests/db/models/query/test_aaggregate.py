from django.db.models import Avg, Count, Max, Min, Sum

from test_app.models import Author, Book, Review, TestModel


async def test_aaggregate_count(async_db):
    await TestModel.async_object.abulk_create(
        [TestModel(name=f"T{i}", value=i) for i in range(5)]
    )
    result = await TestModel.async_object.aaggregate(total=Count("id"))
    assert result == {"total": 5}


async def test_aaggregate_sum(async_db):
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
            TestModel(name="C", value=30),
        ]
    )
    result = await TestModel.async_object.aaggregate(total=Sum("value"))
    assert result == {"total": 60}


async def test_aaggregate_avg(async_db):
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
        ]
    )
    result = await TestModel.async_object.aaggregate(avg=Avg("value"))
    assert result == {"avg": 15.0}


async def test_aaggregate_min_max(async_db):
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=5),
            TestModel(name="B", value=50),
            TestModel(name="C", value=25),
        ]
    )
    result = await TestModel.async_object.aaggregate(
        min_val=Min("value"), max_val=Max("value")
    )
    assert result == {"min_val": 5, "max_val": 50}


async def test_aaggregate_multiple(async_db):
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
            TestModel(name="C", value=30),
        ]
    )
    result = await TestModel.async_object.aaggregate(
        total=Count("id"),
        sum_val=Sum("value"),
        avg_val=Avg("value"),
    )
    assert result["total"] == 3
    assert result["sum_val"] == 60
    assert result["avg_val"] == 20.0


async def test_aaggregate_empty_queryset(async_db):
    result = await TestModel.async_object.aaggregate(
        total=Count("id"), sum_val=Sum("value")
    )
    assert result == {"total": 0, "sum_val": None}


async def test_aaggregate_with_filter(async_db):
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
            TestModel(name="C", value=30),
        ]
    )
    result = await TestModel.async_object.filter(value__gte=20).aaggregate(
        total=Count("id"), sum_val=Sum("value")
    )
    assert result == {"total": 2, "sum_val": 50}


async def test_aaggregate_across_relations(async_db):
    a1 = await Author.async_object.acreate(name="Author1")
    a2 = await Author.async_object.acreate(name="Author2")
    b1 = await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a2)
    await Review.async_object.acreate(text="R1", book=b1)
    await Review.async_object.acreate(text="R2", book=b1)

    result = await Author.async_object.aaggregate(
        book_count=Count("books", distinct=True),
        review_count=Count("books__reviews", distinct=True),
    )
    assert result["book_count"] == 3
    assert result["review_count"] == 2


async def test_aaggregate_default_alias(async_db):
    """Without explicit alias, uses field__aggfunc pattern."""
    await TestModel.async_object.acreate(name="A", value=10)
    result = await TestModel.async_object.aaggregate(Count("id"))
    assert "id__count" in result
    assert result["id__count"] == 1
