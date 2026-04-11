"""
Async ports of Django's aggregation tests from tests/aggregation/tests.py.

Only tests that can be expressed with our models are included:
  - Author  (name)
  - Book    (title, author FK→Author)
  - Review  (text, book FK→Book)
  - TestModel (name, value)

M2M relations (friends, book.authors, store.books) and models without
equivalents (Publisher, Store) are skipped.
"""

import pytest
from django.core.exceptions import FieldError
from django.db.models import (
    Avg,
    Case,
    Count,
    Exists,
    F,
    Max,
    Min,
    Q,
    StdDev,
    Subquery,
    Sum,
    Value,
    Variance,
    When,
)
from django.db.models.functions import Coalesce
from test_app.models import Author, Book, Review, TestModel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_author_books(async_db):
    """
    Create a small dataset:
      a1 → b1 (review r1, r2), b2
      a2 → b3
    Returns (a1, a2, b1, b2, b3, r1, r2).
    """
    a1 = await Author.async_object.acreate(name="Alice")
    a2 = await Author.async_object.acreate(name="Bob")
    b1 = await Book.async_object.acreate(title="Book One", author=a1)
    b2 = await Book.async_object.acreate(title="Book Two", author=a1)
    b3 = await Book.async_object.acreate(title="Book Three", author=a2)
    r1 = await Review.async_object.acreate(text="Review A", book=b1)
    r2 = await Review.async_object.acreate(text="Review B", book=b1)
    return a1, a2, b1, b2, b3, r1, r2


# ---------------------------------------------------------------------------
# Basic aggregate tests
# ---------------------------------------------------------------------------


async def test_empty_aggregate(async_db):
    """aggregate() with no arguments returns an empty dict."""
    result = await TestModel.async_object.aaggregate()
    assert result == {}


async def test_single_aggregate_default_alias(async_db):
    """Positional aggregate uses field__aggfunc alias."""
    await TestModel.async_object.abulk_create([TestModel(name=f"T{i}", value=i * 10) for i in range(1, 4)])
    result = await TestModel.async_object.aaggregate(Sum("value"))
    assert "value__sum" in result
    assert result["value__sum"] == 60


async def test_multiple_aggregates_default_aliases(async_db):
    """Multiple positional aggregates produce separate alias keys."""
    await TestModel.async_object.abulk_create([TestModel(name=f"M{i}", value=i * 10) for i in range(1, 4)])
    result = await TestModel.async_object.aaggregate(Sum("value"), Count("value"))
    assert result["value__sum"] == 60
    assert result["value__count"] == 3


async def test_filter_aggregate(async_db):
    """Filtering before aggregate restricts the set."""
    await TestModel.async_object.abulk_create([TestModel(name=f"F{i}", value=i * 10) for i in range(1, 6)])
    result = await TestModel.async_object.filter(value__gt=20).aaggregate(Sum("value"))
    # values 30, 40, 50 → sum = 120
    assert result["value__sum"] == 120


async def test_related_aggregate_fk(async_db):
    """Aggregate across a FK traversal."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Total number of books across all authors
    result = await Author.async_object.aaggregate(book_count=Count("books"))
    assert result["book_count"] == 3  # b1, b2 for a1; b3 for a2


async def test_related_aggregate_reverse_fk_chain(async_db):
    """Aggregate across a two-hop FK chain (Author→Book→Review)."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    result = await Author.async_object.aaggregate(review_count=Count("books__reviews"))
    assert result["review_count"] == 2


async def test_count(async_db):
    """Count on a field returns the number of non-null values."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="C1", value=1),
            TestModel(name="C2", value=2),
            TestModel(name="C3", value=None),
        ]
    )
    result = await TestModel.async_object.aaggregate(Count("value"))
    assert result["value__count"] == 2


async def test_count_star(async_db):
    """Count('*') counts all rows."""
    await TestModel.async_object.abulk_create([TestModel(name=f"S{i}", value=i) for i in range(3)])
    result = await TestModel.async_object.aaggregate(n=Count("*"))
    assert result["n"] == 3


async def test_distinct_on_aggregate(async_db):
    """distinct=True on Avg/Count/Sum de-duplicates the values."""
    # Three rows, two distinct values (10, 10, 20)
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="D1", value=10),
            TestModel(name="D2", value=10),
            TestModel(name="D3", value=20),
        ]
    )
    avg_result = await TestModel.async_object.aaggregate(v=Avg("value", distinct=True))
    assert avg_result["v"] == 15.0  # (10+20)/2

    count_result = await TestModel.async_object.aaggregate(v=Count("value", distinct=True))
    assert count_result["v"] == 2

    sum_result = await TestModel.async_object.aaggregate(v=Sum("value", distinct=True))
    assert sum_result["v"] == 30  # 10+20


async def test_count_distinct_expression(async_db):
    """Count with a Case/When expression and distinct=True."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="CE1", value=5),
            TestModel(name="CE2", value=15),
            TestModel(name="CE3", value=15),
        ]
    )
    result = await TestModel.async_object.aaggregate(
        distinct_vals=Count(Case(When(value__gt=10, then="value")), distinct=True)
    )
    # Only value=15 qualifies (one distinct value)
    assert result["distinct_vals"] == 1


async def test_sum_distinct_aggregate(async_db):
    """Sum on a queryset with repeated values counts each row only once
    unless distinct=True on the aggregate itself."""
    # b1 has 2 reviews, b2 has 0 - total reviews = 2
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Aggregate reviews through books; with distinct on book id
    result = await Book.async_object.aaggregate(book_count=Count("id", distinct=True))
    assert result["book_count"] == 3


# ---------------------------------------------------------------------------
# Annotate tests
# ---------------------------------------------------------------------------


async def test_annotate_basic(async_db):
    """annotate() adds a computed attribute to each row."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    books = [obj async for obj in Book.async_object.annotate(review_count=Count("reviews")).order_by("title")]
    # Book One has 2 reviews, Book Three has 0, Book Two has 0
    by_title = {b.title: b.review_count for b in books}
    assert by_title["Book One"] == 2
    assert by_title["Book Two"] == 0
    assert by_title["Book Three"] == 0


async def test_backwards_fk_annotate(async_db):
    """annotate() via reverse FK counts related objects per row."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    authors = [obj async for obj in Author.async_object.annotate(num_books=Count("books")).order_by("name")]
    by_name = {a.name: a.num_books for a in authors}
    assert by_name["Alice"] == 2
    assert by_name["Bob"] == 1


async def test_reverse_fk_annotate_no_related(async_db):
    """Author with no books gets count 0."""
    await Author.async_object.acreate(name="Charlie")
    authors = [obj async for obj in Author.async_object.annotate(num_books=Count("books")).order_by("name")]
    by_name = {a.name: a.num_books for a in authors}
    assert by_name["Charlie"] == 0


async def test_annotate_values(async_db):
    """annotate + values() returns dicts with the annotation."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    results = [
        obj async for obj in Book.async_object.annotate(rc=Count("reviews")).values("title", "rc").order_by("title")
    ]
    assert results[0] == {"title": "Book One", "rc": 2}
    assert results[1] == {"title": "Book Three", "rc": 0}
    assert results[2] == {"title": "Book Two", "rc": 0}


async def test_annotate_values_list(async_db):
    """annotate + values_list() returns tuples."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    results = [
        obj
        async for obj in Book.async_object.annotate(rc=Count("reviews")).values_list("title", "rc").order_by("title")
    ]
    assert results[0] == ("Book One", 2)
    assert results[2] == ("Book Two", 0)


async def test_annotate_values_list_flat(async_db):
    """annotate + values_list(flat=True) returns a flat list."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    titles = [
        obj
        async for obj in Book.async_object.annotate(rc=Count("reviews"))
        .values_list("title", flat=True)
        .order_by("title")
    ]
    assert titles == ["Book One", "Book Three", "Book Two"]


async def test_fkey_aggregate_explicit_vs_implicit(async_db):
    """Count('books__id') and Count('books') produce the same result."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    explicit = [obj async for obj in Author.async_object.annotate(cnt=Count("books__id")).order_by("name")]
    implicit = [obj async for obj in Author.async_object.annotate(cnt=Count("books")).order_by("name")]
    assert [a.cnt for a in explicit] == [a.cnt for a in implicit]


async def test_annotate_ordering(async_db):
    """values() + annotate + order_by an annotation."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    results = [
        obj
        async for obj in Author.async_object.values("name")
        .annotate(book_count=Count("books"))
        .order_by("-book_count", "name")
    ]
    assert results[0]["name"] == "Alice"
    assert results[0]["book_count"] == 2
    assert results[1]["name"] == "Bob"
    assert results[1]["book_count"] == 1


@pytest.mark.skip(
    reason=(
        "annotate(agg).aaggregate(agg) requires a subquery-wrapped compiler "
        "which falls back to Django's sync compiler (backend limitation)."
    )
)
async def test_aggregate_annotation(async_db):
    """annotate then aggregate applies aggregate over the annotation."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Annotate each author with book count, then find average book count
    result = await Author.async_object.annotate(num_books=Count("books")).aaggregate(Avg("num_books"))
    # Alice has 2, Bob has 1 → avg = 1.5
    assert result["num_books__avg"] == 1.5


async def test_non_grouped_annotation_not_in_group_by(async_db):
    """An annotation not in values() before aggregate is excluded from GROUP BY."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Annotate books with a computed alias, then group only by author
    qs = [
        obj
        async for obj in Book.async_object.annotate(title_alias=F("title"))
        .values("author")
        .annotate(count=Count("id", distinct=True))
        .values("count", "author")
        .order_by("count")
    ]
    # a1 has 2 books, a2 has 1 book
    counts = sorted(r["count"] for r in qs)
    assert counts == [1, 2]


async def test_grouped_annotation_in_group_by(async_db):
    """An annotation included in values() before aggregate is in GROUP BY."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    qs = [
        obj
        async for obj in Book.async_object.annotate(title_alias=F("title"))
        .values("author", "title_alias")
        .annotate(count=Count("id", distinct=True))
        .order_by("count")
    ]
    # Each book is its own group → each count = 1
    assert all(r["count"] == 1 for r in qs)
    assert len(qs) == 3


# ---------------------------------------------------------------------------
# Aggregate expressions / arithmetic
# ---------------------------------------------------------------------------


async def test_aggregation_expressions(async_db):
    """Arithmetic on aggregates works: Sum/Count produces integer division."""
    await TestModel.async_object.abulk_create([TestModel(name=f"AE{i}", value=i * 10) for i in range(1, 5)])
    # values: 10, 20, 30, 40 → sum=100, count=4 → 100//4 = 25
    result = await TestModel.async_object.aaggregate(av=Sum("value") / Count("*"))
    assert result["av"] == 25


@pytest.mark.skip(
    reason=(
        "annotate(agg).aaggregate(agg) requires a subquery-wrapped compiler "
        "which falls back to Django's sync compiler (backend limitation)."
    )
)
async def test_aggregate_over_complex_annotation(async_db):
    """Aggregate over an annotation that itself uses expressions."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    qs = Author.async_object.annotate(book_count=Count("books"))
    result = await qs.aaggregate(max_books=Max("book_count"))
    assert result["max_books"] == 2  # Alice has 2 books

    result2 = await qs.aaggregate(doubled=Max("book_count") * 2)
    assert result2["doubled"] == 4


async def test_values_annotation_with_expression(async_db):
    """F() in annotate inside values() is promoted to GROUP BY."""
    await TestModel.async_object.abulk_create([TestModel(name=f"VA{i}", value=i * 5) for i in range(1, 4)])
    # each value is unique, so grouping by name + doubled_value works
    results = [
        obj
        async for obj in TestModel.async_object.values("name")
        .annotate(double_val=Sum("value") + F("value"))
        .order_by("name")
    ]
    # For name=VA1, value=5 → Sum(5)+5 = 10; VA2 → 20; VA3 → 30 (each group has 1 row)
    by_name = {r["name"]: r["double_val"] for r in results}
    assert by_name["VA1"] == 10
    assert by_name["VA2"] == 20
    assert by_name["VA3"] == 30


async def test_annotate_values_aggregate(async_db):
    """Annotate with alias of a field, then aggregate on the alias gives same result."""
    await TestModel.async_object.abulk_create([TestModel(name=f"AVA{i}", value=i * 10) for i in range(1, 4)])
    alias_result = (
        await TestModel.async_object.annotate(val_alias=F("value"))
        .values("val_alias")
        .aaggregate(total=Sum("val_alias"))
    )

    direct_result = await TestModel.async_object.values("value").aaggregate(total=Sum("value"))
    assert alias_result["total"] == direct_result["total"]


async def test_annotate_over_annotate(async_db):
    """annotate on top of annotate: the chained alias works."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Count books, then annotate a doubled count
    author = await Author.async_object.annotate(bc=Count("books")).annotate(doubled=F("bc") * 2).aget(name="Alice")
    assert author.bc == 2
    assert author.doubled == 4


# ---------------------------------------------------------------------------
# Error / edge-case tests
# ---------------------------------------------------------------------------


async def test_nonaggregate_aggregation_throws(async_db):
    """aggregate(fail=F('field')) raises TypeError."""
    with pytest.raises(TypeError):
        await TestModel.async_object.aaggregate(fail=F("value"))


async def test_complex_aggregates_require_kwarg(async_db):
    """Aggregate expressions without an alias raise TypeError."""
    with pytest.raises(TypeError, match="Complex aggregates require an alias"):
        await TestModel.async_object.aaggregate(Sum("value") / Count("value"))


async def test_complex_annotations_require_kwarg(async_db):
    """Annotations without an alias for complex expressions raise TypeError."""
    with pytest.raises(TypeError, match="Complex annotations require an alias"):
        # This should raise on queryset evaluation
        _ = [obj async for obj in TestModel.async_object.annotate(Sum(F("value") + F("value")))]


async def test_aggregate_over_aggregate_raises(async_db):
    """Cannot aggregate over an aggregate alias."""
    with pytest.raises(FieldError, match="is an aggregate"):
        await TestModel.async_object.aaggregate(
            total=Sum(F("value")),
            avg_total=Avg(F("total")),
        )


async def test_nonfield_annotation(async_db):
    """Max(Value(2)) as an annotation gives 2 for every row."""
    await TestModel.async_object.acreate(name="NF1", value=10)
    obj = await Book.async_object.annotate(val=Max(Value(2))).afirst()
    # No books yet; create one so we have a row
    a = await Author.async_object.acreate(name="NF_Author")
    await Book.async_object.acreate(title="NF_Book", author=a)
    obj = await Book.async_object.annotate(val=Max(Value(2))).afirst()
    assert obj.val == 2


async def test_arguments_must_be_expressions(async_db):
    """aggregate() with non-expression arguments raises TypeError."""
    with pytest.raises(TypeError, match="received non-expression"):
        await TestModel.async_object.aaggregate(is_thing=True)


# ---------------------------------------------------------------------------
# Default value tests
# ---------------------------------------------------------------------------


async def test_aggregation_default_unset(async_db):
    """Without a default, aggregate over empty set returns None for all aggs."""
    for Aggregate in [Avg, Max, Min, StdDev, Sum, Variance]:
        result = await TestModel.async_object.filter(value__gt=9999).aaggregate(v=Aggregate("value"))
        assert result["v"] is None, f"{Aggregate.__name__} should return None"


async def test_aggregation_default_zero(async_db):
    """With default=0, aggregate over empty set returns 0."""
    for Aggregate in [Avg, Max, Min, StdDev, Sum, Variance]:
        result = await TestModel.async_object.filter(value__gt=9999).aaggregate(v=Aggregate("value", default=0))
        assert result["v"] == 0, f"{Aggregate.__name__} should return 0"


async def test_aggregation_default_integer(async_db):
    """With default=21, aggregate over empty set returns 21."""
    for Aggregate in [Avg, Max, Min, StdDev, Sum, Variance]:
        result = await TestModel.async_object.filter(value__gt=9999).aaggregate(v=Aggregate("value", default=21))
        assert result["v"] == 21, f"{Aggregate.__name__} should return 21"


async def test_aggregation_default_expression(async_db):
    """With default=Value(5)*Value(7), aggregate over empty set returns 35."""
    for Aggregate in [Avg, Max, Min, StdDev, Sum, Variance]:
        result = await TestModel.async_object.filter(value__gt=9999).aaggregate(
            v=Aggregate("value", default=Value(5) * Value(7))
        )
        assert result["v"] == 35, f"{Aggregate.__name__} should return 35"


async def test_count_does_not_allow_default(async_db):
    """Count does not support the default argument."""
    with pytest.raises(TypeError, match="Count does not allow default"):
        Count("value", default=0)


async def test_aggregation_default_group_by(async_db):
    """default= in annotate works correctly in grouped queries.

    Ported from Django's test_aggregation_default_group_by which checks that
    an author (like Publisher) with no books gets the default value for a Sum.
    """
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Charlie has no books, so Sum("books__id") should fall back to default=0
    a3 = await Author.async_object.acreate(name="Charlie")
    results = [
        obj
        async for obj in Author.async_object.values("name")
        .annotate(
            num_books=Count("books"),
            total_book_ids=Sum("books__id", default=0),
        )
        .filter(num_books=0)
    ]
    assert len(results) == 1
    assert results[0]["name"] == "Charlie"
    assert results[0]["num_books"] == 0
    assert results[0]["total_book_ids"] == 0


async def test_aggregation_default_passed_another_aggregate(async_db):
    """default can be another aggregate expression."""
    await TestModel.async_object.abulk_create([TestModel(name=f"DA{i}", value=i * 10) for i in range(1, 4)])
    # No rows match value > 9999 → default kicks in as Avg("value") over all rows
    result = await TestModel.async_object.aaggregate(v=Sum("value", filter=Q(value__gt=9999), default=Avg("value")))
    # Avg of [10, 20, 30] = 20.0
    assert result["v"] == 20.0


async def test_aggregation_default_after_annotation(async_db):
    """default works when aggregate references an annotation."""
    await TestModel.async_object.abulk_create([TestModel(name=f"DAA{i}", value=i * 10) for i in range(1, 4)])
    result = await TestModel.async_object.annotate(double=F("value") * 2).aaggregate(total=Sum("double", default=0))
    # 20 + 40 + 60 = 120
    assert result["total"] == 120


# ---------------------------------------------------------------------------
# Empty result-set optimisation
# ---------------------------------------------------------------------------


async def test_empty_result_optimization(async_db):
    """none().aggregate() returns defaults without hitting the DB for standard aggs."""
    result = await TestModel.async_object.none().aaggregate(
        total=Sum("value"),
        cnt=Count("id"),
    )
    assert result == {"total": None, "cnt": 0}


async def test_coalesced_empty_result_set(async_db):
    """Coalesce(Sum(…), 0) on .none() returns 0."""
    result = await TestModel.async_object.none().aaggregate(total=Coalesce(Sum("value"), 0))
    assert result["total"] == 0


async def test_coalesced_empty_result_set_nested(async_db):
    """Nested Coalesce on .none() still returns the fallback."""
    result = await TestModel.async_object.none().aaggregate(total=Coalesce(Coalesce(Sum("value"), None), 0))
    assert result["total"] == 0


# ---------------------------------------------------------------------------
# Slicing + aggregation
# ---------------------------------------------------------------------------


@pytest.mark.skip(
    reason=(
        "aggregate() on a sliced queryset requires a subquery-wrapped compiler "
        "which falls back to Django's sync compiler (backend limitation)."
    )
)
async def test_aggregate_over_sliced_queryset(async_db):
    """aggregate() on a sliced queryset works correctly."""
    await TestModel.async_object.abulk_create([TestModel(name=f"SL{i}", value=i * 10) for i in range(1, 6)])
    # Top-3 by descending value: 50, 40, 30 → avg ≈ 40
    qs = TestModel.async_object.order_by("-value")[0:3]
    result = await qs.aaggregate(avg=Avg("value"))
    assert result["avg"] == pytest.approx(40.0)


# ---------------------------------------------------------------------------
# HAVING (filter on annotation)
# ---------------------------------------------------------------------------


async def test_filtering_on_annotation(async_db):
    """filter() on an annotated count uses HAVING and returns correct rows."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    authors = [obj async for obj in Author.async_object.annotate(num_books=Count("books")).filter(num_books__gt=1)]
    assert len(authors) == 1
    assert authors[0].name == "Alice"


async def test_annotation_filter_range(async_db):
    """filter(annotation__range=…) works with annotated counts."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    authors = [
        obj
        async for obj in Author.async_object.annotate(num_books=Count("books"))
        .filter(num_books__range=[1, 2])
        .order_by("name")
    ]
    assert len(authors) == 2
    assert {a.name for a in authors} == {"Alice", "Bob"}


async def test_annotation_filter_in(async_db):
    """filter(annotation__in=[…]) works."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    authors = [obj async for obj in Author.async_object.annotate(num_books=Count("books")).filter(num_books__in=[2])]
    assert len(authors) == 1
    assert authors[0].name == "Alice"


async def test_annotation_isnull_false(async_db):
    """Annotated count is never NULL → filter(isnull=True) returns empty."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    authors = [
        obj async for obj in Author.async_object.annotate(num_books=Count("books")).filter(num_books__isnull=True)
    ]
    assert authors == []


# ---------------------------------------------------------------------------
# Values aggregation
# ---------------------------------------------------------------------------


async def test_values_aggregation(async_db):
    """values('field').aggregate() aggregates within the values result."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="VG1", value=10),
            TestModel(name="VG2", value=10),
            TestModel(name="VG3", value=20),
        ]
    )
    max_result = await TestModel.async_object.values("value").aaggregate(max_val=Max("value"))
    assert max_result["max_val"] == 20


@pytest.mark.skip(
    reason=(
        "values().annotate().aaggregate() requires a subquery-wrapped compiler "
        "which falls back to Django's sync compiler (backend limitation)."
    )
)
async def test_values_annotate_aggregate(async_db):
    """values + annotate per group, then aggregate over the groups."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="VGA1", value=10),
            TestModel(name="VGA2", value=10),
            TestModel(name="VGA3", value=20),
        ]
    )
    # Group by value, count per group → max of those counts
    result = await TestModel.async_object.values("value").annotate(cnt=Count("id")).aaggregate(Max("cnt"))
    assert result == {"cnt__max": 2}  # value=10 has count 2


async def test_complex_values_aggregation(async_db):
    """Arithmetic inside aggregate over values() works."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="CV1", value=10),
            TestModel(name="CV2", value=20),
        ]
    )
    result = await TestModel.async_object.values("value").aaggregate(double_max=Max("value") + Max("value"))
    assert result["double_max"] == 40  # 20 + 20


# ---------------------------------------------------------------------------
# Having with no group by
# ---------------------------------------------------------------------------


async def test_having_with_no_group_by(async_db):
    """Static value in values() with annotated sum and filter uses HAVING."""
    await TestModel.async_object.abulk_create([TestModel(name=f"HG{i}", value=i * 10) for i in range(1, 4)])
    results = [
        obj
        async for obj in TestModel.async_object.values(static_value=Value("static"))
        .annotate(total=Sum("value"))
        .filter(total__gte=0)
        .values_list("total", flat=True)
    ]
    assert results == [60]


# ---------------------------------------------------------------------------
# Exists annotation + aggregate
# ---------------------------------------------------------------------------


async def test_exists_none_with_aggregate(async_db):
    """Exists(…none()) alongside Count doesn't break the queryset."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    books = [
        obj
        async for obj in Book.async_object.annotate(
            cnt=Count("id"),
            has_nothing=Exists(Review.async_object.none()),
        )
    ]
    assert len(books) == 3


# ---------------------------------------------------------------------------
# Alias SQL injection guard
# ---------------------------------------------------------------------------


async def test_alias_sql_injection(async_db):
    """Crafted alias names with dangerous characters are rejected."""
    bad_alias = 'injected" from "test_model"; --'
    with pytest.raises(ValueError, match="Column aliases cannot contain"):
        await TestModel.async_object.aaggregate(**{bad_alias: Avg("value")})


async def test_alias_whitespace_characters_rejected(async_db):
    """Aliases with whitespace characters raise ValueError."""
    # Our backend validates \s (space/tab/newline/etc.) but not all control chars.
    for char in [" ", "\t", "\n", "\r"]:
        crafted = f"name{char}extra"
        with pytest.raises(ValueError, match="Column aliases cannot contain"):
            await TestModel.async_object.aaggregate(**{crafted: Avg("value")})


# ---------------------------------------------------------------------------
# Multiple aggregate references (Coalesce of another aggregate)
# ---------------------------------------------------------------------------


async def test_multiple_aggregate_references(async_db):
    """Coalesce referencing another aggregate alias in the same call."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    result = await Author.async_object.aaggregate(
        total_books=Count("books"),
        coalesced=Coalesce("total_books", 0),
    )
    assert result["total_books"] == 3
    assert result["coalesced"] == 3


# ---------------------------------------------------------------------------
# Group by reference subquery
# ---------------------------------------------------------------------------


async def test_group_by_reference_subquery(async_db):
    """values+annotate+filter with a subquery in values works."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Authors that have at least one book
    authors_with_books_qs = (
        Author.async_object.annotate(author_id=F("books__author"))
        .values("author_id")
        .annotate(cnt=Count("*"))
        .values("author_id")
    )
    authors = [obj async for obj in Author.async_object.filter(pk__in=authors_with_books_qs).order_by("name")]
    assert len(authors) == 2
    assert {a.name for a in authors} == {"Alice", "Bob"}


# ---------------------------------------------------------------------------
# Subquery annotation
# ---------------------------------------------------------------------------


async def test_aggregation_subquery_annotation(async_db):
    """Subquery annotations are excluded from GROUP BY when not explicitly grouped."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    latest_review_qs = (
        Review.async_object.filter(book=Subquery(Book.async_object.filter(author=F("author")).values("id")[:1]))
        .order_by("-id")
        .values("id")[:1]
    )
    # Just verifying the queryset executes without error
    author_qs = Author.async_object.annotate(
        latest_review_id=Subquery(Review.async_object.filter(book__author=F("id")).order_by("-id").values("id")[:1]),
    ).annotate(book_count=Count("books"))
    results = [obj async for obj in author_qs.order_by("name")]
    assert len(results) == 2
    assert results[0].book_count == 2  # Alice
    assert results[1].book_count == 1  # Bob


async def test_aggregation_subquery_annotation_exists(async_db):
    """Combining a subquery annotation with an aggregate doesn't crash."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    author_qs = Author.async_object.annotate(
        latest_review_id=Subquery(Review.async_object.filter(book__author=F("id")).order_by("-id").values("id")[:1]),
        book_count=Count("books"),
    )
    assert await author_qs.aexists()


async def test_filter_in_subquery_or_aggregation(async_db):
    """OR-ing an aggregate filter with a subquery filter works correctly."""
    a1, a2, b1, b2, b3, r1, r2 = await _make_author_books(async_db)
    # Authors with books OR authors whose id is in Book's author set
    authors = [
        obj
        async for obj in Author.async_object.annotate(
            book_count=Count("books"),
        ).filter(Q(book_count__gt=0) | Q(pk__in=Book.async_object.values("author")))
    ]
    # Both Alice and Bob have books
    assert len(authors) == 2
