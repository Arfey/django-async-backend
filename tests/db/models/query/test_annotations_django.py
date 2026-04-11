"""
Port of Django's annotation tests to our async backend.

Source: /tmp/django_annotations_tests.py (NonAggregateAnnotationTestCase, AliasTests)
Skipped: M2M relations, DepartmentStore/Company/Employee/Ticket/Store/Publisher models,
         JSON fields, distinct-on (DB-specific), raw SQL with inherited fields.
"""

import pytest
from django.core.exceptions import FieldError
from django.db.models import (
    BooleanField,
    Case,
    Count,
    Exists,
    ExpressionWrapper,
    F,
    IntegerField,
    Max,
    OuterRef,
    Q,
    Subquery,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Coalesce, Length, Lower, Upper
from test_app.models import Author, Book, Review, TestModel

# ---------------------------------------------------------------------------
# Basic Value / F annotations
# ---------------------------------------------------------------------------


async def test_basic_value_annotation(async_db):
    """Value(1) annotated onto every row."""
    await Author.async_object.acreate(name="Alice")
    await Author.async_object.acreate(name="Bob")
    results = [a async for a in Author.async_object.annotate(is_author=Value(1))]
    assert len(results) == 2
    for a in results:
        assert a.is_author == 1


async def test_basic_f_annotation(async_db):
    """F('name') annotated as an alias."""
    await Author.async_object.acreate(name="Alice")
    results = [a async for a in Author.async_object.annotate(another_name=F("name"))]
    assert results[0].another_name == results[0].name


async def test_null_annotation(async_db):
    """Annotating None onto a model round-trips."""
    await Author.async_object.acreate(name="Alice")
    result = await Author.async_object.annotate(no_value=Value(None, output_field=IntegerField())).afirst()
    assert result.no_value is None


async def test_boolean_value_annotation(async_db):
    """Boolean Value annotations."""
    await Author.async_object.acreate(name="Alice")
    results = [
        a
        async for a in Author.async_object.annotate(
            is_author=Value(True, output_field=BooleanField()),
            is_robot=Value(False, output_field=BooleanField()),
            is_none=Value(None, output_field=BooleanField(null=True)),
        )
    ]
    assert len(results) > 0
    for a in results:
        assert a.is_author is True
        assert a.is_robot is False
        assert a.is_none is None


# ---------------------------------------------------------------------------
# F-expression arithmetic
# ---------------------------------------------------------------------------


async def test_annotate_f_arithmetic(async_db):
    """F arithmetic annotations (doubled, tripled)."""
    await TestModel.async_object.acreate(name="X", value=5)
    results = [
        obj async for obj in TestModel.async_object.annotate(doubled=F("value") * 2).annotate(tripled=F("value") * 3)
    ]
    assert results[0].doubled == 10
    assert results[0].tripled == 15


async def test_combined_annotation_commutative(async_db):
    """F('value') + 2  ==  2 + F('value')."""
    await TestModel.async_object.acreate(name="X", value=5)
    obj1 = await TestModel.async_object.annotate(adj=F("value") + 2).afirst()
    obj2 = await TestModel.async_object.annotate(adj=2 + F("value")).afirst()
    assert obj1.adj == obj2.adj


async def test_combined_annotation_with_none(async_db):
    """F('value') + None == None."""
    await TestModel.async_object.acreate(name="X", value=5)
    obj1 = await TestModel.async_object.annotate(adj=F("value") + None).afirst()
    obj2 = await TestModel.async_object.annotate(adj=None + F("value")).afirst()
    assert obj1.adj is None
    assert obj1.adj == obj2.adj


# ---------------------------------------------------------------------------
# ExpressionWrapper
# ---------------------------------------------------------------------------


async def test_expression_wrapper_arithmetic(async_db):
    """ExpressionWrapper around integer arithmetic."""
    obj = await TestModel.async_object.acreate(name="X", value=7)
    result = await TestModel.async_object.annotate(
        combined=ExpressionWrapper(F("value") * 2, output_field=IntegerField())
    ).afirst()
    assert result.combined == 14


async def test_empty_expression_annotation(async_db):
    """ExpressionWrapper(Q(pk__in=[]), BooleanField()) is False for every row."""
    await Author.async_object.acreate(name="Alice")
    results = [
        a
        async for a in Author.async_object.annotate(
            selected=ExpressionWrapper(Q(pk__in=[]), output_field=BooleanField())
        )
    ]
    assert len(results) > 0
    assert all(not a.selected for a in results)


async def test_full_expression_annotation(async_db):
    """~Q(pk__in=[]) is True for every row."""
    await Author.async_object.acreate(name="Alice")
    results = [
        a
        async for a in Author.async_object.annotate(
            selected=ExpressionWrapper(~Q(pk__in=[]), output_field=BooleanField())
        )
    ]
    assert len(results) > 0
    assert all(a.selected for a in results)


async def test_full_expression_annotation_with_aggregation(async_db):
    """Combine a boolean ExpressionWrapper with an aggregate."""
    a = await Author.async_object.acreate(name="Alice")
    b = await Book.async_object.acreate(title="B1", author=a)
    results = [
        obj
        async for obj in Book.async_object.filter(pk=b.pk).annotate(
            selected=ExpressionWrapper(~Q(pk__in=[]), output_field=BooleanField()),
            review_count=Count("reviews"),
        )
    ]
    assert len(results) == 1
    assert results[0].selected is True
    assert results[0].review_count == 0


async def test_combined_expression_annotation_with_aggregation(async_db):
    """Value(3) * Value(4) and Count together."""
    a = await Author.async_object.acreate(name="Alice")
    await Book.async_object.acreate(title="B1", author=a)
    result = await Book.async_object.annotate(
        combined=ExpressionWrapper(Value(3) * Value(4), output_field=IntegerField()),
        review_count=Count("reviews"),
    ).afirst()
    assert result.combined == 12
    assert result.review_count == 0


# ---------------------------------------------------------------------------
# String-function annotations
# ---------------------------------------------------------------------------


async def test_annotate_upper_lower_length(async_db):
    """Upper, Lower, Length on name field."""
    await TestModel.async_object.acreate(name="Hello", value=1)
    result = await TestModel.async_object.annotate(
        upper_name=Upper("name"),
        lower_name=Lower("name"),
        name_length=Length("name"),
    ).afirst()
    assert result.upper_name == "HELLO"
    assert result.lower_name == "hello"
    assert result.name_length == 5


# ---------------------------------------------------------------------------
# Aggregate annotations
# ---------------------------------------------------------------------------


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


async def test_annotate_exists(async_db):
    """annotate + Count, then filter(c__gt=1) finds nothing when each id is unique."""
    await Author.async_object.acreate(name="Alice")
    await Author.async_object.acreate(name="Bob")
    authors = [a async for a in Author.async_object.annotate(c=Count("id")).filter(c__gt=1)]
    assert authors == []


async def test_annotate_min_max_reviews(async_db):
    """Min/Max aggregate on a nested relation."""
    a = await Author.async_object.acreate(name="A")
    b1 = await Book.async_object.acreate(title="B1", author=a)
    b2 = await Book.async_object.acreate(title="B2", author=a)
    await Review.async_object.acreate(text="Short", book=b1)
    await Review.async_object.acreate(text="LongerReview", book=b2)

    books = [b async for b in Book.async_object.annotate(review_count=Count("reviews")).order_by("title")]
    assert books[0].review_count == 1
    assert books[1].review_count == 1


async def test_aggregate_over_annotation(async_db):
    """aggregate() over an annotated field matches direct aggregate."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
            TestModel(name="C", value=30),
        ]
    )
    agg = await TestModel.async_object.annotate(other_value=F("value")).aaggregate(other_sum=Sum("other_value"))
    direct_agg = await TestModel.async_object.aaggregate(value_sum=Sum("value"))
    assert agg["other_sum"] == direct_agg["value_sum"]


async def test_values_annotate_multiple_aggregates(async_db):
    """Multiple aggregates grouped by values."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    for i in range(3):
        await Book.async_object.acreate(title=f"Book{i}", author=a1)
    await Book.async_object.acreate(title="Single", author=a2)

    results = [r async for r in Author.async_object.values("name").annotate(book_count=Count("books")).order_by("name")]
    assert results[0]["name"] == "A1"
    assert results[0]["book_count"] == 3
    assert results[1]["name"] == "A2"
    assert results[1]["book_count"] == 1


async def test_order_by_aggregate(async_db):
    """Order by an aggregate annotation."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    a3 = await Author.async_object.acreate(name="A3")
    # a1 has 2 books, a2 and a3 have 1 each
    for title in ("X", "Y"):
        await Book.async_object.acreate(title=title, author=a1)
    await Book.async_object.acreate(title="Z", author=a2)
    await Book.async_object.acreate(title="W", author=a3)

    results = [
        r
        async for r in Author.async_object.values("name")
        .annotate(book_count=Count("books"))
        .order_by("book_count", "name")
    ]
    counts = [r["book_count"] for r in results]
    # a2 and a3 each have 1, a1 has 2; sorted ascending
    assert counts == sorted(counts)
    assert counts[-1] == 2


# ---------------------------------------------------------------------------
# Filter / exclude on annotations
# ---------------------------------------------------------------------------


async def test_filter_annotation_value(async_db):
    """Filter on an annotated Value."""
    a = await Author.async_object.acreate(name="Alice")
    results = [a async for a in Author.async_object.annotate(is_author=Value(1)).filter(is_author=1)]
    assert len(results) == 1


async def test_filter_annotation_with_f(async_db):
    """Filter on an annotated F expression."""
    await TestModel.async_object.acreate(name="A", value=5)
    await TestModel.async_object.acreate(name="B", value=10)
    results = [obj async for obj in TestModel.async_object.annotate(other_value=F("value")).filter(other_value=5)]
    assert len(results) == 1
    assert results[0].name == "A"


async def test_filter_annotation_with_double_f(async_db):
    """Filter where annotation equals its own source field (tautology)."""
    await TestModel.async_object.acreate(name="A", value=5)
    await TestModel.async_object.acreate(name="B", value=10)
    results = [
        obj async for obj in TestModel.async_object.annotate(other_value=F("value")).filter(other_value=F("value"))
    ]
    assert len(results) == 2


async def test_filter_on_count_annotation(async_db):
    """Filter using an annotated Count field."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a2)

    prolific = [a async for a in Author.async_object.annotate(book_count=Count("books")).filter(book_count__gte=2)]
    assert len(prolific) == 1
    assert prolific[0].name == "A1"


async def test_order_by_annotation(async_db):
    """Order_by an annotated field."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="C", value=30),
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
        ]
    )
    results = [obj async for obj in TestModel.async_object.annotate(other_value=F("value")).order_by("other_value")]
    assert [obj.other_value for obj in results] == [10, 20, 30]


# ---------------------------------------------------------------------------
# values() + annotate
# ---------------------------------------------------------------------------


async def test_values_annotation_f_minus(async_db):
    """values() referencing F expression annotation."""
    await TestModel.async_object.acreate(name="A", value=10)
    results = [r async for r in TestModel.async_object.values("value").annotate(other_value=F("value") - 1)]
    assert results[0]["value"] - 1 == results[0]["other_value"]


async def test_annotate_in_f_grouped_by_annotation(async_db):
    """Sum of multiplier * value grouped by name."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="X", value=3),
            TestModel(name="Y", value=5),
        ]
    )
    results = [
        r
        async for r in TestModel.async_object.annotate(multiplier=Value(2))
        .values("name")
        .annotate(multiplied_sum=Sum(F("multiplier") * F("value")))
        .order_by("name")
    ]
    assert results[0]["name"] == "X"
    assert results[0]["multiplied_sum"] == 6
    assert results[1]["name"] == "Y"
    assert results[1]["multiplied_sum"] == 10


# ---------------------------------------------------------------------------
# Case / When
# ---------------------------------------------------------------------------


async def test_case_when_annotation(async_db):
    """Case/When annotation."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="Small", value=5),
            TestModel(name="Large", value=50),
        ]
    )
    results = [
        obj
        async for obj in TestModel.async_object.annotate(
            size=Case(
                When(value__gte=10, then=Value("big")),
                default=Value("small"),
            )
        ).order_by("name")
    ]
    # "Large" (value=50) -> big, "Small" (value=5) -> small
    by_name = {obj.name: obj.size for obj in results}
    assert by_name["Large"] == "big"
    assert by_name["Small"] == "small"


async def test_case_when_with_q(async_db):
    """Case/When using Q lookups."""
    a = await Author.async_object.acreate(name="A")
    b1 = await Book.async_object.acreate(title="Alpha", author=a)
    b2 = await Book.async_object.acreate(title="Beta", author=a)

    results = [
        obj
        async for obj in Book.async_object.annotate(
            starts_with_a=Case(
                When(Q(title__startswith="A"), then=Value(True, output_field=BooleanField())),
                default=Value(False, output_field=BooleanField()),
            )
        ).order_by("title")
    ]
    assert results[0].starts_with_a is True  # Alpha
    assert results[1].starts_with_a is False  # Beta


# ---------------------------------------------------------------------------
# Subquery + OuterRef
# ---------------------------------------------------------------------------


async def test_annotation_with_subquery(async_db):
    """Annotate with a correlated Subquery."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    await Book.async_object.acreate(title="First", author=a1)
    await Book.async_object.acreate(title="Second", author=a1)
    await Book.async_object.acreate(title="Only", author=a2)

    first_book = Subquery(Book.async_object.filter(author=OuterRef("pk")).order_by("title").values("title")[:1])
    authors = [a async for a in Author.async_object.annotate(first_book=first_book).order_by("name")]
    assert authors[0].first_book == "First"
    assert authors[1].first_book == "Only"


async def test_empty_queryset_subquery_annotation(async_db):
    """Subquery over .none() returns None."""
    await Author.async_object.acreate(name="Alice")
    result = await Author.async_object.annotate(empty=Subquery(Author.async_object.values("id").none())).afirst()
    assert result.empty is None


async def test_annotation_filter_with_subquery(async_db):
    """Annotate then filter against a Subquery value."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    # a1 has 2 books, a2 has 1
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a2)

    # Subquery counts books per author; filter where total_books == subquery count
    book_count_sq = (
        Book.async_object.filter(author=OuterRef("pk")).values("author").annotate(c=Count("pk")).values("c")[:1]
    )
    results = [
        a
        async for a in Author.async_object.annotate(total_books=Count("books")).filter(
            total_books=Subquery(book_count_sq, output_field=IntegerField())
        )
    ]
    assert len(results) == 2  # both match since sq == annotation


async def test_annotation_and_alias_filter_in_subquery(async_db):
    """annotate + alias then filter another queryset using pk__in."""
    a1 = await Author.async_object.acreate(name="ManyBooks")
    a2 = await Author.async_object.acreate(name="FewBooks")
    for i in range(3):
        await Book.async_object.acreate(title=f"Bk{i}", author=a1)
    await Book.async_object.acreate(title="Solo", author=a2)

    # Authors with more than 1 book: must project to a single column for pk__in
    many_books_qs = (
        Author.async_object.annotate(bc=Count("books")).filter(bc__gt=1).alias(is_prolific=Value(1)).values("pk")
    )
    results = [a async for a in Author.async_object.filter(pk__in=many_books_qs)]
    assert len(results) == 1
    assert results[0].name == "ManyBooks"


# ---------------------------------------------------------------------------
# Exists
# ---------------------------------------------------------------------------


async def test_exists_annotation(async_db):
    """Annotate with Exists subquery."""
    a1 = await Author.async_object.acreate(name="WithBooks")
    a2 = await Author.async_object.acreate(name="NoBooks")
    await Book.async_object.acreate(title="B1", author=a1)

    results = [
        a
        async for a in Author.async_object.annotate(
            has_books=Exists(Book.async_object.filter(author=OuterRef("pk")))
        ).order_by("name")
    ]
    by_name = {a.name: a.has_books for a in results}
    assert by_name["NoBooks"] is False
    assert by_name["WithBooks"] is True


async def test_exists_annotation_none_queryset(async_db):
    """Exists over .none() is always False."""
    a = await Author.async_object.acreate(name="Alice")
    result = await Author.async_object.annotate(exists=Exists(Author.async_object.none())).afirst()
    assert result.exists is False


async def test_exists_filter(async_db):
    """Filter using Exists annotation."""
    a1 = await Author.async_object.acreate(name="WithBooks")
    a2 = await Author.async_object.acreate(name="NoBooks")
    await Book.async_object.acreate(title="B1", author=a1)

    results = [
        a
        async for a in Author.async_object.annotate(
            has_books=Exists(Book.async_object.filter(author=OuterRef("pk")))
        ).filter(has_books=True)
    ]
    assert len(results) == 1
    assert results[0].name == "WithBooks"


# ---------------------------------------------------------------------------
# Aggregate with filter argument (conditional aggregation)
# ---------------------------------------------------------------------------


async def test_conditional_count_with_filter(async_db):
    """Count with filter= kwarg (conditional aggregation)."""
    a = await Author.async_object.acreate(name="A")
    b1 = await Book.async_object.acreate(title="Python Tricks", author=a)
    b2 = await Book.async_object.acreate(title="Java Guide", author=a)
    await Review.async_object.acreate(text="Great", book=b1)
    await Review.async_object.acreate(text="OK", book=b1)
    await Review.async_object.acreate(text="Meh", book=b2)

    results = [
        r
        async for r in Book.async_object.annotate(
            total_reviews=Count("reviews"),
            python_reviews=Count("reviews", filter=Q(title__icontains="python")),
        ).order_by("title")
    ]
    by_title = {r.title: r for r in results}
    # "Java Guide" has 1 review but python_reviews=0 because title doesn't match
    assert by_title["Java Guide"].total_reviews == 1
    assert by_title["Java Guide"].python_reviews == 0
    # "Python Tricks" has 2 reviews and python_reviews=2
    assert by_title["Python Tricks"].total_reviews == 2
    assert by_title["Python Tricks"].python_reviews == 2


async def test_sum_with_filter(async_db):
    """Sum with filter= kwarg."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
            TestModel(name="C", value=5),
        ]
    )
    result = await TestModel.async_object.aaggregate(high_sum=Sum("value", filter=Q(value__gte=10)))
    assert result["high_sum"] == 30  # 10 + 20


# ---------------------------------------------------------------------------
# Annotation chaining
# ---------------------------------------------------------------------------


async def test_annotation_chaining(async_db):
    """Chained annotate() calls can reference previous annotations."""
    await TestModel.async_object.acreate(name="X", value=4)
    result = (
        await TestModel.async_object.annotate(doubled=F("value") * 2).annotate(quadrupled=F("doubled") * 2).afirst()
    )
    assert result.doubled == 8
    assert result.quadrupled == 16


async def test_annotation_used_in_order_by(async_db):
    """Annotate then order_by the annotation."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a1)
    await Book.async_object.acreate(title="B3", author=a2)

    results = [a async for a in Author.async_object.annotate(bc=Count("books")).order_by("-bc", "name")]
    assert results[0].name == "A1"  # 2 books
    assert results[1].name == "A2"  # 1 book


async def test_annotation_coalesce(async_db):
    """Coalesce annotation."""
    await TestModel.async_object.acreate(name="NoValue", value=None)
    result = await TestModel.async_object.annotate(safe_value=Coalesce("value", Value(0))).afirst()
    assert result.safe_value == 0


# ---------------------------------------------------------------------------
# alias()
# ---------------------------------------------------------------------------


async def test_basic_alias(async_db):
    """alias() does not appear as an attribute."""
    await Author.async_object.acreate(name="Alice")
    result = await Author.async_object.alias(is_author=Value(1)).afirst()
    assert not hasattr(result, "is_author")


async def test_basic_alias_annotation(async_db):
    """alias() used by a subsequent annotate()."""
    await Author.async_object.acreate(name="Alice")
    results = [
        a async for a in Author.async_object.alias(is_author_alias=Value(1)).annotate(is_author=F("is_author_alias"))
    ]
    assert not hasattr(results[0], "is_author_alias")
    assert results[0].is_author == 1


async def test_overwrite_annotation_with_alias(async_db):
    """alias() can overwrite a prior annotate(), removing the attribute."""
    await Author.async_object.acreate(name="Alice")
    result = await Author.async_object.annotate(is_author=Value(1)).alias(is_author=F("is_author")).afirst()
    assert not hasattr(result, "is_author")


async def test_overwrite_alias_with_annotation(async_db):
    """annotate() can overwrite a prior alias(), exposing the attribute."""
    await Author.async_object.acreate(name="Alice")
    results = [a async for a in Author.async_object.alias(is_author=Value(1)).annotate(is_author=F("is_author"))]
    assert results[0].is_author == 1


async def test_alias_after_annotation(async_db):
    """alias() after annotate() hides the alias but keeps the annotation."""
    await Author.async_object.acreate(name="Alice")
    result = await Author.async_object.annotate(is_author=Value(1)).alias(is_author_alias=F("is_author")).afirst()
    assert hasattr(result, "is_author")
    assert not hasattr(result, "is_author_alias")


async def test_filter_alias_with_f(async_db):
    """Filter on an alias."""
    await TestModel.async_object.acreate(name="A", value=5)
    await TestModel.async_object.acreate(name="B", value=10)
    results = [obj async for obj in TestModel.async_object.alias(other_value=F("value")).filter(other_value=5)]
    assert not hasattr(results[0], "other_value")
    assert len(results) == 1
    assert results[0].name == "A"


async def test_order_by_alias(async_db):
    """Order by an alias."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="C", value=30),
            TestModel(name="A", value=10),
            TestModel(name="B", value=20),
        ]
    )
    results = [obj async for obj in TestModel.async_object.alias(other_value=F("value")).order_by("other_value")]
    assert not hasattr(results[0], "other_value")
    assert [obj.name for obj in results] == ["A", "B", "C"]


async def test_order_by_alias_aggregate(async_db):
    """Order by an alias aggregate."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    for i in range(3):
        await Book.async_object.acreate(title=f"Book{i}", author=a1)
    await Book.async_object.acreate(title="Solo", author=a2)

    results = [r async for r in Author.async_object.values("name").alias(bc=Count("books")).order_by("bc", "name")]
    # A2 (1 book) before A1 (3 books)
    assert results[0]["name"] == "A2"
    assert results[-1]["name"] == "A1"


async def test_alias_annotation_expression(async_db):
    """alias used inside Coalesce annotation."""
    await Author.async_object.acreate(name="Alice")
    results = [
        a
        async for a in Author.async_object.alias(is_author_alias=Value(1)).annotate(
            is_author=Coalesce("is_author_alias", 0)
        )
    ]
    assert not hasattr(results[0], "is_author_alias")
    assert results[0].is_author == 1


async def test_values_alias_raises(async_db):
    """values() on an alias raises FieldError."""
    await Author.async_object.acreate(name="Alice")
    qs = Author.async_object.alias(is_author=Value(1))
    with pytest.raises(FieldError):
        list(qs.values("is_author"))


async def test_aggregate_alias_raises(async_db):
    """aggregate() over an alias raises FieldError."""
    await Author.async_object.acreate(name="Alice")
    with pytest.raises(FieldError):
        await Author.async_object.alias(other_name=F("name")).aaggregate(name_count=Count("other_name"))


async def test_alias_annotate_with_aggregation(async_db):
    """alias + annotate combining Value and Count."""
    a = await Author.async_object.acreate(name="Alice")
    await Book.async_object.acreate(title="B1", author=a)

    results = [
        r
        async for r in Book.async_object.alias(
            is_book_alias=Value(1),
            review_count_alias=Count("reviews"),
        ).annotate(
            is_book=F("is_book_alias"),
            review_count=F("review_count_alias"),
        )
    ]
    assert not hasattr(results[0], "is_book_alias")
    assert not hasattr(results[0], "review_count_alias")
    assert results[0].is_book == 1
    assert results[0].review_count == 0


# ---------------------------------------------------------------------------
# arguments_must_be_expressions
# ---------------------------------------------------------------------------


async def test_arguments_must_be_expressions(async_db):
    """annotate() rejects non-expression arguments."""
    with pytest.raises(TypeError):
        Author.async_object.annotate(BooleanField())
    with pytest.raises(TypeError):
        Author.async_object.annotate(is_author=True)


# ---------------------------------------------------------------------------
# Alias SQL injection protection
# ---------------------------------------------------------------------------


async def test_alias_sql_injection_protection(async_db):
    """Dangerous alias names raise ValueError."""
    with pytest.raises(ValueError):
        Author.async_object.annotate(**{'injected" from "test_author"; --': Value(1)})

    with pytest.raises(ValueError):
        Author.async_object.alias(**{'injected" from "test_author"; --': Value(1)})


# ---------------------------------------------------------------------------
# FK traversal in annotations
# ---------------------------------------------------------------------------


async def test_annotation_fk_traversal(async_db):
    """Annotate using a FK traversal (book__author__name style)."""
    a = await Author.async_object.acreate(name="Alice")
    b = await Book.async_object.acreate(title="MyBook", author=a)
    r = await Review.async_object.acreate(text="Excellent", book=b)

    results = [rev async for rev in Review.async_object.annotate(author_name=F("book__author__name"))]
    assert results[0].author_name == "Alice"


async def test_annotation_reverse_fk(async_db):
    """Annotate with Max over reverse FK."""
    a = await Author.async_object.acreate(name="A")
    b1 = await Book.async_object.acreate(title="Alpha", author=a)
    b2 = await Book.async_object.acreate(title="Zeta", author=a)

    result = await Author.async_object.annotate(last_title=Max("books__title")).afirst()
    assert result.last_title == "Zeta"


# ---------------------------------------------------------------------------
# update() with annotation
# ---------------------------------------------------------------------------


async def test_update_with_annotation(async_db):
    """update() can reference an annotated field."""
    obj = await TestModel.async_object.acreate(name="ToUpdate", value=10)
    await (
        TestModel.async_object.annotate(new_value=F("value") + 5).filter(name="ToUpdate").aupdate(value=F("new_value"))
    )
    await obj.arefresh_from_db()
    assert obj.value == 15
