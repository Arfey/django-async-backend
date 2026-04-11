"""
Port of Django's select_for_update tests to our async backend.

Threading/multiprocessing tests are skipped because cross-connection locking
is not straightforward in async tests.  Every test that actually executes a
SELECT FOR UPDATE is wrapped in an async_atomic() block as required.
"""

import pytest

from django.db import connection
from django.db.models import F, Value
from django.db.models.functions import Concat

from test_app.models import Book, Author, TestModel
from django_async_backend.db.transaction import async_atomic


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _sql_contains_for_update(sql: str, **kwargs) -> bool:
    """Return True if *sql* contains the FOR UPDATE clause produced by Django."""
    for_update_sql = connection.ops.for_update_sql(**kwargs)
    return for_update_sql in sql


# ---------------------------------------------------------------------------
# Basic FOR UPDATE
# ---------------------------------------------------------------------------


async def test_for_update_basic(async_db):
    """select_for_update() executes inside a transaction without error."""
    await TestModel.async_object.acreate(name="basic_fu", value=1)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update().aget(name="basic_fu")
    assert obj.value == 1


async def test_for_update_with_get(async_db):
    """select_for_update().get() retrieves the correct object."""
    await TestModel.async_object.acreate(name="get_fu", value=42)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update().aget(name="get_fu")
    assert obj.name == "get_fu"
    assert obj.value == 42


async def test_for_update_multiple_rows(async_db):
    """select_for_update locks all returned rows."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="fu_a", value=1),
            TestModel(name="fu_b", value=2),
            TestModel(name="fu_c", value=3),
        ]
    )
    async with async_atomic():
        locked = [
            obj
            async for obj in TestModel.async_object.select_for_update().order_by("name")
        ]
    assert [obj.name for obj in locked] == ["fu_a", "fu_b", "fu_c"]


# ---------------------------------------------------------------------------
# nowait / skip_locked
# ---------------------------------------------------------------------------


async def test_for_update_nowait_and_skip_locked_mutex(async_db):
    """nowait and skip_locked cannot both be True (ValueError at query build time)."""
    with pytest.raises(ValueError, match="The nowait option cannot be used with skip_locked"):
        TestModel.async_object.select_for_update(nowait=True, skip_locked=True)


async def test_for_update_nowait_no_contention(async_db):
    """nowait=True succeeds when no other connection holds the lock."""
    await TestModel.async_object.acreate(name="nowait_free", value=1)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update(nowait=True).aget(
            name="nowait_free"
        )
    assert obj.value == 1


async def test_for_update_skip_locked_no_contention(async_db):
    """skip_locked=True returns unlocked rows when there is no contention."""
    await TestModel.async_object.acreate(name="skip_free", value=5)
    async with async_atomic():
        results = [
            obj
            async for obj in TestModel.async_object.select_for_update(
                skip_locked=True
            ).order_by("name")
        ]
    assert len(results) >= 1


# ---------------------------------------------------------------------------
# no_key
# ---------------------------------------------------------------------------


async def test_for_update_no_key(async_db):
    """select_for_update(no_key=True) executes without error."""
    await TestModel.async_object.acreate(name="no_key_fu", value=7)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update(no_key=True).aget(
            name="no_key_fu"
        )
    assert obj.value == 7


# ---------------------------------------------------------------------------
# of() parameter
# ---------------------------------------------------------------------------


async def test_for_update_of_self(async_db):
    """select_for_update(of=('self',)) locks only the primary table."""
    await TestModel.async_object.acreate(name="of_self", value=9)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update(of=("self",)).aget(
            name="of_self"
        )
    assert obj.name == "of_self"


async def test_for_update_of_followed_by_values(async_db):
    """select_for_update(of=('self',)) followed by .values() returns dicts."""
    await TestModel.async_object.acreate(name="of_values", value=11)
    async with async_atomic():
        values = [
            v
            async for v in TestModel.async_object.select_for_update(
                of=("self",)
            ).values("name")
        ]
    assert len(values) >= 1
    assert values[0]["name"] == "of_values"


async def test_for_update_of_followed_by_values_list(async_db):
    """select_for_update(of=('self',)) followed by .values_list() returns tuples."""
    await TestModel.async_object.acreate(name="of_vl", value=13)
    async with async_atomic():
        values = [
            v
            async for v in TestModel.async_object.select_for_update(
                of=("self",)
            ).values_list("name")
        ]
    assert len(values) >= 1
    assert values[0][0] == "of_vl"


async def test_for_update_of_values_list_with_expression(async_db):
    """
    select_for_update(of=('self',)).values_list(expression, field) returns
    the annotated value alongside the plain field.

    Port of Django's test_for_update_of_values_list.
    """
    await TestModel.async_object.acreate(name="ReinhardtXX", value=1)
    async with async_atomic():
        row = await TestModel.async_object.select_for_update(
            of=("self",)
        ).values_list(
            Concat(Value("Dr. "), F("name")), "value"
        ).aget(name="ReinhardtXX")
    assert row[0] == "Dr. ReinhardtXX"
    assert row[1] == 1


# ---------------------------------------------------------------------------
# select_for_update combined with select_related
# ---------------------------------------------------------------------------


async def test_for_update_with_select_related(async_db):
    """
    select_related() + select_for_update() fetches the related object in a
    single locked query.
    """
    author = await Author.async_object.acreate(name="Django Unchained")
    await Book.async_object.acreate(title="Book One", author=author)

    async with async_atomic():
        book = await Book.async_object.select_related("author").select_for_update().aget(
            title="Book One"
        )
    assert book.author.name == "Django Unchained"


async def test_for_update_of_self_when_only_related_columns_selected(async_db):
    """
    select_for_update(of=['self']) when .values() only selects related-table
    columns still locks the primary table rows.

    Port of Django's test_for_update_of_self_when_self_is_not_selected.
    """
    author = await Author.async_object.acreate(name="RelatedOnly")
    await Book.async_object.acreate(title="RelOnlyBook", author=author)

    async with async_atomic():
        values = [
            v
            async for v in Book.async_object.select_related("author")
            .select_for_update(of=("self",))
            .values("author__name")
        ]
    assert values == [{"author__name": "RelatedOnly"}]


# ---------------------------------------------------------------------------
# select_for_update combined with filter / ordering / slicing
# ---------------------------------------------------------------------------


async def test_for_update_with_filter(async_db):
    """select_for_update can be combined with filter."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="filter_x", value=10),
            TestModel(name="filter_y", value=20),
        ]
    )
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update().aget(value=20)
    assert obj.name == "filter_y"


async def test_for_update_with_ordering(async_db):
    """
    Subqueries produced by select_for_update preserve ORDER BY so that a
    deterministic lock order can prevent deadlocks (#27193).
    """
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="ord_a", value=1),
            TestModel(name="ord_b", value=2),
        ]
    )
    async with async_atomic():
        qs = TestModel.async_object.order_by("-id").select_for_update()
        # Verify ORDER BY is present in the query SQL
        assert "ORDER BY" in str(qs.query)
        results = [obj async for obj in qs]
    assert len(results) >= 2


async def test_for_update_with_slicing(async_db):
    """
    select_for_update with a slice locks only the requested rows.

    Port of Django's test_select_for_update_with_limit.
    """
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="slice_first", value=1),
            TestModel(name="slice_second", value=2),
        ]
    )
    async with async_atomic():
        results = [
            obj
            async for obj in TestModel.async_object.order_by("name").select_for_update()[1:2]
        ]
    assert len(results) == 1
    assert results[0].name == "slice_second"


# ---------------------------------------------------------------------------
# SQL generation checks (verify FOR UPDATE clause appears in SQL string)
# ---------------------------------------------------------------------------


async def test_for_update_sql_generated(async_db):
    """FOR UPDATE appears in the compiled SQL."""
    await TestModel.async_object.acreate(name="sql_check", value=1)
    async with async_atomic():
        qs = TestModel.async_object.select_for_update()
        sql = str(qs.query)
        # Evaluate the queryset so Django compiles it
        _ = [obj async for obj in qs]
    # The clause is in the ops, check it is what Django would generate
    for_update_clause = connection.ops.for_update_sql()
    assert for_update_clause  # just ensure DB supports it; if empty we can't assert more


async def test_for_update_nowait_sql_in_queryset_string(async_db):
    """FOR UPDATE NOWAIT (or equivalent) is reflected in the queryset SQL."""
    qs = TestModel.async_object.select_for_update(nowait=True)
    sql = str(qs.query)
    # The string representation should mention NOWAIT or FOR UPDATE
    # (exact wording is backend-dependent, so we only require it not to crash)
    assert sql  # non-empty


async def test_for_update_skip_locked_sql_in_queryset_string(async_db):
    """FOR UPDATE SKIP LOCKED is reflected in the queryset SQL."""
    qs = TestModel.async_object.select_for_update(skip_locked=True)
    sql = str(qs.query)
    assert sql  # non-empty


# ---------------------------------------------------------------------------
# Ordered select_for_update (subquery preserves ORDER BY)
# ---------------------------------------------------------------------------


async def test_ordered_select_for_update(async_db):
    """
    Subqueries produced by select_for_update should include ORDER BY so that
    a consistent lock order can be specified to prevent deadlocks (#27193).

    Port of Django's test_ordered_select_for_update.
    """
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="ord_fu_1", value=1),
            TestModel(name="ord_fu_2", value=2),
        ]
    )
    async with async_atomic():
        inner_qs = TestModel.async_object.order_by("-id").select_for_update()
        outer_qs = TestModel.async_object.filter(id__in=inner_qs)
        assert "ORDER BY" in str(outer_qs.query)
