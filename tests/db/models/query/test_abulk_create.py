import pytest
from django.db import IntegrityError
from test_app.models import TestModel


async def test_abulk_create_basic(async_db):
    objs = [
        TestModel(name="Bulk1", value=1),
        TestModel(name="Bulk2", value=2),
        TestModel(name="Bulk3", value=3),
    ]
    result = await TestModel.async_object.abulk_create(objs)
    assert len(result) == 3
    for obj in result:
        assert obj.pk is not None
    assert await TestModel.async_object.acount() == 3


async def test_abulk_create_empty(async_db):
    result = await TestModel.async_object.abulk_create([])
    assert result == []
    assert await TestModel.async_object.acount() == 0


async def test_abulk_create_batch_size(async_db):
    objs = [TestModel(name=f"Batch{i}", value=i) for i in range(5)]
    result = await TestModel.async_object.abulk_create(objs, batch_size=2)
    assert len(result) == 5
    assert await TestModel.async_object.acount() == 5


async def test_abulk_create_pks_are_unique(async_db):
    objs = [
        TestModel(name="A", value=1),
        TestModel(name="B", value=2),
    ]
    result = await TestModel.async_object.abulk_create(objs)
    pks = [obj.pk for obj in result]
    assert len(set(pks)) == 2


async def test_abulk_create_persists_to_db(async_db):
    objs = [TestModel(name="Persist1", value=10)]
    await TestModel.async_object.abulk_create(objs)
    fetched = await TestModel.async_object.aget(name="Persist1")
    assert fetched.value == 10


# ── ignore_conflicts ────────────────────────────────────────────────────


async def test_abulk_create_ignore_conflicts_skips_duplicates(async_db):
    """Duplicate names (unique field) are silently ignored."""
    await TestModel.async_object.abulk_create([TestModel(name="Dup", value=1)])
    await TestModel.async_object.abulk_create(
        [TestModel(name="Dup", value=2), TestModel(name="New", value=3)],
        ignore_conflicts=True,
    )
    assert await TestModel.async_object.acount() == 2
    original = await TestModel.async_object.aget(name="Dup")
    assert original.value == 1  # not overwritten


async def test_abulk_create_without_ignore_conflicts_raises(async_db):
    """Without ignore_conflicts, duplicate unique field raises IntegrityError."""
    await TestModel.async_object.abulk_create([TestModel(name="Dup", value=1)])
    with pytest.raises(IntegrityError):
        await TestModel.async_object.abulk_create([TestModel(name="Dup", value=2)])


async def test_abulk_create_ignore_conflicts_all_duplicates(async_db):
    """All rows are duplicates — nothing inserted, no error."""
    await TestModel.async_object.abulk_create([TestModel(name="X", value=1)])
    await TestModel.async_object.abulk_create(
        [TestModel(name="X", value=99)],
        ignore_conflicts=True,
    )
    assert await TestModel.async_object.acount() == 1


# ── update_conflicts (upsert) ──────────────────────────────────────────


async def test_abulk_create_update_conflicts_upserts(async_db):
    """Conflicting rows update the specified fields."""
    await TestModel.async_object.abulk_create([TestModel(name="Upsert", value=1)])
    await TestModel.async_object.abulk_create(
        [TestModel(name="Upsert", value=42)],
        update_conflicts=True,
        update_fields=["value"],
        unique_fields=["name"],
    )
    assert await TestModel.async_object.acount() == 1
    obj = await TestModel.async_object.aget(name="Upsert")
    assert obj.value == 42


async def test_abulk_create_update_conflicts_inserts_new(async_db):
    """Non-conflicting rows are inserted normally during upsert."""
    await TestModel.async_object.abulk_create(
        [TestModel(name="Existing", value=1), TestModel(name="New", value=2)],
        update_conflicts=True,
        update_fields=["value"],
        unique_fields=["name"],
    )
    assert await TestModel.async_object.acount() == 2


async def test_abulk_create_update_conflicts_mixed(async_db):
    """Mix of inserts and updates in a single call."""
    await TestModel.async_object.abulk_create([TestModel(name="A", value=1)])
    await TestModel.async_object.abulk_create(
        [TestModel(name="A", value=10), TestModel(name="B", value=20)],
        update_conflicts=True,
        update_fields=["value"],
        unique_fields=["name"],
    )
    assert await TestModel.async_object.acount() == 2
    a = await TestModel.async_object.aget(name="A")
    b = await TestModel.async_object.aget(name="B")
    assert a.value == 10
    assert b.value == 20


async def test_abulk_create_update_conflicts_requires_update_fields(async_db):
    """update_conflicts without update_fields raises ValueError."""
    with pytest.raises(ValueError, match="Fields that will be updated"):
        await TestModel.async_object.abulk_create(
            [TestModel(name="X", value=1)],
            update_conflicts=True,
            unique_fields=["name"],
        )


async def test_abulk_create_update_conflicts_requires_unique_fields(async_db):
    """update_conflicts without unique_fields raises ValueError."""
    with pytest.raises(ValueError, match="Unique fields"):
        await TestModel.async_object.abulk_create(
            [TestModel(name="X", value=1)],
            update_conflicts=True,
            update_fields=["value"],
        )


async def test_abulk_create_ignore_and_update_conflicts_mutex(async_db):
    """ignore_conflicts and update_conflicts cannot both be True."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        await TestModel.async_object.abulk_create(
            [TestModel(name="X", value=1)],
            ignore_conflicts=True,
            update_conflicts=True,
            update_fields=["value"],
            unique_fields=["name"],
        )
