import pytest
from shared.models import TestModel

from django_async_backend.db.transaction import async_atomic


async def test_select_for_update_basic(async_db):
    """select_for_update() acquires a row lock within a transaction."""
    await TestModel.async_object.acreate(name="Locked", value=1)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update().aget(name="Locked")
        assert obj.value == 1


async def test_select_for_update_nowait_and_skip_locked_mutex(async_db):
    """nowait and skip_locked cannot both be True."""
    with pytest.raises(ValueError, match="cannot be used with"):
        TestModel.async_object.select_for_update(nowait=True, skip_locked=True)


async def test_select_for_update_no_key(async_db):
    """select_for_update(no_key=True) uses FOR NO KEY UPDATE."""
    await TestModel.async_object.acreate(name="NoKey", value=1)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update(no_key=True).aget(name="NoKey")
        assert obj.value == 1


async def test_select_for_update_multiple_rows(async_db):
    """select_for_update locks all returned rows."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="A", value=1),
            TestModel(name="B", value=2),
            TestModel(name="C", value=3),
        ]
    )
    async with async_atomic():
        locked = [obj async for obj in TestModel.async_object.select_for_update().order_by("name")]
        assert len(locked) == 3
        assert [obj.name for obj in locked] == ["A", "B", "C"]


async def test_select_for_update_with_filter(async_db):
    """select_for_update can be combined with filter."""
    await TestModel.async_object.abulk_create(
        [
            TestModel(name="X", value=10),
            TestModel(name="Y", value=20),
        ]
    )
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update().aget(value=20)
        assert obj.name == "Y"


async def test_select_for_update_skip_locked_basic(async_db):
    """skip_locked flag is accepted and returns results."""
    await TestModel.async_object.acreate(name="Row1", value=1)
    async with async_atomic():
        results = [obj async for obj in TestModel.async_object.select_for_update(skip_locked=True)]
        assert len(results) == 1


async def test_select_for_update_nowait_no_contention(async_db):
    """nowait succeeds when there's no contention."""
    await TestModel.async_object.acreate(name="Free", value=1)
    async with async_atomic():
        obj = await TestModel.async_object.select_for_update(nowait=True).aget(name="Free")
        assert obj.value == 1
