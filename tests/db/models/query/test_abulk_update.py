import pytest
from shared.models import TestModel


async def test_abulk_update_basic(async_db):
    obj1 = await TestModel.async_object.acreate(name="A", value=1)
    obj2 = await TestModel.async_object.acreate(name="B", value=2)
    obj3 = await TestModel.async_object.acreate(name="C", value=3)

    obj1.value = 10
    obj2.value = 20
    obj3.value = 30
    rows = await TestModel.async_object.abulk_update([obj1, obj2, obj3], ["value"])
    assert rows == 3

    fetched = await TestModel.async_object.aget(pk=obj1.pk)
    assert fetched.value == 10
    fetched = await TestModel.async_object.aget(pk=obj2.pk)
    assert fetched.value == 20


async def test_abulk_update_empty(async_db):
    await TestModel.async_object.acreate(name="A", value=1)
    rows = await TestModel.async_object.abulk_update([], ["value"])
    assert rows == 0


async def test_abulk_update_batch_size(async_db):
    obj1 = await TestModel.async_object.acreate(name="A", value=1)
    obj2 = await TestModel.async_object.acreate(name="B", value=2)
    obj3 = await TestModel.async_object.acreate(name="C", value=3)

    obj1.value = 100
    obj2.value = 200
    obj3.value = 300
    rows = await TestModel.async_object.abulk_update([obj1, obj2, obj3], ["value"], batch_size=2)
    assert rows == 3


async def test_abulk_update_no_fields_raises(async_db):
    obj1 = await TestModel.async_object.acreate(name="A", value=1)
    with pytest.raises(ValueError):
        await TestModel.async_object.abulk_update([obj1], [])
