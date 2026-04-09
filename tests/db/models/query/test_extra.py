import pytest
from test_app.models import TestModel


async def test_extra_select(async_db):
    await TestModel.async_object.acreate(name="Test1", value=3)
    await TestModel.async_object.acreate(name="Test2", value=1)
    await TestModel.async_object.acreate(name="Test3", value=2)

    results = [obj async for obj in TestModel.async_object.extra(select={"value_plus_one": "value + 1"})]

    assert len(results) == 3
    assert results[0].value_plus_one == 4
    assert results[1].value_plus_one == 2
    assert results[2].value_plus_one == 3


async def test_extra_where(async_db):
    await TestModel.async_object.acreate(name="Test1", value=3)
    await TestModel.async_object.acreate(name="Test2", value=1)
    await TestModel.async_object.acreate(name="Test3", value=2)

    results = [obj async for obj in TestModel.async_object.extra(where=["value > 1"])]

    assert len(results) == 2
    assert results[0].value == 3
    assert results[1].value == 2


async def test_extra_order_by(async_db):
    await TestModel.async_object.acreate(name="Test1", value=3)
    await TestModel.async_object.acreate(name="Test2", value=1)
    await TestModel.async_object.acreate(name="Test3", value=2)

    results = [obj async for obj in TestModel.async_object.extra(order_by=["-value"])]

    assert len(results) == 3
    assert results[0].value == 3
    assert results[1].value == 2
    assert results[2].value == 1


async def test_extra_with_slice_error(async_db):
    with pytest.raises(TypeError):
        [obj async for obj in TestModel.async_object[:1].extra(select={"value_plus_one": "value + 1"})]
