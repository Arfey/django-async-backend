import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_order_by_single_field(async_db):
    await TestModel.async_object.acreate(name="Test1", value=3)
    await TestModel.async_object.acreate(name="Test2", value=1)
    await TestModel.async_object.acreate(name="Test3", value=2)

    results = [obj async for obj in TestModel.async_object.order_by("value")]

    assert len(results) == 3
    assert results[0].value == 1
    assert results[1].value == 2
    assert results[2].value == 3


async def test_order_by_descending(async_db):
    await TestModel.async_object.acreate(name="Test1", value=3)
    await TestModel.async_object.acreate(name="Test2", value=1)
    await TestModel.async_object.acreate(name="Test3", value=2)

    results = [obj async for obj in TestModel.async_object.order_by("-value")]

    assert len(results) == 3
    assert results[0].value == 3
    assert results[1].value == 2
    assert results[2].value == 1


async def test_order_by_multiple_fields(async_db):
    await TestModel.async_object.acreate(name="Test1", value=3)
    await TestModel.async_object.acreate(name="Test2", value=1)
    await TestModel.async_object.acreate(name="Test3", value=2)

    results = [obj async for obj in TestModel.async_object.order_by("value", "name")]

    assert len(results) == 3
    assert results[0].value == 1
    assert results[1].value == 2
    assert results[2].value == 3


async def test_order_by_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.order_by("nonexistent_field")]


async def test_order_by_is_sliced_error(async_db):
    with pytest.raises(TypeError):
        [obj async for obj in TestModel.async_object[:1].order_by("value")]
