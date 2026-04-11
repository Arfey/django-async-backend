import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_reverse_order(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [obj async for obj in TestModel.async_object.order_by("value").reverse()]

    assert len(results) == 3
    assert results[0].value == 3
    assert results[1].value == 2
    assert results[2].value == 1


async def test_reverse_with_filter(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [obj async for obj in TestModel.async_object.filter(value__gte=2).order_by("value").reverse()]

    assert len(results) == 2
    assert results[0].value == 3
    assert results[1].value == 2


async def test_reverse_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.order_by("nonexistent_field").reverse()]


async def test_reverse_with_slice_error(async_db):
    with pytest.raises(TypeError):
        [obj async for obj in TestModel.async_object[:1].reverse()]
