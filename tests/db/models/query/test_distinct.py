import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_distinct_single_field(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=1)

    results = [obj async for obj in TestModel.async_object.distinct("value")]

    assert len(results) == 2
    assert results[0].value == 1
    assert results[1].value == 2


async def test_distinct_multiple_fields(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=1)

    results = [obj async for obj in TestModel.async_object.distinct("value", "name").order_by("name")]

    assert len(results) == 3
    assert results[0].name == "Test1"
    assert results[1].name == "Test2"
    assert results[2].name == "Test3"


async def test_distinct_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.distinct("nonexistent_field")]


async def test_distinct_with_slice_error(async_db):
    with pytest.raises(TypeError):
        [obj async for obj in TestModel.async_object[:1].distinct("value")]
