import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_filter_by_name(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(name="Test1")]

    assert len(results) == 1
    assert results[0].name == "Test1"


async def test_filter_no_results(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(name="Nonexistent")]

    assert len(results) == 0


async def test_filter_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.filter(nonexistent_field="value")]


async def test_filter_multiple_conditions(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(name="Test1", value=1)]

    assert len(results) == 1
    assert results[0].name == "Test1"
    assert results[0].value == 1
