import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_exclude_by_name(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.exclude(name="Test1")]

    assert len(results) == 1
    assert results[0].name == "Test2"


async def test_exclude_no_results(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.exclude(name="Nonexistent")]

    assert len(results) == 2


async def test_exclude_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.exclude(nonexistent_field="value")]


async def test_exclude_multiple_conditions(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.exclude(name="Test1", value=1)]

    assert len(results) == 1
    assert results[0].name == "Test2"
