import pytest
from django.utils.connection import ConnectionDoesNotExist
from shared.models import TestModel


async def test_using_default_database(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.using("default")]

    assert len(results) == 2
    assert results[0].name == "Test1"
    assert results[1].name == "Test2"


async def test_using_invalid_database(async_db):
    with pytest.raises(ConnectionDoesNotExist):
        [obj async for obj in TestModel.async_object.using("invalid_db")]


async def test_using_with_filter(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.using("default").filter(name="Test1")]

    assert len(results) == 1
    assert results[0].name == "Test1"
