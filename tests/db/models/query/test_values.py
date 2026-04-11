import pytest
from django.core.exceptions import FieldError
from django.db.models import Count
from shared.models import TestModel


async def test_values(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values("name", "value")]

    assert len(results) == 2
    assert results[0]["name"] == "Test1"
    assert results[0]["value"] == 1
    assert results[1]["name"] == "Test2"
    assert results[1]["value"] == 2


async def test_values_no_objects(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(id=10).values("name", "value")]
    assert len(results) == 0


async def test_values_with_filter(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(name="Test1").values("name", "value")]

    assert len(results) == 1
    assert results[0]["name"] == "Test1"
    assert results[0]["value"] == 1


async def test_values_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.values("nonexistent_field")]


async def test_values_annotate(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [
        obj
        async for obj in TestModel.async_object.annotate(count=Count("name")).order_by("name").values("name", "count")
    ]

    assert results == [{"name": "Test1", "count": 1}, {"name": "Test2", "count": 1}]


async def test_values_no_fields(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values()]

    assert len(results) == 2
    assert "id" in results[0]
    assert "name" in results[0]
    assert "value" in results[0]
    assert results[0]["name"] == "Test1"
    assert results[1]["name"] == "Test2"
