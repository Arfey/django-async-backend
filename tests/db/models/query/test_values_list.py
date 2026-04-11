import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_values_list(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values_list("name", "value")]

    assert len(results) == 2
    assert results[0] == ("Test1", 1)
    assert results[1] == ("Test2", 2)


async def test_values_list_no_objects(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(id=10).values_list("name", "value")]
    assert len(results) == 0


async def test_values_list_with_filter(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.filter(name="Test1").values_list("name", "value")]

    assert len(results) == 1
    assert results[0] == ("Test1", 1)


async def test_values_list_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.values_list("nonexistent_field")]


async def test_values_list_flat(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values_list("name", flat=True)]

    assert len(results) == 2
    assert results[0] == "Test1"
    assert results[1] == "Test2"


async def test_values_list_flat_all_fields(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values_list(flat=True)]

    assert len(results) == 2


async def test_values_list_flat_multiple_fields(async_db):
    with pytest.raises(TypeError):
        [obj async for obj in TestModel.async_object.values_list("name", "value", flat=True)]


async def test_values_list_named(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values_list("name", "value", named=True)]

    assert len(results) == 2
    assert results[0].name == "Test1"
    assert results[0].value == 1
    assert results[1].name == "Test2"
    assert results[1].value == 2


async def test_values_list_named_all_fields(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = [obj async for obj in TestModel.async_object.values_list(named=True)]

    assert len(results) == 2
    assert results[0].name == "Test1"
    assert results[0].value == 1
    assert results[1].name == "Test2"
    assert results[1].value == 2


async def test_values_list_flat_and_named(async_db):
    with pytest.raises(TypeError):
        [obj async for obj in TestModel.async_object.values_list("name", "value", flat=True, named=True)]
