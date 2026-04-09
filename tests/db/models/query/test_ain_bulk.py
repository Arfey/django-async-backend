import pytest
from django.core.exceptions import FieldDoesNotExist
from test_app.models import TestModel


async def test_ain_bulk_with_ids(async_db):
    obj1 = await TestModel.async_object.acreate(name="Test1", value=1)
    obj2 = await TestModel.async_object.acreate(name="Test2", value=2)

    results = await TestModel.async_object.ain_bulk([obj1.id, obj2.id])

    assert len(results) == 2
    assert obj1.id in results
    assert obj2.id in results
    assert results[obj1.id].name == "Test1"
    assert results[obj2.id].name == "Test2"


async def test_ain_bulk_no_ids(async_db):
    results = await TestModel.async_object.ain_bulk([])
    assert len(results) == 0


async def test_ain_bulk_with_field_name(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)

    results = await TestModel.async_object.ain_bulk(["Test1", "Test2"], field_name="name")

    assert len(results) == 2
    assert "Test1" in results
    assert "Test2" in results
    assert results["Test1"].value == 1
    assert results["Test2"].value == 2


async def test_ain_bulk_nonexistent_field(async_db):
    with pytest.raises(FieldDoesNotExist):
        await TestModel.async_object.ain_bulk(["Test1"], field_name="nonexistent_field")


async def test_ain_bulk_invalid_field(async_db):
    with pytest.raises(ValueError):
        await TestModel.async_object.ain_bulk(["Test1"], field_name="value")


async def test_ain_bulk_with_slicing(async_db):
    obj1 = await TestModel.async_object.acreate(name="Test1", value=1)
    sliced_queryset = TestModel.async_object.all()[:1]
    with pytest.raises(TypeError):
        await sliced_queryset.ain_bulk([obj1.id])


async def test_ain_bulk_with_non_model_iterable(async_db):
    obj1 = await TestModel.async_object.acreate(name="Test1", value=1)
    queryset = TestModel.async_object.values("name")
    with pytest.raises(TypeError):
        await queryset.ain_bulk([obj1.id])
