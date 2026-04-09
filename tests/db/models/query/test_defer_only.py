import pytest
from test_app.models import TestModel


async def test_defer_field(async_db):
    await TestModel.async_object.acreate(name="Item1", value=42)

    results = [obj async for obj in TestModel.async_object.defer("value")]
    assert len(results) == 1
    assert results[0].name == "Item1"


async def test_defer_clear(async_db):
    await TestModel.async_object.acreate(name="Item1", value=42)

    results = [obj async for obj in TestModel.async_object.defer(None)]
    assert len(results) == 1


async def test_only_field(async_db):
    await TestModel.async_object.acreate(name="Item1", value=42)

    results = [obj async for obj in TestModel.async_object.only("name")]
    assert len(results) == 1
    assert results[0].name == "Item1"


async def test_only_after_values_raises(async_db):
    with pytest.raises(TypeError):
        TestModel.async_object.values("name").only("name")
