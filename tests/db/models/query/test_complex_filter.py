import pytest
from django.core.exceptions import FieldError
from shared.models import TestModel


async def test_complex_filter_by_range(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [obj async for obj in TestModel.async_object.complex_filter({"value__gte": 2, "value__lte": 3})]

    assert len(results) == 2
    assert results[0].value == 2
    assert results[1].value == 3


async def test_complex_filter_with_exclusion(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [obj async for obj in TestModel.async_object.complex_filter({"value__gte": 1}).exclude(name="Test1")]

    assert len(results) == 2
    assert results[0].name != "Test1"
    assert results[1].name != "Test1"


async def test_complex_filter_invalid_field(async_db):
    with pytest.raises(FieldError):
        [obj async for obj in TestModel.async_object.complex_filter({"nonexistent_field__gte": 1})]


async def test_complex_filter_multiple_conditions(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [
        obj async for obj in TestModel.async_object.complex_filter({"name__startswith": "Test", "value__in": [1, 3]})
    ]

    assert len(results) == 2
    assert results[0].name == "Test1"
    assert results[1].name == "Test3"
