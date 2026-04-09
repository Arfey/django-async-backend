import pytest
from django.db import NotSupportedError
from test_app.models import TestModel


async def test_aget_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    item = await TestModel.async_object.aget(name="Item1")
    assert item is not None
    assert item.name == "Item1"


async def test_aget_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    with pytest.raises(TestModel.DoesNotExist):
        await TestModel.async_object.aget(name="NonExistent")


async def test_aget_multiple_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item4", value=1)
    await TestModel.async_object.acreate(name="Item5", value=1)

    with pytest.raises(TestModel.MultipleObjectsReturned):
        await TestModel.async_object.aget(value=1)


async def test_aget_with_filters(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    item = await TestModel.async_object.filter(name="Item1").aget()
    assert item is not None
    assert item.name == "Item1"


async def test_union_combinator(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")

    queryset1 = TestModel.async_object.filter(name="Item1")
    queryset2 = TestModel.async_object.filter(name="Item2")
    combined_queryset = queryset1.union(queryset2)

    with pytest.raises(NotSupportedError):
        await combined_queryset.aget(id=1)
