import pytest
from django.db.models import Count
from test_app.models import TestModel


async def test_alast_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    last_item = await TestModel.async_object.alast()
    assert last_item is not None
    assert last_item.name == "Item3"


async def test_alast_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    last_item = await TestModel.async_object.filter(name="Item4").alast()
    assert last_item is None


async def test_alast_with_ordering(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    last_item = await TestModel.async_object.order_by("name").alast()
    assert last_item is not None
    assert last_item.name == "Item3"


async def test_alast_check_ordering_error(async_db):
    with pytest.raises(TypeError):
        await TestModel.async_object.values("name").annotate(count=Count("name")).alast()
