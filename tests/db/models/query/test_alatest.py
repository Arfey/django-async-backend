import pytest
from shared.models import GetLatestByModel, TestModel


async def test_alatest_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    latest_item = await TestModel.async_object.alatest("name")
    assert latest_item is not None
    assert latest_item.name == "Item3"


async def test_alatest_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    with pytest.raises(TestModel.DoesNotExist):
        await TestModel.async_object.filter(name="some").alatest("name")


async def test_alatest_with_custom_field(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    latest_item = await TestModel.async_object.alatest("name")
    assert latest_item is not None
    assert latest_item.name == "Item3"


async def test_alatest_no_get_latest_by(async_db):
    with pytest.raises(ValueError):
        await TestModel.async_object.alatest()


async def test_alatest_get_latest_by(async_db):
    await GetLatestByModel.async_object.acreate(name="Item1")
    await GetLatestByModel.async_object.acreate(name="Item2")

    latest_item = await GetLatestByModel.async_object.alatest()
    assert latest_item is not None
    assert latest_item.name == "Item2"


async def test_alatest_with_sliced_query(async_db):
    sliced_queryset = TestModel.async_object.all()[:1]
    with pytest.raises(TypeError):
        await sliced_queryset.alatest("name")
