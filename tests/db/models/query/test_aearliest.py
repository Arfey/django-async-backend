import pytest
from shared.models import GetLatestByModel, TestModel


async def test_aearliest_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    earliest_item = await TestModel.async_object.aearliest("name")
    assert earliest_item is not None
    assert earliest_item.name == "Item1"


async def test_aearliest_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    with pytest.raises(TestModel.DoesNotExist):
        await TestModel.async_object.filter(name="some").aearliest("name")


async def test_aearliest_with_custom_field(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")

    earliest_item = await TestModel.async_object.aearliest("name")
    assert earliest_item is not None
    assert earliest_item.name == "Item1"


async def test_aearliest_no_get_latest_by(async_db):
    with pytest.raises(ValueError):
        await TestModel.async_object.aearliest()


async def test_aearliest_get_latest_by(async_db):
    await GetLatestByModel.async_object.acreate(name="Item1")
    await GetLatestByModel.async_object.acreate(name="Item2")

    earliest_item = await GetLatestByModel.async_object.aearliest()
    assert earliest_item is not None
    assert earliest_item.name == "Item1"


async def test_aearliest_with_sliced_query(async_db):
    sliced_queryset = TestModel.async_object.all()[:1]
    with pytest.raises(TypeError):
        await sliced_queryset.aearliest("name")
