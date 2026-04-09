import pytest
from test_app.models import TestModel


async def test_aupdate_all(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    rows = await TestModel.async_object.aupdate(value=99)
    assert rows == 3

    results = [obj async for obj in TestModel.async_object.all()]
    for obj in results:
        assert obj.value == 99


async def test_aupdate_filtered(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    rows = await TestModel.async_object.filter(name="Item1").aupdate(value=42)
    assert rows == 1

    obj = await TestModel.async_object.aget(name="Item1")
    assert obj.value == 42

    # Others unchanged
    obj2 = await TestModel.async_object.aget(name="Item2")
    assert obj2.value == 20


async def test_aupdate_no_match(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    rows = await TestModel.async_object.filter(name="NonExistent").aupdate(value=0)
    assert rows == 0


async def test_aupdate_sliced_raises(async_db):
    with pytest.raises(TypeError):
        await TestModel.async_object.all()[:1].aupdate(value=0)
