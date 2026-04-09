from test_app.models import TestModel


async def test_acount(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")

    assert await TestModel.async_object.acount() == 2


async def test_acount_no_objects(async_db):
    assert await TestModel.async_object.acount() == 0
