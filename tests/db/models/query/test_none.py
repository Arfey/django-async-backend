from shared.models import TestModel


async def test_success(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    assert await TestModel.async_object.none().acount() == 0
