from shared.models import TestModel


async def test_aiter(async_db):
    await TestModel.async_object.acreate(name="Test1")
    await TestModel.async_object.acreate(name="Test2")

    results = [obj async for obj in TestModel.async_object.all()]

    assert len(results) == 2
    assert results[0].name == "Test1"
    assert results[1].name == "Test2"


async def test_aiter_no_objects(async_db):
    results = [obj async for obj in TestModel.async_object.all()]
    assert len(results) == 0


async def test_aiter_with_filter(async_db):
    await TestModel.async_object.acreate(name="Test1")

    results = [obj async for obj in TestModel.async_object.filter(name="Test1")]

    assert len(results) == 1
    assert results[0].name == "Test1"
