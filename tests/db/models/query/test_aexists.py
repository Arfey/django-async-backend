from shared.models import TestModel


async def test_has_results(async_db):
    await TestModel.async_object.acreate(name="Test1")

    assert await TestModel.async_object.aexists()


async def test_has_results_no_objects(async_db):
    assert not await TestModel.async_object.aexists()


async def test_has_results_from_cache(async_db):
    await TestModel.async_object.acreate(name="Test1")

    results = [i async for i in TestModel.async_object.all()]
    assert len(results) == 1

    assert await TestModel.async_object.aexists()
