from shared.models import TestModel


async def test_and_operator(async_db):
    await TestModel.async_object.acreate(name="A", value=1)
    await TestModel.async_object.acreate(name="B", value=2)
    await TestModel.async_object.acreate(name="C", value=3)

    qs1 = TestModel.async_object.filter(value__gte=1)
    qs2 = TestModel.async_object.filter(value__lte=2)
    combined = qs1 & qs2
    results = [obj async for obj in combined]
    names = {obj.name for obj in results}
    assert names == {"A", "B"}


async def test_or_operator(async_db):
    await TestModel.async_object.acreate(name="A", value=1)
    await TestModel.async_object.acreate(name="B", value=2)
    await TestModel.async_object.acreate(name="C", value=3)

    qs1 = TestModel.async_object.filter(name="A")
    qs2 = TestModel.async_object.filter(name="C")
    combined = qs1 | qs2
    results = [obj async for obj in combined]
    names = {obj.name for obj in results}
    assert names == {"A", "C"}
