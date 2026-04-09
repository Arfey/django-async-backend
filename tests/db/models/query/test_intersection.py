from test_app.models import TestModel


async def test_intersection_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=2)
    queryset2 = TestModel.async_object.filter(value__gte=1)
    intersection_queryset = queryset1.intersection(queryset2)

    results = [item async for item in intersection_queryset]
    assert len(results) == 2
    assert any(item.name == "Item1" for item in results)
    assert any(item.name == "Item2" for item in results)


async def test_intersection_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=1)
    queryset2 = TestModel.async_object.filter(value__gte=3)
    intersection_queryset = queryset1.intersection(queryset2)

    results = [item async for item in intersection_queryset]
    assert len(results) == 0


async def test_intersection_with_empty_queryset(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=1)
    queryset2 = TestModel.async_object.none()
    intersection_queryset = queryset1.intersection(queryset2)

    results = [item async for item in intersection_queryset]
    assert len(results) == 0


async def test_intersection_with_multiple_querysets(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=3)
    queryset2 = TestModel.async_object.filter(value__gte=2)
    queryset3 = TestModel.async_object.filter(value=3)
    intersection_queryset = queryset1.intersection(queryset2, queryset3)

    results = [item async for item in intersection_queryset]
    assert len(results) == 1
    assert results[0].name == "Item3"
