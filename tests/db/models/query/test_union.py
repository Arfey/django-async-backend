from test_app.models import TestModel


async def test_union_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=2)
    queryset2 = TestModel.async_object.filter(value__gte=2)
    union_queryset = queryset1.union(queryset2)

    results = [item async for item in union_queryset]
    assert len(results) == 3
    assert any(item.name == "Item1" for item in results)
    assert any(item.name == "Item2" for item in results)
    assert any(item.name == "Item3" for item in results)


async def test_union_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=0)
    queryset2 = TestModel.async_object.filter(value__gte=4)
    union_queryset = queryset1.union(queryset2)

    results = [item async for item in union_queryset]
    assert len(results) == 0


async def test_union_with_empty_queryset(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=1)
    queryset2 = TestModel.async_object.none()
    union_queryset = queryset1.union(queryset2)

    results = [item async for item in union_queryset]
    assert len(results) == 1
    assert results[0].name == "Item1"

    queryset1 = TestModel.async_object.none()
    queryset2 = TestModel.async_object.none()
    union_queryset = queryset1.union(queryset2)

    results = [item async for item in union_queryset]
    assert len(results) == 0


async def test_union_with_multiple_querysets(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=1)
    queryset2 = TestModel.async_object.filter(value=2)
    queryset3 = TestModel.async_object.filter(value=3)
    union_queryset = queryset1.union(queryset2, queryset3)

    results = [item async for item in union_queryset]
    assert len(results) == 3
    assert any(item.name == "Item1" for item in results)
    assert any(item.name == "Item2" for item in results)
    assert any(item.name == "Item3" for item in results)
