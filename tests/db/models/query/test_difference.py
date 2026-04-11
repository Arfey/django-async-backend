from shared.models import TestModel


async def test_difference_with_results(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=2)
    queryset2 = TestModel.async_object.filter(value__gte=2)
    difference_queryset = queryset1.difference(queryset2)

    results = [item async for item in difference_queryset]
    assert len(results) == 1
    assert results[0].name == "Item1"


async def test_difference_no_results(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=1)
    queryset2 = TestModel.async_object.filter(value__gte=2)
    difference_queryset = queryset1.difference(queryset2)

    results = [item async for item in difference_queryset]
    assert len(results) == 1
    assert results[0].name == "Item1"


async def test_difference_with_empty_queryset(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=1)
    queryset2 = TestModel.async_object.none()
    difference_queryset = queryset1.difference(queryset2)

    results = [item async for item in difference_queryset]
    assert len(results) == 1
    assert results[0].name == "Item1"


async def test_difference_with_multiple_querysets(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    await TestModel.async_object.acreate(name="Item2", value=2)
    await TestModel.async_object.acreate(name="Item3", value=3)

    queryset1 = TestModel.async_object.filter(value__lte=3)
    queryset2 = TestModel.async_object.filter(value=2)
    queryset3 = TestModel.async_object.filter(value=3)
    difference_queryset = queryset1.difference(queryset2, queryset3)

    results = [item async for item in difference_queryset]
    assert len(results) == 1
    assert results[0].name == "Item1"
