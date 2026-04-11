from django.db.models import Count
from shared.models import TestModel


async def test_annotate_count(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [
        obj async for obj in TestModel.async_object.values("value").annotate(count=Count("value")).order_by("value")
    ]

    assert len(results) == 3
    assert results[0]["value"] == 1
    assert results[0]["count"] == 1
    assert results[1]["value"] == 2
    assert results[1]["count"] == 1
    assert results[2]["value"] == 3
    assert results[2]["count"] == 1


async def test_annotate_with_filter(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [
        obj async for obj in TestModel.async_object.filter(value=1).values("value").annotate(count=Count("value"))
    ]

    assert len(results) == 1
    assert results[0]["value"] == 1
    assert results[0]["count"] == 1


async def test_annotate_no_results(async_db):
    await TestModel.async_object.acreate(name="Test1", value=1)
    await TestModel.async_object.acreate(name="Test2", value=2)
    await TestModel.async_object.acreate(name="Test3", value=3)

    results = [
        obj async for obj in TestModel.async_object.filter(value=99).values("value").annotate(count=Count("value"))
    ]

    assert len(results) == 0
