from shared.models import TestModel


async def test_aiterator_basic(async_db):
    for i in range(5):
        await TestModel.async_object.acreate(name=f"Item{i}", value=i)

    results = []
    async for obj in TestModel.async_object.all().aiterator():
        results.append(obj)
    assert len(results) == 5


async def test_aiterator_chunk_size(async_db):
    for i in range(5):
        await TestModel.async_object.acreate(name=f"Item{i}", value=i)

    results = []
    async for obj in TestModel.async_object.all().aiterator(chunk_size=2):
        results.append(obj)
    assert len(results) == 5


async def test_aiterator_filtered(async_db):
    for i in range(5):
        await TestModel.async_object.acreate(name=f"Item{i}", value=i)

    results = []
    async for obj in TestModel.async_object.filter(value__gte=3).aiterator():
        results.append(obj)
    assert len(results) == 2


async def test_aiterator_no_cache(async_db):
    for i in range(5):
        await TestModel.async_object.acreate(name=f"Item{i}", value=i)

    qs = TestModel.async_object.all()
    async for _ in qs.aiterator():
        pass
    # aiterator should NOT populate _result_cache
    assert qs._result_cache is None
