from test_app.models import TestModel


async def test_abulk_create_basic(async_db):
    objs = [
        TestModel(name="Bulk1", value=1),
        TestModel(name="Bulk2", value=2),
        TestModel(name="Bulk3", value=3),
    ]
    result = await TestModel.async_object.abulk_create(objs)
    assert len(result) == 3
    for obj in result:
        assert obj.pk is not None
    assert await TestModel.async_object.acount() == 3


async def test_abulk_create_empty(async_db):
    result = await TestModel.async_object.abulk_create([])
    assert result == []
    assert await TestModel.async_object.acount() == 0


async def test_abulk_create_batch_size(async_db):
    objs = [TestModel(name=f"Batch{i}", value=i) for i in range(5)]
    result = await TestModel.async_object.abulk_create(objs, batch_size=2)
    assert len(result) == 5
    assert await TestModel.async_object.acount() == 5


async def test_abulk_create_pks_are_unique(async_db):
    objs = [
        TestModel(name="A", value=1),
        TestModel(name="B", value=2),
    ]
    result = await TestModel.async_object.abulk_create(objs)
    pks = [obj.pk for obj in result]
    assert len(set(pks)) == 2


async def test_abulk_create_persists_to_db(async_db):
    objs = [TestModel(name="Persist1", value=10)]
    await TestModel.async_object.abulk_create(objs)
    fetched = await TestModel.async_object.aget(name="Persist1")
    assert fetched.value == 10
