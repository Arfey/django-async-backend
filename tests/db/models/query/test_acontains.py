from test_app.models import TestModel


async def test_acontains_true(async_db):
    obj = await TestModel.async_object.acreate(name="Item1", value=1)
    result = await TestModel.async_object.all().acontains(obj)
    assert result


async def test_acontains_false(async_db):
    await TestModel.async_object.acreate(name="Item1", value=1)
    other = TestModel(pk=99999, name="Ghost")
    result = await TestModel.async_object.all().acontains(other)
    assert not result


async def test_acontains_filtered(async_db):
    obj = await TestModel.async_object.acreate(name="Item1", value=1)
    result = await TestModel.async_object.filter(value=1).acontains(obj)
    assert result

    result = await TestModel.async_object.filter(value=999).acontains(obj)
    assert not result
