from test_app.models import TestModel


async def test_acreate_basic(async_db):
    obj = await TestModel.async_object.acreate(name="NewItem")
    assert obj.pk is not None
    assert obj.name == "NewItem"
    assert obj.value is None


async def test_acreate_with_all_fields(async_db):
    obj = await TestModel.async_object.acreate(name="Full", value=42)
    assert obj.pk is not None
    assert obj.name == "Full"
    assert obj.value == 42


async def test_acreate_with_fk(async_db):
    parent = await TestModel.async_object.acreate(name="Parent", value=1)
    child = await TestModel.async_object.acreate(name="Child", value=2, relative=parent)
    assert child.pk is not None
    assert child.relative_id == parent.pk


async def test_acreate_auto_pk_assigned(async_db):
    obj1 = await TestModel.async_object.acreate(name="First")
    obj2 = await TestModel.async_object.acreate(name="Second")
    assert obj1.pk is not None
    assert obj2.pk is not None
    assert obj1.pk != obj2.pk


async def test_acreate_persists_to_db(async_db):
    obj = await TestModel.async_object.acreate(name="Persisted", value=7)
    fetched = await TestModel.async_object.aget(pk=obj.pk)
    assert fetched.name == "Persisted"
    assert fetched.value == 7
