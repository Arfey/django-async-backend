import pytest
from shared.models import TestModel


async def test_asave_insert(async_db):
    obj = TestModel(name="SaveTest", value=42)
    await obj.asave()
    assert obj.pk is not None
    assert not obj._state.adding

    fetched = await TestModel.async_object.aget(pk=obj.pk)
    assert fetched.name == "SaveTest"
    assert fetched.value == 42


async def test_asave_update(async_db):
    obj = TestModel(name="UpdateTest", value=1)
    await obj.asave()
    original_pk = obj.pk

    obj.value = 99
    await obj.asave()
    assert obj.pk == original_pk

    fetched = await TestModel.async_object.aget(pk=obj.pk)
    assert fetched.value == 99


async def test_asave_force_insert(async_db):
    obj = TestModel(name="ForceInsert", value=10)
    await obj.asave(force_insert=True)
    assert obj.pk is not None

    fetched = await TestModel.async_object.aget(name="ForceInsert")
    assert fetched.value == 10


async def test_asave_update_fields(async_db):
    obj = TestModel(name="Partial", value=1)
    await obj.asave()

    obj.name = "Changed"
    obj.value = 999
    await obj.asave(update_fields=["value"])

    fetched = await TestModel.async_object.aget(pk=obj.pk)
    assert fetched.name == "Partial"  # unchanged
    assert fetched.value == 999  # updated


async def test_asave_with_fk(async_db):
    parent = TestModel(name="Parent")
    await parent.asave()

    child = TestModel(name="Child", relative=parent)
    await child.asave()
    assert child.pk is not None
    assert child.relative_id == parent.pk


async def test_adelete_instance(async_db):
    obj = await TestModel.async_object.acreate(name="ToDelete", value=1)
    pk = obj.pk
    count, _per_model = await obj.adelete()
    assert count == 1
    assert obj.pk is None
    assert not await TestModel.async_object.filter(pk=pk).aexists()


async def test_adelete_no_pk_raises(async_db):
    obj = TestModel(name="NoPK")
    with pytest.raises(ValueError):
        await obj.adelete()


async def test_arefresh_from_db(async_db):
    obj = await TestModel.async_object.acreate(name="Refresh", value=1)
    await TestModel.async_object.filter(pk=obj.pk).aupdate(value=99)
    assert obj.value == 1
    await obj.arefresh_from_db()
    assert obj.value == 99


async def test_arefresh_from_db_fields(async_db):
    obj = await TestModel.async_object.acreate(name="Partial", value=10)
    await TestModel.async_object.filter(pk=obj.pk).aupdate(value=50, name="Changed")
    await obj.arefresh_from_db(fields=["value"])
    assert obj.value == 50
    assert obj.name == "Partial"  # not refreshed
