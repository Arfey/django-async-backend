import pytest
from test_app.models import TestModel


async def test_adelete_all(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    num_deleted, _deleted_per_model = await TestModel.async_object.all().adelete()
    assert num_deleted == 3
    assert await TestModel.async_object.acount() == 0


async def test_adelete_filtered(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    num_deleted, _ = await TestModel.async_object.filter(name="Item1").adelete()
    assert num_deleted == 1
    assert await TestModel.async_object.acount() == 2


async def test_adelete_no_match(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    num_deleted, _ = await TestModel.async_object.filter(name="NonExistent").adelete()
    assert num_deleted == 0
    assert await TestModel.async_object.acount() == 3


async def test_adelete_sliced_raises(async_db):
    with pytest.raises(TypeError):
        await TestModel.async_object.all()[:1].adelete()


async def test_adelete_returns_model_label(async_db):
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)

    _, deleted_per_model = await TestModel.async_object.all().adelete()
    assert "test_app.TestModel" in deleted_per_model


async def test_adelete_cascade_fk(async_db):
    """Deleting a parent should cascade-delete its children."""
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)
    parent = await TestModel.async_object.acreate(name="Parent", value=1)
    await TestModel.async_object.acreate(name="Child1", value=2, relative=parent)
    await TestModel.async_object.acreate(name="Child2", value=3, relative=parent)
    assert await TestModel.async_object.acount() == 6  # 3 from setUp + 3

    # Delete the parent — children should be cascade-deleted
    num_deleted, _ = await TestModel.async_object.filter(pk=parent.pk).adelete()
    assert num_deleted == 3  # parent + 2 children
    assert await TestModel.async_object.acount() == 3  # only setUp items


async def test_adelete_instance_cascade(async_db):
    """Model.adelete() should cascade too."""
    await TestModel.async_object.acreate(name="Item1", value=10)
    await TestModel.async_object.acreate(name="Item2", value=20)
    await TestModel.async_object.acreate(name="Item3", value=30)
    parent = await TestModel.async_object.acreate(name="Parent2", value=1)
    await TestModel.async_object.acreate(name="Kid1", value=2, relative=parent)
    await TestModel.async_object.acreate(name="Kid2", value=3, relative=parent)

    count, _ = await parent.adelete()
    assert count == 3
    assert parent.pk is None
