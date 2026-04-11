from shared.models import TestModel


async def test_creates_when_not_exists(async_db):
    obj, created = await TestModel.async_object.aupdate_or_create(name="New", defaults={"value": 42})
    assert created
    assert obj.name == "New"
    assert obj.value == 42


async def test_updates_when_exists(async_db):
    await TestModel.async_object.acreate(name="Exists", value=1)
    obj, created = await TestModel.async_object.aupdate_or_create(name="Exists", defaults={"value": 99})
    assert not created
    assert obj.value == 99

    # Verify persisted
    fetched = await TestModel.async_object.aget(name="Exists")
    assert fetched.value == 99


async def test_create_defaults(async_db):
    obj, created = await TestModel.async_object.aupdate_or_create(
        name="New",
        defaults={"value": 10},
        create_defaults={"value": 50},
    )
    assert created
    assert obj.value == 50
