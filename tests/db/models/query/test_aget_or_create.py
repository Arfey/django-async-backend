from test_app.models import TestModel


async def test_creates_when_not_exists(async_db):
    obj, created = await TestModel.async_object.aget_or_create(name="New", defaults={"value": 42})
    assert created
    assert obj.name == "New"
    assert obj.value == 42
    assert obj.pk is not None


async def test_gets_when_exists(async_db):
    existing = await TestModel.async_object.acreate(name="Exists", value=1)
    obj, created = await TestModel.async_object.aget_or_create(name="Exists", defaults={"value": 99})
    assert not created
    assert obj.pk == existing.pk
    assert obj.value == 1  # original value, not default


async def test_no_defaults(async_db):
    obj, created = await TestModel.async_object.aget_or_create(name="NoDefaults")
    assert created
    assert obj.name == "NoDefaults"
