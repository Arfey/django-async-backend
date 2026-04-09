import pytest
from django.db import DEFAULT_DB_ALIAS
from test_app.models import TestModel

from django_async_backend.db import async_connections
from django_async_backend.test import AsyncCaptureQueriesContext


async def test_get_single_item(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")
    await TestModel.async_object.acreate(name="Item4")

    qs = TestModel.async_object.order_by("name")

    async with AsyncCaptureQueriesContext(async_connections[DEFAULT_DB_ALIAS]) as ctx:
        item = await qs[1]
        assert item.name == "Item2"
        assert qs._result_cache is None

        item = await qs[1]
        assert item.name == "Item2"
        assert len(ctx.captured_queries) == 2
        assert qs._result_cache is None

    # populate cache
    [i async for i in qs]

    async with AsyncCaptureQueriesContext(async_connections[DEFAULT_DB_ALIAS]) as ctx:
        item = await qs[0]
        await qs[1]
        await qs[2]

        assert item.name == "Item1"
        assert len(ctx.captured_queries) == 0
        assert len(qs._result_cache) == await qs.acount()


async def test_get_slice(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")
    await TestModel.async_object.acreate(name="Item4")

    qs = TestModel.async_object.all()

    async with AsyncCaptureQueriesContext(async_connections[DEFAULT_DB_ALIAS]) as ctx:
        items = [i async for i in qs[1:3]]

        assert len(ctx.captured_queries) == 1
        assert len(items) == 2
        assert items[0].name == "Item2"
        assert items[1].name == "Item3"

    async with AsyncCaptureQueriesContext(async_connections[DEFAULT_DB_ALIAS]) as ctx:
        # populate cache
        [i async for i in qs]
        assert len(ctx.captured_queries) == 1

        items = [i async for i in qs[1:3]]
        assert len(ctx.captured_queries) == 1
        assert len(items) == 2
        assert items[0].name == "Item2"
        assert items[1].name == "Item3"


async def test_get_slice_with_step(async_db):
    await TestModel.async_object.acreate(name="Item1")
    await TestModel.async_object.acreate(name="Item2")
    await TestModel.async_object.acreate(name="Item3")
    await TestModel.async_object.acreate(name="Item4")

    qs = TestModel.async_object.all()
    async with AsyncCaptureQueriesContext(async_connections[DEFAULT_DB_ALIAS]) as ctx:
        items = [i async for i in qs[0:3:2]]
        assert len(ctx.captured_queries) == 1

    assert len(items) == 2
    assert items[0].name == "Item1"
    assert items[1].name == "Item3"

    # populate cache
    [i async for i in qs]

    async with AsyncCaptureQueriesContext(async_connections[DEFAULT_DB_ALIAS]) as ctx:
        items = [i async for i in qs[0:3:2]]
        assert len(ctx.captured_queries) == 0


async def test_invalid_index(async_db):
    with pytest.raises(TypeError):
        await TestModel.async_object.all()["invalid"]


async def test_negative_index(async_db):
    with pytest.raises(ValueError):
        await TestModel.async_object.all()[-1]


async def test_out_of_range(async_db):
    with pytest.raises(IndexError):
        await TestModel.async_object.all()[100]
