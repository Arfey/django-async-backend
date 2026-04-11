"""Raw query tests. Uses plain async functions without async_db fixture since raw queries
bypass transaction wrapping. Manually cleans up test data."""

import pytest
from shared.models import TestModel


@pytest.fixture
async def raw_db():
    """Set up and tear down test data for raw query tests without transaction wrapping."""
    await TestModel.async_object.all().adelete()
    await TestModel.async_object.acreate(name="Raw1", value=10)
    await TestModel.async_object.acreate(name="Raw2", value=20)
    await TestModel.async_object.acreate(name="Raw3", value=30)

    yield

    await TestModel.async_object.all().adelete()


async def test_araw_basic(raw_db):
    qs = TestModel.async_object.araw("SELECT * FROM test_model")
    results = [obj async for obj in qs]
    assert len(results) == 3
    assert isinstance(results[0], TestModel)


async def test_araw_with_params(raw_db):
    qs = TestModel.async_object.araw("SELECT * FROM test_model WHERE value > %s", [15])
    results = [obj async for obj in qs]
    assert len(results) == 2
    names = {obj.name for obj in results}
    assert names == {"Raw2", "Raw3"}


async def test_araw_fields(raw_db):
    qs = TestModel.async_object.araw("SELECT * FROM test_model WHERE name = %s", ["Raw1"])
    results = [obj async for obj in qs]
    assert len(results) == 1
    assert results[0].name == "Raw1"
    assert results[0].value == 10


async def test_araw_empty_result(raw_db):
    qs = TestModel.async_object.araw("SELECT * FROM test_model WHERE value > %s", [999])
    results = [obj async for obj in qs]
    assert len(results) == 0


async def test_araw_ordering(raw_db):
    qs = TestModel.async_object.araw("SELECT * FROM test_model ORDER BY value DESC")
    results = [obj async for obj in qs]
    assert results[0].name == "Raw3"
    assert results[-1].name == "Raw1"
