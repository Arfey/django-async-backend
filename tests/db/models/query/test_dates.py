import datetime

import pytest
from test_app.models import Event


@pytest.fixture
async def events(async_db):
    """Create test events with various dates."""
    await Event.async_object.abulk_create(
        [
            Event(
                title="E1",
                date=datetime.date(2024, 1, 15),
                timestamp=datetime.datetime(2024, 1, 15, 10, 30, 0),
            ),
            Event(
                title="E2",
                date=datetime.date(2024, 1, 20),
                timestamp=datetime.datetime(2024, 1, 20, 14, 0, 0),
            ),
            Event(
                title="E3",
                date=datetime.date(2024, 3, 5),
                timestamp=datetime.datetime(2024, 3, 5, 8, 15, 0),
            ),
            Event(
                title="E4",
                date=datetime.date(2025, 6, 1),
                timestamp=datetime.datetime(2025, 6, 1, 12, 0, 0),
            ),
        ]
    )


async def test_dates_year(events):
    """dates() with kind='year'."""
    result = [d async for d in Event.async_object.dates("date", "year")]
    assert len(result) == 2
    assert result[0] == datetime.date(2024, 1, 1)
    assert result[1] == datetime.date(2025, 1, 1)


async def test_dates_month(events):
    """dates() with kind='month'."""
    result = [d async for d in Event.async_object.dates("date", "month")]
    assert len(result) == 3
    assert result[0] == datetime.date(2024, 1, 1)
    assert result[1] == datetime.date(2024, 3, 1)
    assert result[2] == datetime.date(2025, 6, 1)


async def test_dates_day(events):
    """dates() with kind='day'."""
    result = [d async for d in Event.async_object.dates("date", "day")]
    assert len(result) == 4


async def test_dates_descending(events):
    """dates() with order='DESC'."""
    result = [d async for d in Event.async_object.dates("date", "year", order="DESC")]
    assert result[0] == datetime.date(2025, 1, 1)
    assert result[1] == datetime.date(2024, 1, 1)


async def test_dates_invalid_kind(async_db):
    """dates() with invalid kind raises ValueError."""
    with pytest.raises(ValueError, match="'kind' must be"):
        [d async for d in Event.async_object.dates("date", "invalid")]


async def test_dates_invalid_order(async_db):
    """dates() with invalid order raises ValueError."""
    with pytest.raises(ValueError, match="'order' must be"):
        [d async for d in Event.async_object.dates("date", "year", order="RANDOM")]


async def test_dates_empty(async_db):
    """dates() on empty queryset returns empty."""
    result = [d async for d in Event.async_object.dates("date", "year")]
    assert result == []


async def test_datetimes_year(events):
    """datetimes() with kind='year'."""
    result = [d async for d in Event.async_object.datetimes("timestamp", "year")]
    assert len(result) == 2


async def test_datetimes_month(events):
    """datetimes() with kind='month'."""
    result = [d async for d in Event.async_object.datetimes("timestamp", "month")]
    assert len(result) == 3


async def test_datetimes_day(events):
    """datetimes() with kind='day'."""
    result = [d async for d in Event.async_object.datetimes("timestamp", "day")]
    assert len(result) == 4


async def test_datetimes_hour(events):
    """datetimes() with kind='hour'."""
    result = [d async for d in Event.async_object.datetimes("timestamp", "hour")]
    assert len(result) == 4


async def test_datetimes_descending(events):
    """datetimes() with order='DESC'."""
    result = [d async for d in Event.async_object.datetimes("timestamp", "year", order="DESC")]
    assert len(result) == 2
    assert result[0].year == 2025
    assert result[1].year == 2024


async def test_datetimes_invalid_kind(async_db):
    """datetimes() with invalid kind raises ValueError."""
    with pytest.raises(ValueError, match="'kind' must be"):
        [d async for d in Event.async_object.datetimes("timestamp", "invalid")]


async def test_dates_with_filter(events):
    """dates() combined with filter."""
    result = [d async for d in Event.async_object.filter(date__year=2024).dates("date", "month")]
    assert len(result) == 2
    assert result[0] == datetime.date(2024, 1, 1)
    assert result[1] == datetime.date(2024, 3, 1)
