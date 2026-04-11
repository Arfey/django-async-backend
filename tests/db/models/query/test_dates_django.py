"""
Port of Django's dates tests to our async backend.

Uses the Event model (date: DateField, timestamp: DateTimeField) and the
async iteration pattern `[obj async for obj in queryset]`.
"""

import datetime

import pytest
from django.core.exceptions import FieldError
from test_app.models import Event

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def events(async_db):
    """Create a set of Events spread across multiple years/months/days."""
    await Event.async_object.abulk_create(
        [
            # Two events in January 2020
            Event(
                title="E1",
                date=datetime.date(2020, 1, 5),
                timestamp=datetime.datetime(2020, 1, 5, 8, 0, 0),
            ),
            Event(
                title="E2",
                date=datetime.date(2020, 1, 28),
                timestamp=datetime.datetime(2020, 1, 28, 14, 30, 0),
            ),
            # One event in March 2020
            Event(
                title="E3",
                date=datetime.date(2020, 3, 15),
                timestamp=datetime.datetime(2020, 3, 15, 9, 45, 0),
            ),
            # One event in a different year
            Event(
                title="E4",
                date=datetime.date(2021, 7, 4),
                timestamp=datetime.datetime(2021, 7, 4, 18, 0, 0),
            ),
        ]
    )


# ---------------------------------------------------------------------------
# dates() — kind='year'
# ---------------------------------------------------------------------------


async def test_dates_year(events):
    """dates() with kind='year' returns one entry per distinct year."""
    result = [d async for d in Event.async_object.dates("date", "year")]
    assert result == [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)]


async def test_dates_year_desc(events):
    """dates() with kind='year', order='DESC' returns years in descending order."""
    result = [d async for d in Event.async_object.dates("date", "year", order="DESC")]
    assert result == [datetime.date(2021, 1, 1), datetime.date(2020, 1, 1)]


# ---------------------------------------------------------------------------
# dates() — kind='month'
# ---------------------------------------------------------------------------


async def test_dates_month(events):
    """dates() with kind='month' returns one entry per distinct (year, month)."""
    result = [d async for d in Event.async_object.dates("date", "month")]
    assert result == [
        datetime.date(2020, 1, 1),
        datetime.date(2020, 3, 1),
        datetime.date(2021, 7, 1),
    ]


async def test_dates_month_desc(events):
    """dates() with kind='month', order='DESC' returns months descending."""
    result = [d async for d in Event.async_object.dates("date", "month", order="DESC")]
    assert result == [
        datetime.date(2021, 7, 1),
        datetime.date(2020, 3, 1),
        datetime.date(2020, 1, 1),
    ]


# ---------------------------------------------------------------------------
# dates() — kind='week'
# ---------------------------------------------------------------------------


async def test_dates_week(events):
    """dates() with kind='week' returns one entry per distinct ISO week start."""
    result = [d async for d in Event.async_object.dates("date", "week")]
    # Jan 5 2020 is a Sunday; ISO week starts Monday Dec 30 2019
    # Jan 28 2020 — week starts Mon Jan 27
    # Mar 15 2020 — week starts Mon Mar 9
    # Jul 4 2021  — week starts Mon Jun 28
    assert len(result) == 4
    # Results are date objects
    for d in result:
        assert isinstance(d, datetime.date)


# ---------------------------------------------------------------------------
# dates() — kind='day'
# ---------------------------------------------------------------------------


async def test_dates_day(events):
    """dates() with kind='day' returns one entry per distinct date."""
    result = [d async for d in Event.async_object.dates("date", "day")]
    assert result == [
        datetime.date(2020, 1, 5),
        datetime.date(2020, 1, 28),
        datetime.date(2020, 3, 15),
        datetime.date(2021, 7, 4),
    ]


async def test_dates_day_desc(events):
    """dates() with kind='day', order='DESC' returns days in descending order."""
    result = [d async for d in Event.async_object.dates("date", "day", order="DESC")]
    assert result == [
        datetime.date(2021, 7, 4),
        datetime.date(2020, 3, 15),
        datetime.date(2020, 1, 28),
        datetime.date(2020, 1, 5),
    ]


# ---------------------------------------------------------------------------
# dates() — empty queryset
# ---------------------------------------------------------------------------


async def test_dates_empty_queryset(async_db):
    """dates() on an empty table returns an empty list."""
    result = [d async for d in Event.async_object.dates("date", "year")]
    assert result == []


# ---------------------------------------------------------------------------
# dates() — with filter
# ---------------------------------------------------------------------------


async def test_dates_with_filter_year(events):
    """dates() combined with filter restricts to matching rows."""
    result = [d async for d in Event.async_object.filter(date__year=2020).dates("date", "month")]
    assert result == [datetime.date(2020, 1, 1), datetime.date(2020, 3, 1)]


async def test_dates_with_filter_month(events):
    """dates() filtered by month returns only days in that month."""
    result = [d async for d in Event.async_object.filter(date__month=1).dates("date", "day")]
    assert result == [datetime.date(2020, 1, 5), datetime.date(2020, 1, 28)]


# ---------------------------------------------------------------------------
# dates() — invalid arguments
# ---------------------------------------------------------------------------


async def test_dates_invalid_kind(async_db):
    """dates() with an unrecognised kind raises ValueError."""
    with pytest.raises(ValueError, match="'kind' must be one of"):
        [d async for d in Event.async_object.dates("date", "bad_kind")]


async def test_dates_invalid_order(async_db):
    """dates() with an unrecognised order raises ValueError."""
    with pytest.raises(ValueError, match="'order' must be either"):
        [d async for d in Event.async_object.dates("date", "year", order="bad order")]


async def test_dates_invalid_field(async_db):
    """dates() on a non-existent field raises FieldError."""
    with pytest.raises(FieldError):
        [d async for d in Event.async_object.dates("invalid_field", "year")]


# ---------------------------------------------------------------------------
# datetimes() — kind='year'
# ---------------------------------------------------------------------------


async def test_datetimes_year(events):
    """datetimes() with kind='year' returns one datetime per distinct year."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "year")]
    assert len(result) == 2
    assert result[0].year == 2020
    assert result[1].year == 2021


async def test_datetimes_year_desc(events):
    """datetimes() with kind='year', order='DESC' returns descending."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "year", order="DESC")]
    assert result[0].year == 2021
    assert result[1].year == 2020


# ---------------------------------------------------------------------------
# datetimes() — kind='month'
# ---------------------------------------------------------------------------


async def test_datetimes_month(events):
    """datetimes() with kind='month' returns one entry per (year, month)."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "month")]
    assert len(result) == 3
    assert (result[0].year, result[0].month) == (2020, 1)
    assert (result[1].year, result[1].month) == (2020, 3)
    assert (result[2].year, result[2].month) == (2021, 7)


# ---------------------------------------------------------------------------
# datetimes() — kind='week'
# ---------------------------------------------------------------------------


async def test_datetimes_week(events):
    """datetimes() with kind='week' returns one entry per ISO week."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "week")]
    assert len(result) == 4
    for dt in result:
        assert isinstance(dt, datetime.datetime)


# ---------------------------------------------------------------------------
# datetimes() — kind='day'
# ---------------------------------------------------------------------------


async def test_datetimes_day(events):
    """datetimes() with kind='day' returns one entry per distinct day."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "day")]
    assert len(result) == 4


# ---------------------------------------------------------------------------
# datetimes() — kind='hour'
# ---------------------------------------------------------------------------


async def test_datetimes_hour(events):
    """datetimes() with kind='hour' returns one entry per distinct hour."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "hour")]
    assert len(result) == 4


# ---------------------------------------------------------------------------
# datetimes() — kind='minute'
# ---------------------------------------------------------------------------


async def test_datetimes_minute(events):
    """datetimes() with kind='minute' returns one entry per distinct minute."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "minute")]
    assert len(result) == 4


# ---------------------------------------------------------------------------
# datetimes() — kind='second'
# ---------------------------------------------------------------------------


async def test_datetimes_second(events):
    """datetimes() with kind='second' returns one entry per distinct second."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "second")]
    assert len(result) == 4


# ---------------------------------------------------------------------------
# datetimes() — empty queryset
# ---------------------------------------------------------------------------


async def test_datetimes_empty_queryset(async_db):
    """datetimes() on an empty table returns an empty list."""
    result = [dt async for dt in Event.async_object.datetimes("timestamp", "year")]
    assert result == []


# ---------------------------------------------------------------------------
# datetimes() — with filter
# ---------------------------------------------------------------------------


async def test_datetimes_with_filter(events):
    """datetimes() combined with a filter restricts to matching rows."""
    result = [dt async for dt in Event.async_object.filter(timestamp__year=2020).datetimes("timestamp", "month")]
    assert len(result) == 2
    assert (result[0].year, result[0].month) == (2020, 1)
    assert (result[1].year, result[1].month) == (2020, 3)


# ---------------------------------------------------------------------------
# datetimes() — invalid arguments
# ---------------------------------------------------------------------------


async def test_datetimes_invalid_kind(async_db):
    """datetimes() with an unrecognised kind raises ValueError."""
    with pytest.raises(ValueError, match="'kind' must be one of"):
        [dt async for dt in Event.async_object.datetimes("timestamp", "bad_kind")]


async def test_datetimes_invalid_order(async_db):
    """datetimes() with an unrecognised order raises ValueError."""
    with pytest.raises(ValueError, match="'order' must be either"):
        [dt async for dt in Event.async_object.datetimes("timestamp", "year", order="bad order")]


async def test_datetimes_invalid_field(async_db):
    """datetimes() on a non-existent field raises FieldError."""
    with pytest.raises(FieldError):
        [dt async for dt in Event.async_object.datetimes("invalid_field", "year")]
