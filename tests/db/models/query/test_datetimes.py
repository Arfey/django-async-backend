import datetime

from django.test import override_settings
from django.utils import timezone
from test_app.models import DatesModel

from django_async_backend.test import AsyncioTestCase


class TestDatetimes(AsyncioTestCase):
    async def _create(self, name, value):
        await DatesModel(name=name, datetime=value).async_save()

    async def test_kind_second(self):
        await self._create("Test1", datetime.datetime(2026, 1, 2, 3, 4, 5))
        await self._create("Test2", datetime.datetime(2026, 1, 2, 3, 4, 6))

        queryset = DatesModel.async_objects.datetimes("datetime", "second")

        self.assertEqual(
            [i async for i in queryset],
            [
                datetime.datetime(2026, 1, 2, 3, 4, 5),
                datetime.datetime(2026, 1, 2, 3, 4, 6),
            ],
            "datetimes should return every second, ascending",
        )

    async def test_kind_hour(self):
        await self._create("Test1", datetime.datetime(2026, 1, 2, 3, 4, 5))
        await self._create("Test2", datetime.datetime(2026, 1, 2, 3, 40, 50))

        queryset = DatesModel.async_objects.datetimes("datetime", "hour")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.datetime(2026, 1, 2, 3, 0, 0)],
            "datetimes should truncate to the hour and dedupe",
        )

    async def test_kind_minute(self):
        await self._create("Test1", datetime.datetime(2026, 1, 2, 3, 4, 5))

        queryset = DatesModel.async_objects.datetimes("datetime", "minute")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.datetime(2026, 1, 2, 3, 4, 0)],
            "datetimes should truncate to the minute",
        )

    async def test_kind_day(self):
        await self._create("Test1", datetime.datetime(2026, 1, 2, 3, 4, 5))

        queryset = DatesModel.async_objects.datetimes("datetime", "day")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.datetime(2026, 1, 2, 0, 0, 0)],
            "datetimes should truncate to the day",
        )

    async def test_order_desc(self):
        await self._create("Test1", datetime.datetime(2026, 1, 2, 3, 0, 0))
        await self._create("Test2", datetime.datetime(2026, 1, 2, 4, 0, 0))

        queryset = DatesModel.async_objects.datetimes(
            "datetime", "hour", order="DESC"
        )

        self.assertEqual(
            [i async for i in queryset],
            [
                datetime.datetime(2026, 1, 2, 4, 0, 0),
                datetime.datetime(2026, 1, 2, 3, 0, 0),
            ],
            "datetimes should return hours in descending order",
        )

    async def test_ignores_null(self):
        await self._create("Test1", datetime.datetime(2026, 1, 2, 3, 4, 5))
        await self._create("Test2", None)

        queryset = DatesModel.async_objects.datetimes("datetime", "day")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.datetime(2026, 1, 2, 0, 0, 0)],
            "datetimes should skip rows whose field is NULL",
        )

    @override_settings(USE_TZ=True, TIME_ZONE="UTC")
    async def test_use_tz_default_timezone(self):
        """With USE_TZ and no tzinfo, the current timezone is used."""
        await self._create(
            "Test1",
            datetime.datetime(
                2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc
            ),
        )

        queryset = DatesModel.async_objects.datetimes("datetime", "day")

        self.assertEqual(
            [i async for i in queryset],
            [
                datetime.datetime(
                    2026, 1, 2, 0, 0, 0, tzinfo=timezone.get_current_timezone()
                )
            ],
            "datetimes should truncate in the current timezone",
        )

    @override_settings(USE_TZ=True)
    async def test_use_tz_explicit_timezone(self):
        """An explicit tzinfo is used instead of the current timezone."""
        await self._create(
            "Test1",
            datetime.datetime(
                2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc
            ),
        )
        tz = datetime.timezone(datetime.timedelta(hours=-5))

        queryset = DatesModel.async_objects.datetimes(
            "datetime", "day", tzinfo=tz
        )

        self.assertEqual(
            [i async for i in queryset],
            [datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)],
            "datetimes should truncate in the given timezone",
        )

    async def test_no_rows(self):
        queryset = DatesModel.async_objects.datetimes("datetime", "day")

        self.assertEqual(
            [i async for i in queryset],
            [],
            "datetimes should return nothing when there are no rows",
        )

    async def test_invalid_kind(self):
        with self.assertRaisesRegex(
            ValueError,
            r"'kind' must be one of 'year', 'month', 'week', 'day', 'hour', "
            r"'minute', or 'second'\.",
        ):
            DatesModel.async_objects.datetimes("datetime", "century")

    async def test_invalid_order(self):
        with self.assertRaisesRegex(
            ValueError, r"'order' must be either 'ASC' or 'DESC'\."
        ):
            DatesModel.async_objects.datetimes(
                "datetime", "day", order="SIDEWAYS"
            )
