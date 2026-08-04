import datetime

from test_app.models import DatesModel

from django_async_backend.test import AsyncioTestCase


class TestDates(AsyncioTestCase):
    async def _create(self, name, date):
        await DatesModel(name=name, date=date).async_save()

    async def test_kind_day(self):
        await self._create("Test1", datetime.date(2026, 1, 2))
        await self._create("Test2", datetime.date(2026, 3, 4))

        queryset = DatesModel.async_objects.dates("date", "day")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.date(2026, 1, 2), datetime.date(2026, 3, 4)],
            "dates should return every day, ascending",
        )

    async def test_kind_month(self):
        await self._create("Test1", datetime.date(2026, 1, 2))
        await self._create("Test2", datetime.date(2026, 1, 20))

        queryset = DatesModel.async_objects.dates("date", "month")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.date(2026, 1, 1)],
            "dates should truncate to the first of the month and dedupe",
        )

    async def test_kind_year(self):
        await self._create("Test1", datetime.date(2026, 1, 2))
        await self._create("Test2", datetime.date(2026, 8, 9))

        queryset = DatesModel.async_objects.dates("date", "year")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.date(2026, 1, 1)],
            "dates should truncate to the first of the year and dedupe",
        )

    async def test_kind_week(self):
        await self._create("Test1", datetime.date(2026, 1, 2))

        queryset = DatesModel.async_objects.dates("date", "week")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.date(2025, 12, 29)],
            "dates should truncate to the start of the week",
        )

    async def test_order_desc(self):
        await self._create("Test1", datetime.date(2026, 1, 2))
        await self._create("Test2", datetime.date(2026, 3, 4))

        queryset = DatesModel.async_objects.dates("date", "day", order="DESC")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.date(2026, 3, 4), datetime.date(2026, 1, 2)],
            "dates should return days in descending order",
        )

    async def test_ignores_null(self):
        await self._create("Test1", datetime.date(2026, 1, 2))
        await self._create("Test2", None)

        queryset = DatesModel.async_objects.dates("date", "day")

        self.assertEqual(
            [i async for i in queryset],
            [datetime.date(2026, 1, 2)],
            "dates should skip rows whose field is NULL",
        )

    async def test_no_rows(self):
        queryset = DatesModel.async_objects.dates("date", "day")

        self.assertEqual(
            [i async for i in queryset],
            [],
            "dates should return nothing when there are no rows",
        )

    async def test_invalid_kind(self):
        with self.assertRaisesRegex(
            ValueError,
            r"'kind' must be one of 'year', 'month', 'week', or 'day'\.",
        ):
            DatesModel.async_objects.dates("date", "century")

    async def test_invalid_order(self):
        with self.assertRaisesRegex(
            ValueError, r"'order' must be either 'ASC' or 'DESC'\."
        ):
            DatesModel.async_objects.dates("date", "day", order="SIDEWAYS")
