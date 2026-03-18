from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestExtra(AsyncioTestCase):
    async def asyncSetUp(self):
        await TestModel.async_object.acreate(name="Test1", value=3)
        await TestModel.async_object.acreate(name="Test2", value=1)
        await TestModel.async_object.acreate(name="Test3", value=2)

    async def test_extra_select(self):
        results = [
            obj
            async for obj in TestModel.async_object.extra(
                select={"value_plus_one": "value + 1"}
            )
        ]

        self.assertEqual(
            len(results), 3, "Should return all objects with extra field"
        )
        self.assertEqual(
            results[0].value_plus_one,
            4,
            "First object extra field should be 4",
        )
        self.assertEqual(
            results[1].value_plus_one,
            2,
            "Second object extra field should be 2",
        )
        self.assertEqual(
            results[2].value_plus_one,
            3,
            "Third object extra field should be 3",
        )

    async def test_extra_where(self):
        results = [
            obj
            async for obj in TestModel.async_object.extra(where=["value > 1"])
        ]

        self.assertEqual(
            len(results),
            2,
            "Should return objects matching the extra where condition",
        )
        self.assertEqual(results[0].value, 3, "First object value should be 3")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )

    async def test_extra_order_by(self):
        results = [
            obj
            async for obj in TestModel.async_object.extra(order_by=["-value"])
        ]

        self.assertEqual(
            len(results),
            3,
            "Should return all objects ordered by extra order_by condition",
        )
        self.assertEqual(results[0].value, 3, "First object value should be 3")
        self.assertEqual(
            results[1].value, 2, "Second object value should be 2"
        )
        self.assertEqual(results[2].value, 1, "Third object value should be 1")

    async def test_extra_with_slice_error(self):
        with self.assertRaises(TypeError):
            [
                obj
                async for obj in TestModel.async_object[:1].extra(
                    select={"value_plus_one": "value + 1"}
                )
            ]
