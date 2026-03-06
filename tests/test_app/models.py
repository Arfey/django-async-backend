from django.db import DEFAULT_DB_ALIAS, models

from django_async_backend.db.models.manager import AsyncManager
from django_async_backend.db import async_connections


class CustomAsyncManager(AsyncManager):
    async def acreate(self, name):
        async with await async_connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            await cursor.execute(
                f"INSERT INTO test_model (name) VALUES ('{name}');"
            )
            await cursor.execute(
                f"SELECT * FROM test_model WHERE name = '{name}';"
            )
            row = await cursor.fetchone()
            return TestModel(id=row[0], name=row[1], value=row[2])


class TestModel(models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    async_object = CustomAsyncManager()

    class Meta:
        db_table = "test_model"
