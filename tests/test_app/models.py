from django.db import DEFAULT_DB_ALIAS, models

from django_async_backend.db.models.manager import AsyncManager
from django_async_backend.db import async_connections


class CustomAsyncManager(AsyncManager):
    async def acreate(self, name, value=None):
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            table_name = (
                self.model._meta.db_table
            )  # Get the table name dynamically
            if value is not None:
                await cursor.execute(
                    f"INSERT INTO {table_name} (name, value) VALUES "
                    f"('{name}', {value});"
                )
            else:
                await cursor.execute(
                    f"INSERT INTO {table_name} (name) VALUES ('{name}');"
                )
            await cursor.execute(
                f"SELECT * FROM {table_name} WHERE name = '{name}';"
            )
            row = await cursor.fetchone()
            model_cls = self.model  # Dynamically get the model class
            field_names = [field.name for field in model_cls._meta.fields]
            field_values = {
                field: value for field, value in zip(field_names, row)
            }
            return model_cls(**field_values)


class AbstractBaseModel(models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    async_object = CustomAsyncManager()

    class Meta:
        abstract = True


class TestModel(AbstractBaseModel):
    class Meta:
        db_table = "test_model"


class GetLatestByModel(AbstractBaseModel):

    class Meta:
        db_table = "latest_by"
        get_latest_by = "id"
