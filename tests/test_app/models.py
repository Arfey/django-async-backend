from django.db import (
    DEFAULT_DB_ALIAS,
    models,
)
from django.db.models import Value

from django_async_backend.db import async_connections
from django_async_backend.db.models.base import AsyncModelMixin
from django_async_backend.db.models.manager import AsyncManager


class CustomAsyncManager(AsyncManager):
    async def acreate(self, name, value=None, relative_id=None):
        async with await async_connections[
            DEFAULT_DB_ALIAS
        ].cursor() as cursor:
            table_name = (
                self.model._meta.db_table
            )  # Get the table name dynamically
            columns = ["name"]
            values = [f"'{name}'"]

            if value is not None:
                columns.append("value")
                values.append(f"{value}")

            if relative_id is not None:
                columns.append("relative_id")
                values.append(f"{relative_id}")

            columns_str = ", ".join(columns)
            values_str = ", ".join(values)

            await cursor.execute(
                f"INSERT INTO {table_name} ({columns_str}) "
                f"VALUES ({values_str});"
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


class SaveModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    class Meta:
        db_table = "save_model"


class TestModel(AbstractBaseModel):
    relative = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="relatives",
    )

    class Meta:
        db_table = "test_model"


class GetLatestByModel(AbstractBaseModel):

    class Meta:
        db_table = "latest_by"
        get_latest_by = "id"


class ParentModel(models.Model):
    parent_value = models.IntegerField(null=True)

    async_object = AsyncManager()

    class Meta:
        db_table = "parent_model"


class ChildModel(ParentModel):
    """Multi-table inheritance child used to exercise related updates."""

    child_value = models.IntegerField(null=True)

    async_object = AsyncManager()

    class Meta:
        db_table = "child_model"


class SaveParentModel(AsyncModelMixin, models.Model):
    parent_value = models.IntegerField(null=True)

    class Meta:
        db_table = "save_parent_model"


class SaveChildModel(SaveParentModel):

    child_value = models.IntegerField(null=True)

    class Meta:
        db_table = "save_child_model"


class RelatedSaveModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    fk = models.ForeignKey(
        SaveModel,
        on_delete=models.CASCADE,
        null=True,
        related_name="+",
    )
    o2o = models.OneToOneField(
        SaveModel,
        on_delete=models.CASCADE,
        null=True,
        related_name="+",
    )

    class Meta:
        db_table = "related_save_model"


class DbDefaultModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    ts = models.IntegerField(db_default=Value(7))

    class Meta:
        db_table = "db_default_model"
