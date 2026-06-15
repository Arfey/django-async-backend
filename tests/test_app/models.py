from django.db import DEFAULT_DB_ALIAS, models

from django_async_backend.db.models.manager import AsyncManager
from django_async_backend.db.models.base import AsyncSaveModel
from django_async_backend.db import async_connections


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


# --- Async save-contract prototype models -----------------------------------
# Each does `self.foo += 1` in whichever method(s) it overrides, so the final
# `foo` value reveals exactly which code path ran (and how many times).


class SaveContractBase(AsyncSaveModel):
    foo = models.IntegerField(default=0)

    async_object = AsyncManager()

    class Meta:
        abstract = True


class SCNeither(SaveContractBase):
    """Overrides nothing."""

    class Meta:
        db_table = "sc_neither"


class SCSave(SaveContractBase):
    """Overrides sync save only."""

    class Meta:
        db_table = "sc_save"

    def save(self, *args, **kwargs):
        self.foo += 1
        super().save(*args, **kwargs)


class SCAsave(SaveContractBase):
    """Overrides async asave only."""

    class Meta:
        db_table = "sc_asave"

    async def asave(self, *args, **kwargs):
        self.foo += 1
        await super().asave(*args, **kwargs)


class SCBoth(SaveContractBase):
    """Overrides both save and asave (the irreducible hard case)."""

    class Meta:
        db_table = "sc_both"

    def save(self, *args, **kwargs):
        self.foo += 1
        super().save(*args, **kwargs)

    async def asave(self, *args, **kwargs):
        self.foo += 1
        await super().asave(*args, **kwargs)
