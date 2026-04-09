from django.db import models

from django_async_backend.db.models.base import AsyncModel
from django_async_backend.db.models.manager import AsyncManager


class AbstractBaseModel(AsyncModel, models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    async_object = AsyncManager()

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
