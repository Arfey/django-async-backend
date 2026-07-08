import uuid

from django.db import models
from django.db.models import Value

from django_async_backend.db.models.base import AsyncModelMixin


class AbstractBaseModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

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


class ParentModel(AsyncModelMixin, models.Model):
    parent_value = models.IntegerField(null=True)

    class Meta:
        db_table = "parent_model"


class ChildModel(ParentModel):
    """Multi-table inheritance child used to exercise related updates."""

    child_value = models.IntegerField(null=True)

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


class SaveProxyModel(SaveModel):

    class Meta:
        proxy = True


class UuidPkModel(AsyncModelMixin, models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "uuid_pk_model"


class OrderParentModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "order_parent_model"


class OrderItemModel(AsyncModelMixin, models.Model):
    parent = models.ForeignKey(
        OrderParentModel,
        on_delete=models.CASCADE,
        related_name="items",
    )

    class Meta:
        db_table = "order_item_model"
        order_with_respect_to = "parent"


class PkOnlyModel(AsyncModelMixin, models.Model):

    class Meta:
        db_table = "pk_only_model"


class SelectOnSaveModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    class Meta:
        db_table = "select_on_save_model"
        select_on_save = True


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
