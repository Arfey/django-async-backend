import uuid

from django.contrib.contenttypes.fields import (
    GenericForeignKey,
    GenericRelation,
)
from django.contrib.contenttypes.models import ContentType
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


class DbDefaultPkModel(AsyncModelMixin, models.Model):
    id = models.IntegerField(primary_key=True, db_default=Value(1))
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "db_default_pk_model"


class GetOrCreateModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    code = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "get_or_create_model"


class GenericFkModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255)
    content_type = models.ForeignKey(
        ContentType,
        on_delete=models.CASCADE,
        null=True,
    )
    object_id = models.PositiveIntegerField(null=True)
    content_object = GenericForeignKey("content_type", "object_id")

    class Meta:
        db_table = "generic_fk_model"


class DeleteAuthorModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "delete_author_model"


class DeleteBookModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.CASCADE,
        related_name="books",
    )

    class Meta:
        db_table = "delete_book_model"


class DeleteReviewModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    book = models.ForeignKey(
        DeleteBookModel,
        on_delete=models.CASCADE,
        related_name="reviews",
    )

    class Meta:
        db_table = "delete_review_model"


class DeleteProtectedModel(AsyncModelMixin, models.Model):
    author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.PROTECT,
        null=True,
        related_name="protected_refs",
    )

    class Meta:
        db_table = "delete_protected_model"


class DeleteRestrictedModel(AsyncModelMixin, models.Model):
    author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.RESTRICT,
        null=True,
        related_name="restricted_refs",
    )

    class Meta:
        db_table = "delete_restricted_model"


class DeleteSetNullModel(AsyncModelMixin, models.Model):
    author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.SET_NULL,
        null=True,
        related_name="setnull_refs",
    )

    class Meta:
        db_table = "delete_setnull_model"


class DeleteSetDefaultModel(AsyncModelMixin, models.Model):
    author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.SET_DEFAULT,
        default=None,
        null=True,
        related_name="setdefault_refs",
    )

    class Meta:
        db_table = "delete_setdefault_model"


class DeleteCascadeRestrictModel(AsyncModelMixin, models.Model):
    # RESTRICT is waived when the restricted row is itself collected
    # through a CASCADE from the same delete.
    cascade_author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.CASCADE,
        related_name="cascade_restrict_refs",
    )
    restrict_author = models.ForeignKey(
        DeleteAuthorModel,
        on_delete=models.RESTRICT,
        null=True,
        related_name="restricted_via_cascade_refs",
    )

    class Meta:
        db_table = "delete_cascade_restrict_model"


class DeleteTaggedModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    tags = GenericRelation(
        GenericFkModel,
        content_type_field="content_type",
        object_id_field="object_id",
    )

    class Meta:
        db_table = "delete_tagged_model"
