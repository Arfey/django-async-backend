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


class GrandChildModel(ChildModel):
    """Third level of multi-table inheritance, so deleting a leaf has to
    cascade up through two parent tables.
    """

    grand_child_value = models.IntegerField(null=True)

    class Meta:
        db_table = "grand_child_model"


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


class TouchingForeignKey(models.ForeignKey):
    """A ForeignKey with its own pre_save(), so update_or_create() has to add
    both its name and its attname to update_fields.
    """

    def pre_save(self, model_instance, add):
        return super().pre_save(model_instance, add)


class UpdateOrCreateModel(AsyncModelMixin, models.Model):
    """Covers the update_fields branches of update_or_create():
    ``updated_at`` is added because it defines pre_save(), ``related`` because
    its name differs from its attname, and ``upper_name`` is a non-concrete
    property that forces the plain save() path.
    """

    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)
    updated_at = models.DateTimeField(auto_now=True)
    related = TouchingForeignKey(
        SaveModel,
        on_delete=models.CASCADE,
        null=True,
        related_name="+",
    )

    class Meta:
        db_table = "update_or_create_model"

    @property
    def upper_name(self):
        return self.name.upper()

    @upper_name.setter
    def upper_name(self, value):
        self.name = value.lower()


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


class GenericChildModel(AsyncModelMixin, models.Model):
    """Generic child collected via GenericRelation.bulk_related_objects()."""

    name = models.CharField(max_length=255)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey("content_type", "object_id")

    class Meta:
        db_table = "generic_child_model"


class GenericRelationModel(AsyncModelMixin, models.Model):
    """Delete target whose generic children cascade via its GenericRelation."""

    name = models.CharField(max_length=255)
    children = GenericRelation(GenericChildModel)

    class Meta:
        db_table = "generic_relation_model"


class M2MTagModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "m2m_tag_model"


class M2MOwnerModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    tags = models.ManyToManyField(M2MTagModel, related_name="owners")

    class Meta:
        db_table = "m2m_owner_model"


class FastDeleteModel(AsyncModelMixin, models.Model):
    """No relations and no signal listeners, so it can be fast-deleted."""

    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "fast_delete_model"


class DeleteModel(AsyncModelMixin, models.Model):
    """Delete target with one child per on_delete handler."""

    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    class Meta:
        db_table = "delete_model"


class CascadeChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.CASCADE,
        related_name="cascade_children",
    )

    class Meta:
        db_table = "cascade_child_model"


class ProtectChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.PROTECT,
        related_name="protect_children",
    )

    class Meta:
        db_table = "protect_child_model"


class RestrictChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.RESTRICT,
        related_name="restrict_children",
    )
    # RESTRICT is lifted when the same object is collected via CASCADE.
    owner = models.ForeignKey(
        DeleteModel,
        on_delete=models.CASCADE,
        null=True,
        related_name="restrict_owned",
    )

    class Meta:
        db_table = "restrict_child_model"


class SetNullChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.SET_NULL,
        null=True,
        related_name="set_null_children",
    )

    class Meta:
        db_table = "set_null_child_model"


class SetDefaultChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.SET_DEFAULT,
        null=True,
        default=None,
        related_name="set_default_children",
    )

    class Meta:
        db_table = "set_default_child_model"


def get_set_callable_parent():
    return None


class SetCallableChildModel(AsyncModelMixin, models.Model):
    """SET(callable) exercises the non-lazy branch of the collector."""

    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.SET(get_set_callable_parent),
        null=True,
        related_name="set_callable_children",
    )

    class Meta:
        db_table = "set_callable_child_model"


class SetChildModel(AsyncModelMixin, models.Model):
    """SET(None) exercises the lazy_sub_objs branch of the collector."""

    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.SET(None),
        null=True,
        related_name="set_children",
    )

    class Meta:
        db_table = "set_child_model"


def sync_on_delete(collector, field, sub_objs, using):
    """A synchronous on_delete handler, which async delete has to reject."""


async def async_on_delete(collector, field, sub_objs, using):
    collector.add_field_update(field, None, sub_objs)


class SyncOnDeleteParentModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "sync_on_delete_parent_model"


class SyncOnDeleteChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        SyncOnDeleteParentModel,
        on_delete=sync_on_delete,
        null=True,
        related_name="children",
    )

    class Meta:
        db_table = "sync_on_delete_child_model"


class AsyncOnDeleteParentModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "async_on_delete_parent_model"


class AsyncOnDeleteChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        AsyncOnDeleteParentModel,
        on_delete=async_on_delete,
        null=True,
        related_name="children",
    )

    class Meta:
        db_table = "async_on_delete_child_model"


class DoNothingChildModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        DeleteModel,
        on_delete=models.DO_NOTHING,
        null=True,
        db_constraint=False,
        related_name="do_nothing_children",
    )

    class Meta:
        db_table = "do_nothing_child_model"


class MultiLevelDeleteModel(AsyncModelMixin, models.Model):
    """Cascade chain deep enough for the collector to walk its own relations
    more than once, which queues one field update per level.
    """

    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        null=True,
        related_name="children",
    )

    class Meta:
        db_table = "multi_level_delete_model"


class MultiLevelSetNullChildModel(AsyncModelMixin, models.Model):
    """SET_NULL defers its sub_objs, so every visit queues another queryset."""

    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        MultiLevelDeleteModel,
        on_delete=models.SET_NULL,
        null=True,
        related_name="set_null_children",
    )

    class Meta:
        db_table = "multi_level_set_null_child_model"


class KeepParentsParentModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "keep_parents_parent_model"


class KeepParentsChildModel(KeepParentsParentModel):
    """Multi-table inheritance child whose parent is the target of a cascading
    foreign key, so keep_parents=True has to skip that reverse relation.
    """

    child_value = models.IntegerField(null=True)

    class Meta:
        db_table = "keep_parents_child_model"


class KeepParentsRefModel(AsyncModelMixin, models.Model):
    """The child inherits this reverse relation, and it points at a row that
    keep_parents=True leaves behind.
    """

    name = models.CharField(max_length=255, unique=True)
    parent = models.ForeignKey(
        KeepParentsParentModel,
        on_delete=models.CASCADE,
        related_name="refs",
    )

    class Meta:
        db_table = "keep_parents_ref_model"


class DatesModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)
    date = models.DateField(null=True)
    datetime = models.DateTimeField(null=True)

    class Meta:
        db_table = "dates_model"


class SelectRelatedAuthorModel(AsyncModelMixin, models.Model):
    """Root of the select_related graph: reachable forward from
    ``SelectRelatedBookModel.author`` and backwards through the reverse
    one-to-one ``profile``.
    """

    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "select_related_author_model"


class SelectRelatedProfileModel(AsyncModelMixin, models.Model):
    """Reverse one-to-one target, so select_related("profile") exercises the
    reverse branch of SQLCompiler.get_related_selections().
    """

    author = models.OneToOneField(
        SelectRelatedAuthorModel,
        on_delete=models.CASCADE,
        related_name="profile",
    )
    bio = models.CharField(max_length=255)

    class Meta:
        db_table = "select_related_profile_model"


class SelectRelatedPublisherModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "select_related_publisher_model"


class DepthLevel0Model(AsyncModelMixin, models.Model):
    """Head of a chain of non-nullable foreign keys. select_related() with no
    arguments only descends non-null relations, so a chain this deep is what
    makes get_related_selections() hit its ``cur_depth > max_depth`` bail-out.
    """

    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "depth_level0_model"


def _depth_level(index, parent_model):
    return type(
        "DepthLevel%dModel" % index,
        (AsyncModelMixin, models.Model),
        {
            "__module__": __name__,
            "name": models.CharField(max_length=255, unique=True),
            "parent": models.ForeignKey(
                parent_model,
                on_delete=models.CASCADE,
                related_name="children",
            ),
            "Meta": type(
                "Meta", (), {"db_table": "depth_level%d_model" % index}
            ),
        },
    )


DepthLevel1Model = _depth_level(1, DepthLevel0Model)
DepthLevel2Model = _depth_level(2, DepthLevel1Model)
DepthLevel3Model = _depth_level(3, DepthLevel2Model)
DepthLevel4Model = _depth_level(4, DepthLevel3Model)
DepthLevel5Model = _depth_level(5, DepthLevel4Model)
DepthLevel6Model = _depth_level(6, DepthLevel5Model)
DepthLevel7Model = _depth_level(7, DepthLevel6Model)


class SelectRelatedMtiParentModel(AsyncModelMixin, models.Model):
    """Multi-table inheritance base. Selecting the child through the implicit
    parent link makes RelatedPopulator take its ``reorder_for_init`` branch,
    because the parent columns are not in Model.__init__ order.
    """

    name = models.CharField(max_length=255, unique=True)

    class Meta:
        db_table = "select_related_mti_parent_model"


class SelectRelatedMtiChildModel(SelectRelatedMtiParentModel):
    child_value = models.IntegerField(null=True)

    class Meta:
        db_table = "select_related_mti_child_model"


class SelectRelatedBookModel(AsyncModelMixin, models.Model):
    """Two forward foreign keys, one of them nullable, so select_related() can
    be tested with several relations at once and with a NULL related object.
    """

    title = models.CharField(max_length=255, unique=True)
    author = models.ForeignKey(
        SelectRelatedAuthorModel,
        on_delete=models.CASCADE,
        related_name="books",
    )
    publisher = models.ForeignKey(
        SelectRelatedPublisherModel,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="books",
    )

    class Meta:
        db_table = "select_related_book_model"
