# This file was generated automatically. Do not modify it manually. (based on django 6.0)
import copy
import inspect
import warnings
from collections import defaultdict
from functools import partialmethod
from itertools import chain

import django
from asgiref.sync import sync_to_async
from django.apps import apps
from django.conf import settings
from django.core import checks
from django.core.exceptions import (
    NON_FIELD_ERRORS,
    FieldDoesNotExist,
    FieldError,
    MultipleObjectsReturned,
    ObjectDoesNotExist,
    ObjectNotUpdated,
    ValidationError,
)
from django.db import (
    DJANGO_VERSION_PICKLE_KEY,
    DatabaseError,
    connection,
    router,
    transaction,
)
from django.db.models import (
    NOT_PROVIDED,
    ExpressionWrapper,
    IntegerField,
    Max,
    Value,
)
from django.db.models.constants import LOOKUP_SEP
from django.db.models.deletion import (
    CASCADE,
    Collector,
)
from django.db.models.expressions import DatabaseDefault
from django.db.models.fields.composite import CompositePrimaryKey
from django.db.models.fields.related import (
    ForeignObjectRel,
    OneToOneField,
    lazy_related_operation,
    resolve_relation,
)
from django.db.models.functions import Coalesce
from django.db.models.manager import Manager
from django.db.models.options import Options
from django.db.models.query import (
    F,
    Q,
)
from django.db.models.signals import (
    class_prepared,
    post_init,
    post_save,
    pre_init,
    pre_save,
)
from django.db.models.utils import (
    AltersData,
    make_model_tuple,
)
from django.utils.encoding import force_str
from django.utils.functional import classproperty
from django.utils.hashable import make_hashable
from django.utils.text import (
    capfirst,
    get_text_list,
)
from django.utils.translation import gettext_lazy as _

from django_async_backend.db import async_connections
from django_async_backend.db.models.manager import AsyncManager
from django_async_backend.db.transaction import (
    async_atomic,
    async_mark_for_rollback_on_error,
)


class AsyncModelMixin:
    @classproperty
    def async_objects(cls):
        manager = cls.__dict__.get("_async_objects_cache")
        if manager is None:
            manager = AsyncManager()
            manager.name = "async_objects"
            manager.model = cls
            cls._async_objects_cache = manager
        return manager
    @classproperty
    def _async_base_manager(cls):
        manager = cls.__dict__.get("_async_base_manager_cache")
        if manager is None:
            manager = AsyncManager()
            manager.name = "_async_base_manager"
            manager.model = cls
            manager.auto_created = True
            cls._async_base_manager_cache = manager
        return manager

    async def async_save(
        self,
        *,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        """
        Save the current instance. Override this in a subclass if you want to
        control the saving process.

        The 'force_insert' and 'force_update' parameters can be used to insist
        that the "save" must be an SQL insert or update (or equivalent for
        non-SQL backends), respectively. Normally, they should not be set.
        """

        await self._async_prepare_related_fields_for_save(
            operation_name="save"
        )

        using = using or router.db_for_write(self.__class__, instance=self)
        if force_insert and (force_update or update_fields):
            raise ValueError(
                "Cannot force both insert and updating in model saving."
            )

        deferred_non_generated_fields = {
            f.attname
            for f in self._meta.concrete_fields
            if f.attname not in self.__dict__ and f.generated is False
        }
        if update_fields is not None:
            # If update_fields is empty, skip the save. We do also check for
            # no-op saves later on for inheritance cases. This bailout is
            # still needed for skipping signal sending.
            if not update_fields:
                return

            update_fields = frozenset(update_fields)
            field_names = self._meta._non_pk_concrete_field_names
            not_updatable_fields = update_fields.difference(field_names)

            if not_updatable_fields:
                raise ValueError(
                    "The following fields do not exist in this model, are m2m "
                    "fields, primary keys, or are non-concrete fields: %s"
                    % ", ".join(not_updatable_fields)
                )

        # If saving to the same database, and this model is deferred, then
        # automatically do an "update_fields" save on the loaded fields.
        elif (
            not force_insert
            and deferred_non_generated_fields
            and using == self._state.db
            and self._is_pk_set()
        ):
            field_names = set()
            pk_fields = self._meta.pk_fields
            for field in self._meta.concrete_fields:
                if field not in pk_fields and not hasattr(field, "through"):
                    field_names.add(field.attname)
            loaded_fields = field_names.difference(
                deferred_non_generated_fields
            )
            if loaded_fields:
                update_fields = frozenset(loaded_fields)

        await self._async_save_base(
            using=using,
            force_insert=force_insert,
            force_update=force_update,
            update_fields=update_fields,
        )

    async_save.alters_data = True

    async def _async_save_base(
        self,
        raw=False,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        """
        Handle the parts of saving which should be done only once per save,
        yet need to be done in raw saves, too. This includes some sanity
        checks and signal sending.

        The 'raw' argument is telling save_base not to save any parent
        models and not to do any changes to the values before save. This
        is used by fixture loading.
        """
        using = using or router.db_for_write(self.__class__, instance=self)
        assert not (force_insert and (force_update or update_fields))
        assert update_fields is None or update_fields
        cls = origin = self.__class__
        # Skip proxies, but keep the origin as the proxy model.
        if cls._meta.proxy:
            cls = cls._meta.concrete_model
        meta = cls._meta
        if not meta.auto_created:
            await pre_save.asend(
                sender=origin,
                instance=self,
                raw=raw,
                using=using,
                update_fields=update_fields,
            )
        # A transaction isn't needed if one query is issued.
        if meta.parents:
            context_manager = async_atomic(using=using, savepoint=False)
        else:
            context_manager = async_mark_for_rollback_on_error(using=using)
        async with context_manager:
            parent_inserted = False
            if not raw:
                # Validate force insert only when parents are inserted.
                force_insert = self._validate_force_insert(force_insert)
                parent_inserted = self._save_parents(
                    cls, using, update_fields, force_insert
                )
            updated = await self._async_save_table(
                raw,
                cls,
                force_insert or parent_inserted,
                force_update,
                using,
                update_fields,
            )
        # Store the database on which the object was saved
        self._state.db = using
        # Once saved, this is no longer a to-be-added instance.
        self._state.adding = False

        # Signal that the save is complete
        if not meta.auto_created:
            await post_save.asend(
                sender=origin,
                instance=self,
                created=(not updated),
                update_fields=update_fields,
                raw=raw,
                using=using,
            )

    _async_save_base.alters_data = True

    async def _async_save_table(
        self,
        raw=False,
        cls=None,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        """
        Do the heavy-lifting involved in saving. Update or insert the data
        for a single table.
        """
        meta = cls._meta
        pk_fields = meta.pk_fields
        non_pks_non_generated = [
            f
            for f in meta.local_concrete_fields
            if f not in pk_fields and not f.generated
        ]

        if update_fields:
            non_pks_non_generated = [
                f
                for f in non_pks_non_generated
                if f.name in update_fields or f.attname in update_fields
            ]

        if not self._is_pk_set(meta):
            pk_val = meta.pk.get_pk_value_on_save(self)
            setattr(self, meta.pk.attname, pk_val)
        pk_set = self._is_pk_set(meta)
        if not pk_set and (force_update or update_fields):
            raise ValueError(
                "Cannot force an update in save() with no primary key."
            )
        updated = False
        # Skip an UPDATE when adding an instance and primary key has a default.
        if (
            not raw
            and not force_insert
            and not force_update
            and self._state.adding
            and all(
                f.has_default() or f.has_db_default() for f in meta.pk_fields
            )
        ):
            force_insert = True
        # If possible, try an UPDATE. If that doesn't update anything, do an
        # INSERT.
        if pk_set and not force_insert:
            base_qs = cls._base_manager.using(using)
            values = [
                (
                    f,
                    None,
                    (
                        getattr(self, f.attname)
                        if raw
                        else f.pre_save(self, False)
                    ),
                )
                for f in non_pks_non_generated
            ]
            forced_update = update_fields or force_update
            pk_val = self._get_pk_val(meta)
            returning_fields = [
                f
                for f in meta.local_concrete_fields
                if (
                    f.generated
                    and f.referenced_fields.intersection(non_pks_non_generated)
                )
            ]
            for field, _model, value in values:
                if (
                    update_fields is None or field.name in update_fields
                ) and hasattr(value, "resolve_expression"):
                    returning_fields.append(field)
            results = self._do_update(
                base_qs,
                using,
                pk_val,
                values,
                update_fields,
                forced_update,
                returning_fields,
            )
            if updated := bool(results):
                self._assign_returned_values(results[0], returning_fields)
            elif force_update:
                raise self.NotUpdated("Forced update did not affect any rows.")
            elif update_fields:
                raise self.NotUpdated(
                    "Save with update_fields did not affect any rows."
                )
        if not updated:
            if meta.order_with_respect_to:
                # If this is a model with an order_with_respect_to
                # autopopulate the _order field
                field = meta.order_with_respect_to
                filter_args = field.get_filter_kwargs_for_object(self)
                self._order = (
                    cls._base_manager.using(using)
                    .filter(**filter_args)
                    .aggregate(
                        _order__max=Coalesce(
                            ExpressionWrapper(
                                Max("_order") + Value(1),
                                output_field=IntegerField(),
                            ),
                            Value(0),
                        ),
                    )["_order__max"]
                )
            insert_fields = [
                f
                for f in meta.local_concrete_fields
                if not f.generated and (pk_set or f is not meta.auto_field)
            ]
            returning_fields = list(meta.db_returning_fields)
            can_return_columns_from_insert = async_connections[
                using
            ].features.can_return_columns_from_insert
            for field in insert_fields:
                value = (
                    getattr(self, field.attname)
                    if raw
                    else field.pre_save(self, False)
                )
                if hasattr(value, "resolve_expression"):
                    if field not in returning_fields:
                        returning_fields.append(field)
                elif (
                    field.db_returning
                    and not can_return_columns_from_insert
                    and not (pk_set and field is meta.auto_field)
                ):
                    returning_fields.remove(field)
            results = await self._async_do_insert(
                cls._async_base_manager,
                using,
                insert_fields,
                returning_fields,
                raw,
            )
            if results:
                self._assign_returned_values(results[0], returning_fields)
        return updated

    async def _async_do_insert(
        self, manager, using, fields, returning_fields, raw
    ):
        """
        Do an INSERT. If returning_fields is defined then this method should
        return the newly created data for the model.
        """
        return await manager._insert(
            [self],
            fields=fields,
            returning_fields=returning_fields,
            using=using,
            raw=raw,
        )

    async def _async_prepare_related_fields_for_save(
        self, operation_name, fields=None
    ):
        # Ensure that a model instance without a PK hasn't been assigned to
        # a ForeignKey, GenericForeignKey or OneToOneField on this model. If
        # the field is nullable, allowing the save would result in silent data
        # loss.
        for field in self._meta.concrete_fields:
            if fields and field not in fields:
                continue
            # If the related field isn't cached, then an instance hasn't been
            # assigned and there's no need to worry about this check.
            if field.is_relation and field.is_cached(self):
                obj = getattr(self, field.name, None)
                if not obj:
                    continue
                # A pk may have been assigned manually to a model instance not
                # saved to the database (or auto-generated in a case like
                # UUIDField), but we allow the save to proceed and rely on the
                # database to raise an IntegrityError if applicable. If
                # constraints aren't supported by the database, there's the
                # unavoidable risk of data corruption.
                if not obj._is_pk_set():
                    # Remove the object from a related instance cache.
                    if not field.remote_field.multiple:
                        field.remote_field.delete_cached_value(obj)
                    raise ValueError(
                        "%s() prohibited to prevent data loss due to unsaved "
                        "related object '%s'." % (operation_name, field.name)
                    )
                elif getattr(self, field.attname) in field.empty_values:
                    # Set related object if it has been saved after an
                    # assignment.
                    setattr(self, field.name, obj)
                # If the relationship's pk/to_field was changed, clear the
                # cached relationship.
                if getattr(obj, field.target_field.attname) != getattr(
                    self, field.attname
                ):
                    field.delete_cached_value(self)
        # GenericForeignKeys are private.
        for field in self._meta.private_fields:
            if fields and field not in fields:
                continue
            if (
                field.is_relation
                and field.is_cached(self)
                and hasattr(field, "fk_field")
            ):
                obj = field.get_cached_value(self, default=None)
                if obj and not obj._is_pk_set():
                    raise ValueError(
                        f"{operation_name}() prohibited to prevent data loss due to "
                        f"unsaved related object '{field.name}'."
                    )
