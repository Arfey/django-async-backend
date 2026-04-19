from django_async_backend.db.models import sql as async_sql

"""
The main QuerySet implementation. This provides the public API for the ORM.
"""

import asyncio
import copy
import operator
import warnings
from contextlib import nullcontext
from functools import reduce
from itertools import (
    chain,
)

from asgiref.sync import sync_to_async
from django.conf import settings
from django.core import exceptions
from django.db import (
    IntegrityError,
    NotSupportedError,
    router,
)
from django.db.models import (
    AutoField,
    DateField,
    DateTimeField,
    Max,
    sql,
)
from django.db.models.constants import (
    LOOKUP_SEP,
    OnConflict,
)
from django.db.models.expressions import (
    Case,
    DatabaseDefault,
    F,
    Value,
    When,
)
from django.db.models.functions import (
    Cast,
    Trunc,
)
from django.db.models.query_utils import (
    FilteredRelation,
    Q,
)
from django.db.models.sql.constants import (
    GET_ITERATOR_CHUNK_SIZE,
    ROW_COUNT,
)
from django.db.models.utils import (
    AltersData,
    create_namedtuple_class,
    resolve_callables,
)
from django.utils import timezone
from django.utils.deprecation import RemovedInDjango70Warning
from django.utils.functional import cached_property

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import (
    async_atomic,
    async_mark_for_rollback_on_error,
)

# The maximum number of results to fetch in a get() query.
MAX_GET_RESULTS = 21

# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20

PROHIBITED_FILTER_KWARGS = frozenset(["_connector", "_negated"])


class BaseIterable:
    def __init__(self, queryset, chunked_fetch=False, chunk_size=GET_ITERATOR_CHUNK_SIZE):
        self.queryset = queryset
        self.chunked_fetch = chunked_fetch
        self.chunk_size = chunk_size


class ModelIterable(BaseIterable):
    """Iterable that yields a model instance for each row."""

    async def __aiter__(self):
        queryset = self.queryset
        db = queryset.db
        compiler = queryset.query.get_compiler(using=db)
        # Execute the query. This will also fill compiler.select, klass_info,
        # and annotations.
        results = await compiler.execute_sql(chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size)
        select, klass_info, annotation_col_map = (
            compiler.select,
            compiler.klass_info,
            compiler.annotation_col_map,
        )
        model_cls = klass_info["model"]
        select_fields = klass_info["select_fields"]
        model_fields_start, model_fields_end = (
            select_fields[0],
            select_fields[-1] + 1,
        )
        init_list = [f[0].target.attname for f in select[model_fields_start:model_fields_end]]
        related_populators = get_related_populators(klass_info, select, db)
        known_related_objects = [
            (
                field,
                related_objs,
                operator.attrgetter(
                    *[
                        (field.attname if from_field == "self" else queryset.model._meta.get_field(from_field).attname)
                        for from_field in field.from_fields
                    ],
                ),
            )
            for field, related_objs in queryset._known_related_objects.items()
        ]
        for row in await compiler.results_iter(results):
            obj = model_cls.from_db(db, init_list, row[model_fields_start:model_fields_end])
            for rel_populator in related_populators:
                rel_populator.populate(row, obj)
            if annotation_col_map:
                for attr_name, col_pos in annotation_col_map.items():
                    setattr(obj, attr_name, row[col_pos])

            # Add the known related objects to the model.
            for field, rel_objs, rel_getter in known_related_objects:
                # Avoid overwriting objects loaded by, e.g., select_related().
                if field.is_cached(obj):
                    continue
                rel_obj_id = rel_getter(obj)
                try:
                    rel_obj = rel_objs[rel_obj_id]
                except KeyError:
                    pass  # May happen in qs1 | qs2 scenarios.
                else:
                    setattr(obj, field.name, rel_obj)

            yield obj


class RawModelIterable(BaseIterable):
    """
    Iterable that yields a model instance for each row from a raw queryset.
    """

    async def __aiter__(self):
        db = self.queryset.db
        query = self.queryset.query
        connection = async_connections[db]
        compiler = connection.ops.compiler("SQLCompiler")(query, connection, db)

        try:
            (
                model_init_names,
                model_init_pos,
                annotation_fields,
            ) = await self.queryset.aresolve_model_init_order()
            model_cls = self.queryset.model
            if any(f.attname not in model_init_names for f in model_cls._meta.pk_fields):
                raise exceptions.FieldDoesNotExist("Raw query must include the primary key")
            columns = await self.queryset.aget_columns()
            fields = [self.queryset.model_fields.get(c) for c in columns]
            cols = [f.get_col(f.model._meta.db_table) if f else None for f in fields]
            converters = compiler.get_converters(cols)
            rows = [row async for row in query]
            if converters:
                rows = compiler.apply_converters(rows, converters)
            if compiler.has_composite_fields(cols):
                rows = compiler.composite_fields_to_tuples(rows, cols)
            for values in rows:
                model_init_values = [values[pos] for pos in model_init_pos]
                instance = model_cls.from_db(db, model_init_names, model_init_values)
                if annotation_fields:
                    for column, pos in annotation_fields:
                        setattr(instance, column, values[pos])
                yield instance
        finally:
            if hasattr(query, "cursor") and query.cursor:
                await query.cursor.close()


class ValuesIterable(BaseIterable):
    """
    Iterable returned by QuerySet.values() that yields a dict for each row.
    """

    async def __aiter__(self):
        queryset = self.queryset
        query = queryset.query
        compiler = query.get_compiler(queryset.db)

        if query.selected:
            names = list(query.selected)
        else:
            # extra(select=...) cols are always at the start of the row.
            names = [
                *query.extra_select,
                *query.values_select,
                *query.annotation_select,
            ]
        indexes = range(len(names))
        for row in await compiler.results_iter(chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size):
            yield {names[i]: row[i] for i in indexes}


class ValuesListIterable(BaseIterable):
    """
    Iterable returned by QuerySet.values_list(flat=False) that yields a tuple
    for each row.
    """

    async def __aiter__(self):
        queryset = self.queryset
        query = queryset.query
        compiler = query.get_compiler(queryset.db)

        for i in await compiler.results_iter(
            tuple_expected=True,
            chunked_fetch=self.chunked_fetch,
            chunk_size=self.chunk_size,
        ):
            yield i


class NamedValuesListIterable(ValuesListIterable):
    """
    Iterable returned by QuerySet.values_list(named=True) that yields a
    namedtuple for each row.
    """

    async def __aiter__(self):
        queryset = self.queryset
        if queryset._fields:
            names = queryset._fields
        else:
            query = queryset.query
            names = [
                *query.extra_select,
                *query.values_select,
                *query.annotation_select,
            ]
        tuple_class = create_namedtuple_class(*names)
        new = tuple.__new__
        async for row in super().__aiter__():
            yield new(tuple_class, row)


class FlatValuesListIterable(BaseIterable):
    """
    Iterable returned by QuerySet.values_list(flat=True) that yields single
    values.
    """

    async def __aiter__(self):
        queryset = self.queryset
        compiler = queryset.query.get_compiler(queryset.db)
        for row in await compiler.results_iter(chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size):
            yield row[0]


class QuerySet(AltersData):
    """Represent a lazy database lookup for a set of objects."""

    def __init__(self, model=None, query=None, using=None, hints=None):
        self.model = model
        self._db = using
        self._hints = hints or {}
        self._query = query or async_sql.Query(self.model)
        self._result_cache = None
        self._sticky_filter = False
        self._for_write = False
        self._prefetch_related_lookups = ()
        self._prefetch_done = False
        self._known_related_objects = {}  # {rel_field: {pk: rel_obj}}
        self._iterable_class = ModelIterable
        self._fields = None
        self._defer_next_filter = False
        self._deferred_filter = None

    @property
    def query(self):
        if self._deferred_filter:
            negate, args, kwargs = self._deferred_filter
            self._filter_or_exclude_inplace(negate, args, kwargs)
            self._deferred_filter = None
        return self._query

    @query.setter
    def query(self, value):
        if value.values_select:
            self._iterable_class = ValuesIterable
        self._query = value

    def as_manager(cls):
        # Address the circular dependency between `Queryset` and `Manager`.
        from django.db.models.manager import Manager

        manager = Manager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager

    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)

    ########################
    # PYTHON MAGIC METHODS #
    ########################

    def __deepcopy__(self, memo):
        """Don't populate the QuerySet's cache."""
        obj = self.__class__()
        for k, v in self.__dict__.items():
            if k == "_result_cache":
                obj.__dict__[k] = None
            else:
                obj.__dict__[k] = copy.deepcopy(v, memo)
        return obj

    def __iter__(self):
        # Sync iteration works when results are already cached (e.g. after
        # await _fetch_all()). Django internals like get_prefetch_querysets
        # iterate querysets synchronously; the caller must ensure the
        # cache is populated before sync iteration.
        if self._result_cache is not None:
            return iter(self._result_cache)
        raise TypeError(
            "Async QuerySet cannot be iterated synchronously. Use 'async for' or call await qs._fetch_all() first.",
        )

    def __aiter__(self):
        # Remember, __aiter__ itself is synchronous, it's the thing it returns
        # that is async!
        async def generator():
            await self._fetch_all()
            for item in self._result_cache:
                yield item

        return generator()

    def __getitem__(self, k):
        """Retrieve an item or slice from the set of results."""

        async def fetch_data(query_set, index):
            if query_set._result_cache is None:
                await query_set._fetch_all()
            return query_set._result_cache[index]

        async def fetch_data_iter(query_set, index):
            if query_set._result_cache is None:
                await query_set._fetch_all()
            for item in query_set._result_cache[index]:
                yield item

        def get_data():
            if self._result_cache is not None:
                return fetch_data_iter(self, k) if isinstance(k, slice) else fetch_data(self, k)

            if isinstance(k, slice):
                return fetch_data_iter(qs, slice(None, None, k.step)) if k.step else qs

            return fetch_data(qs, 0)

        if not isinstance(k, (int, slice)):
            raise TypeError("QuerySet indices must be integers or slices, not %s." % type(k).__name__)
        if (isinstance(k, int) and k < 0) or (
            isinstance(k, slice) and ((k.start is not None and k.start < 0) or (k.stop is not None and k.stop < 0))
        ):
            raise ValueError("Negative indexing is not supported.")

        if self._result_cache is not None:
            return get_data()

        if isinstance(k, slice):
            qs = self._chain()
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            qs.query.set_limits(start, stop)
            return get_data()

        qs = self._chain()
        qs.query.set_limits(k, k + 1)
        return get_data()

    def __class_getitem__(cls, *args, **kwargs):
        return cls

    def __and__(self, other):
        self._check_operator_queryset(other, "&")
        self._merge_sanity_check(other)
        if isinstance(other, EmptyQuerySet):
            return other
        if isinstance(self, EmptyQuerySet):
            return self
        combined = self._chain()
        combined._merge_known_related_objects(other)
        combined.query.combine(other.query, sql.AND)
        return combined

    def __or__(self, other):
        self._check_operator_queryset(other, "|")
        self._merge_sanity_check(other)
        if isinstance(self, EmptyQuerySet):
            return other
        if isinstance(other, EmptyQuerySet):
            return self
        query = self if self.query.can_filter() else self.model._base_manager.filter(pk__in=self.values("pk"))
        combined = query._chain()
        combined._merge_known_related_objects(other)
        if not other.query.can_filter():
            other = other.model._base_manager.filter(pk__in=other.values("pk"))
        combined.query.combine(other.query, sql.OR)
        return combined

    def __xor__(self, other):
        self._check_operator_queryset(other, "^")
        self._merge_sanity_check(other)
        if isinstance(self, EmptyQuerySet):
            return other
        if isinstance(other, EmptyQuerySet):
            return self
        query = self if self.query.can_filter() else self.model._base_manager.filter(pk__in=self.values("pk"))
        combined = query._chain()
        combined._merge_known_related_objects(other)
        if not other.query.can_filter():
            other = other.model._base_manager.filter(pk__in=other.values("pk"))
        combined.query.combine(other.query, sql.XOR)
        return combined

    async def aaggregate(self, *args, **kwargs):
        """
        Return a dictionary containing the calculations (aggregation)
        over the current queryset.

        If args is present the expression is passed as a kwarg using
        the Aggregate object's default alias.
        """
        if self.query.distinct_fields:
            raise NotImplementedError("aggregate() + distinct(fields) not implemented.")
        self._validate_values_are_expressions((*args, *kwargs.values()), method_name="aggregate")
        for arg in args:
            # The default_alias property raises TypeError if default_alias
            # can't be set automatically or AttributeError if it isn't an
            # attribute.
            try:
                arg.default_alias
            except AttributeError, TypeError:
                raise TypeError("Complex aggregates require an alias")
            kwargs[arg.default_alias] = arg

        return await self.query.chain().get_aggregation(self.db, kwargs)

    async def acount(self):
        """
        Perform a SELECT COUNT() and return the number of records as an
        integer.

        If the QuerySet is already fully cached, return the length of the
        cached results set to avoid multiple SELECT COUNT(*) calls.
        """
        if self._result_cache is not None:
            return len(self._result_cache)

        return await self.query.get_count(using=self.db)

    async def aget(self, *args, **kwargs):
        """
        Perform the query and return a single object matching the given
        keyword arguments.
        """
        if self.query.combinator and (args or kwargs):
            raise NotSupportedError(
                "Calling QuerySet.get(...) with filters after %s() is not supported." % self.query.combinator,
            )
        clone = self._chain() if self.query.combinator else self.filter(*args, **kwargs)
        if self.query.can_filter() and not self.query.distinct_fields:
            clone = clone.order_by()
        limit = None
        if (
            not clone.query.select_for_update
            or async_connections[clone.db].features.supports_select_for_update_with_limit
        ):
            limit = MAX_GET_RESULTS
            clone.query.set_limits(high=limit)
        num = len([i async for i in clone])
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self.model.DoesNotExist("%s matching query does not exist." % self.model._meta.object_name)
        raise self.model.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!"
            % (
                self.model._meta.object_name,
                (num if not limit or num < limit else "more than %s" % (limit - 1)),
            ),
        )

    def _prepare_for_bulk_create(self, objs):
        objs_with_pk, objs_without_pk = [], []
        for obj in objs:
            if isinstance(obj.pk, DatabaseDefault):
                objs_without_pk.append(obj)
            elif obj._is_pk_set():
                objs_with_pk.append(obj)
            else:
                obj.pk = obj._meta.pk.get_pk_value_on_save(obj)
                if obj._is_pk_set():
                    objs_with_pk.append(obj)
                else:
                    objs_without_pk.append(obj)
            obj._prepare_related_fields_for_save(operation_name="bulk_create")
        return objs_with_pk, objs_without_pk

    def _check_bulk_create_options(self, ignore_conflicts, update_conflicts, update_fields, unique_fields):
        if ignore_conflicts and update_conflicts:
            raise ValueError("ignore_conflicts and update_conflicts are mutually exclusive.")
        db_features = async_connections[self.db].features
        if ignore_conflicts:
            if not db_features.supports_ignore_conflicts:
                raise NotSupportedError("This database backend does not support ignoring conflicts.")
            return OnConflict.IGNORE
        if update_conflicts:
            if not db_features.supports_update_conflicts:
                raise NotSupportedError("This database backend does not support updating conflicts.")
            if not update_fields:
                raise ValueError(
                    "Fields that will be updated when a row insertion fails on conflicts must be provided.",
                )
            if unique_fields and not db_features.supports_update_conflicts_with_target:
                raise NotSupportedError(
                    "This database backend does not support updating "
                    "conflicts with specifying unique fields that can trigger "
                    "the upsert.",
                )
            if not unique_fields and db_features.supports_update_conflicts_with_target:
                raise ValueError("Unique fields that can trigger the upsert must be provided.")
            # Updating primary keys and non-concrete fields is forbidden.
            if any(not f.concrete for f in update_fields):
                raise ValueError("bulk_create() can only be used with concrete fields in update_fields.")
            if any(f in self.model._meta.pk_fields for f in update_fields):
                raise ValueError("bulk_create() cannot be used with primary keys in update_fields.")
            if unique_fields:
                if any(not f.concrete for f in unique_fields):
                    raise ValueError("bulk_create() can only be used with concrete fields in unique_fields.")
            return OnConflict.UPDATE
        return None

    def _handle_order_with_respect_to(self, objs):
        if objs and (order_wrt := self.model._meta.order_with_respect_to):
            get_filter_kwargs_for_object = order_wrt.get_filter_kwargs_for_object
            attnames = list(get_filter_kwargs_for_object(objs[0]))
            group_keys = set()
            obj_groups = []
            for obj in objs:
                group_key = tuple(get_filter_kwargs_for_object(obj).values())
                group_keys.add(group_key)
                obj_groups.append((obj, group_key))
            filters = [Q.create(list(zip(attnames, group_key))) for group_key in group_keys]
            next_orders = (
                self.model._base_manager.using(self.db)
                .filter(reduce(operator.or_, filters))
                .values_list(*attnames)
                .annotate(_order__max=Max("_order") + 1)
            )
            # Create mapping of group values to max order.
            group_next_orders = dict.fromkeys(group_keys, 0)
            group_next_orders.update((tuple(group_key), next_order) for *group_key, next_order in next_orders)
            # Assign _order values to new objects.
            for obj, group_key in obj_groups:
                if getattr(obj, "_order", None) is None:
                    group_next_order = group_next_orders[group_key]
                    obj._order = group_next_order
                    group_next_orders[group_key] += 1

    def _extract_model_params(self, defaults, **kwargs):
        """
        Prepare `params` for creating a model instance based on the given
        kwargs; for use by get_or_create().
        """
        defaults = defaults or {}
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        property_names = self.model._meta._property_names
        invalid_params = []
        for param in params:
            try:
                self.model._meta.get_field(param)
            except exceptions.FieldDoesNotExist:
                # It's okay to use a model's property if it has a setter.
                if not (param in property_names and getattr(self.model, param).fset):
                    invalid_params.append(param)
        if invalid_params:
            raise exceptions.FieldError(
                "Invalid field name(s) for model %s: '%s'."
                % (
                    self.model._meta.object_name,
                    "', '".join(sorted(invalid_params)),
                ),
            )
        return params

    def _earliest(self, *fields):
        """
        Return the earliest object according to fields (if given) or by the
        model's Meta.get_latest_by.
        """
        if fields:
            order_by = fields
        else:
            order_by = self.model._meta.get_latest_by
            if order_by and not isinstance(order_by, (tuple, list)):
                order_by = (order_by,)
        if order_by is None:
            raise ValueError(
                "earliest() and latest() require either fields as positional "
                "arguments or 'get_latest_by' in the model's Meta.",
            )
        obj = self._chain()
        obj.query.set_limits(high=1)
        obj.query.clear_ordering(force=True)
        obj.query.add_ordering(*order_by)
        return obj.aget()

    def aearliest(self, *fields):
        if self.query.is_sliced:
            raise TypeError("Cannot change a query once a slice has been taken.")
        return self._earliest(*fields)

    def alatest(self, *fields):
        """
        Return the latest object according to fields (if given) or by the
        model's Meta.get_latest_by.
        """
        if self.query.is_sliced:
            raise TypeError("Cannot change a query once a slice has been taken.")
        return self.reverse()._earliest(*fields)

    async def afirst(self):
        """Return the first object of a query or None if no match is found."""
        if self.ordered:
            queryset = self
        else:
            self._check_ordering_first_last_queryset_aggregation(method="first")
            queryset = self.order_by("pk")
        async for obj in queryset[:1]:
            return obj

    async def alast(self):
        """Return the last object of a query or None if no match is found."""
        if self.ordered:
            queryset = self.reverse()
        else:
            self._check_ordering_first_last_queryset_aggregation(method="last")
            queryset = self.order_by("-pk")
        async for obj in queryset[:1]:
            return obj

    async def ain_bulk(self, id_list=None, *, field_name="pk"):
        """
        Return a dictionary mapping each of the given IDs to the object with
        that ID. If `id_list` isn't provided, evaluate the entire QuerySet.
        """
        if self.query.is_sliced:
            raise TypeError("Cannot use 'limit' or 'offset' with in_bulk().")
        if id_list is not None and not id_list:
            return {}
        opts = self.model._meta
        unique_fields = [
            constraint.fields[0] for constraint in opts.total_unique_constraints if len(constraint.fields) == 1
        ]
        if (
            field_name != "pk"
            and not opts.get_field(field_name).unique
            and field_name not in unique_fields
            and self.query.distinct_fields != (field_name,)
        ):
            raise ValueError("in_bulk()'s field_name must be a unique field but %r isn't." % field_name)

        qs = self

        def get_obj(obj):
            return obj

        if issubclass(self._iterable_class, ModelIterable):
            get_key = operator.attrgetter(field_name)
        elif issubclass(self._iterable_class, ValuesIterable):
            if field_name not in self.query.values_select:
                qs = qs.values(field_name, *self.query.values_select)

                def get_obj(obj):  # noqa: F811
                    del obj[field_name]
                    return obj

            get_key = operator.itemgetter(field_name)
        elif issubclass(self._iterable_class, ValuesListIterable):
            try:
                field_index = self.query.values_select.index(field_name)
            except ValueError:
                field_index = 0
                if issubclass(self._iterable_class, NamedValuesListIterable):
                    kwargs = {"named": True}
                else:
                    kwargs = {}
                    get_obj = operator.itemgetter(slice(1, None))
                qs = qs.values_list(field_name, *self.query.values_select, **kwargs)
            get_key = operator.itemgetter(field_index)
        elif issubclass(self._iterable_class, FlatValuesListIterable):
            if self.query.values_select == (field_name,):
                get_key = get_obj
            else:
                qs = qs.values_list(field_name, *self.query.values_select)
                get_key = operator.itemgetter(0)
                get_obj = operator.itemgetter(1)
        else:
            raise TypeError(f"in_bulk() cannot be used with {self._iterable_class.__name__}.")

        if id_list is not None:
            filter_key = f"{field_name}__in"
            id_list = tuple(id_list)
            batch_size = async_connections[self.db].ops.bulk_batch_size([opts.pk], id_list)
            if batch_size and batch_size < len(id_list):
                rows = []
                for offset in range(0, len(id_list), batch_size):
                    batch = id_list[offset : offset + batch_size]
                    rows.extend([i async for i in qs.filter(**{filter_key: batch})])
            else:
                rows = [i async for i in qs.filter(**{filter_key: id_list})]
        else:
            rows = [i async for i in qs._chain()]
        return {get_key(obj): get_obj(obj) for obj in rows}

    async def _raw_delete(self, using):
        """
        Delete objects found from the given queryset in single direct SQL
        query. No signals are sent and there is no protection for cascades.
        """
        query = self.query.clone()
        query.__class__ = sql.DeleteQuery
        connection = async_connections[using]
        compiler = connection.ops.compiler(query.compiler)(query, connection, using)
        return await compiler.execute_sql(ROW_COUNT)

    _raw_delete.alters_data = True

    async def _update(self, values, returning_fields=None):
        """
        A version of update() that accepts field objects instead of field
        names. Used primarily for model saving and not intended for use by
        general code (it requires too much poking around at model internals to
        be useful at that level).
        """
        if self.query.is_sliced:
            raise TypeError("Cannot update a query once a slice has been taken.")
        query = self.query.chain(sql.UpdateQuery)
        query.add_update_fields(values)
        # Clear any annotations so that they won't be present in subqueries.
        query.annotations = {}
        self._result_cache = None
        connection = async_connections[self.db]
        compiler = connection.ops.compiler(query.compiler)(query, connection, self.db)
        if returning_fields is None:
            return await compiler.execute_sql(ROW_COUNT)
        return await compiler.execute_returning_sql(returning_fields)

    _update.alters_data = True
    _update.queryset_only = False

    # ── Hand-written async write operations ──────────────────────────────

    async def aupdate(self, **kwargs):
        """
        Update all elements in the current QuerySet, setting all the given
        fields to the appropriate values.
        """
        self._not_support_combined_queries("update")
        if self.query.is_sliced:
            raise TypeError("Cannot update a query once a slice has been taken.")
        self._for_write = True
        query = self.query.chain(sql.UpdateQuery)
        query.add_update_values(kwargs)

        # Inline annotations in order_by(), if possible.
        new_order_by = []
        for col in query.order_by:
            alias = col
            descending = False
            if isinstance(alias, str) and alias.startswith("-"):
                alias = alias.removeprefix("-")
                descending = True
            if annotation := query.annotations.get(alias):
                if getattr(annotation, "contains_aggregate", False):
                    raise exceptions.FieldError(f"Cannot update when ordering by an aggregate: {annotation}")
                if descending:
                    annotation = annotation.desc()
                new_order_by.append(annotation)
            else:
                new_order_by.append(col)
        query.order_by = tuple(new_order_by)

        # Clear SELECT clause as all annotation references were inlined by
        # add_update_values() already.
        query.clear_select_clause()
        connection = async_connections[self.db]
        compiler = connection.ops.compiler(query.compiler)(query, connection, self.db)
        async with async_mark_for_rollback_on_error(using=self.db):
            rows = await compiler.execute_sql(ROW_COUNT)
        self._result_cache = None
        return rows

    aupdate.alters_data = True

    async def acreate(self, **kwargs):
        """
        Create a new object with the given kwargs, saving it to the database
        and returning the created object. Fires pre_save/post_save signals.
        """
        self._for_write = True
        obj = self.model(**kwargs)
        await obj.asave(force_insert=True, using=self.db)
        return obj

    acreate.alters_data = True

    async def adelete(self):
        """
        Delete the records in the current QuerySet.

        Handles CASCADE and SET_NULL relationships via AsyncCollector.
        Skips pre_delete/post_delete signals.
        """
        from django_async_backend.db.models.deletion import AsyncCollector

        self._not_support_combined_queries("delete")
        if self.query.is_sliced:
            raise TypeError("Cannot use 'limit' or 'offset' with delete().")
        if self.query.distinct_fields:
            raise TypeError("Cannot call delete() after .distinct(*fields).")
        if self._fields is not None:
            raise TypeError("Cannot call delete() after .values() or .values_list()")

        del_query = self._chain()
        del_query._for_write = True
        del_query.query.select_for_update = False
        del_query.query.select_related = False
        del_query.query.clear_ordering(force=True)

        collector = AsyncCollector(using=del_query.db, origin=self)
        objs = [obj async for obj in del_query]
        await collector.acollect(objs)
        num_deleted, deleted_per_model = await collector.adelete()

        self._result_cache = None
        return num_deleted, deleted_per_model

    adelete.alters_data = True
    adelete.queryset_only = True

    async def aget_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, creating one if necessary.
        Return a tuple of (object, created), where created is a boolean
        specifying whether an object was created.
        """
        self._for_write = True
        try:
            return await self.aget(**kwargs), False
        except self.model.DoesNotExist:
            params = self._extract_model_params(defaults, **kwargs)
            try:
                async with async_atomic(using=self.db):
                    params = dict(resolve_callables(params))
                    return await self.acreate(**params), True
            except IntegrityError:
                try:
                    return await self.aget(**kwargs), False
                except self.model.DoesNotExist:
                    pass
                raise

    aget_or_create.alters_data = True

    async def aupdate_or_create(self, defaults=None, create_defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, updating one with defaults
        if it exists, otherwise create a new one.
        Return a tuple (object, created), where created is a boolean
        specifying whether an object was created.

        Simplified: does not use select_for_update or Model.save() for the
        update path. Instead updates fields directly via the queryset.
        """
        update_defaults = defaults or {}
        if create_defaults is None:
            create_defaults = update_defaults

        self._for_write = True
        async with async_atomic(using=self.db):
            try:
                obj = await self.aget(**kwargs)
                created = False
            except self.model.DoesNotExist:
                params = self._extract_model_params(create_defaults, **kwargs)
                try:
                    async with async_atomic(using=self.db):
                        params = dict(resolve_callables(params))
                        obj = await self.acreate(**params)
                        created = True
                except IntegrityError:
                    try:
                        obj = await self.aget(**kwargs)
                        created = False
                    except self.model.DoesNotExist:
                        raise

            if not created:
                update_kwargs = dict(resolve_callables(update_defaults))
                for k, v in update_kwargs.items():
                    setattr(obj, k, v)
                if update_kwargs:
                    await self.filter(pk=obj.pk).aupdate(**update_kwargs)

        return obj, created

    aupdate_or_create.alters_data = True

    async def abulk_update(self, objs, fields, batch_size=None):
        """
        Update the given fields in each of the given objects in the database.
        """
        if batch_size is not None and batch_size <= 0:
            raise ValueError("Batch size must be a positive integer.")
        if not fields:
            raise ValueError("Field names must be given to bulk_update().")
        objs = tuple(objs)
        if not all(obj._is_pk_set() for obj in objs):
            raise ValueError("All bulk_update() objects must have a primary key set.")
        opts = self.model._meta
        fields = [opts.get_field(name) for name in fields]
        if any(not f.concrete for f in fields):
            raise ValueError("bulk_update() can only be used with concrete fields.")
        all_pk_fields = set(opts.pk_fields)
        for parent in opts.all_parents:
            all_pk_fields.update(parent._meta.pk_fields)
        if any(f in all_pk_fields for f in fields):
            raise ValueError("bulk_update() cannot be used with primary key fields.")
        if not objs:
            return 0
        for obj in objs:
            obj._prepare_related_fields_for_save(operation_name="bulk_update", fields=fields)
        self._for_write = True
        connection = async_connections[self.db]
        max_batch_size = connection.ops.bulk_batch_size([opts.pk, opts.pk, *fields], objs)
        batch_size = min(batch_size, max_batch_size) if batch_size else max_batch_size
        requires_casting = connection.features.requires_casted_case_in_updates
        batches = (objs[i : i + batch_size] for i in range(0, len(objs), batch_size))
        updates = []
        for batch_objs in batches:
            update_kwargs = {}
            for field in fields:
                when_statements = []
                for obj in batch_objs:
                    attr = getattr(obj, field.attname)
                    if not hasattr(attr, "resolve_expression"):
                        attr = Value(attr, output_field=field)
                    when_statements.append(When(pk=obj.pk, then=attr))
                case_statement = Case(*when_statements, output_field=field)
                if requires_casting:
                    case_statement = Cast(case_statement, output_field=field)
                update_kwargs[field.attname] = case_statement
            updates.append(([obj.pk for obj in batch_objs], update_kwargs))
        rows_updated = 0
        queryset = self.using(self.db)
        async with async_atomic(using=self.db, savepoint=False):
            for pks, update_kwargs in updates:
                rows_updated += await queryset.filter(pk__in=pks).aupdate(**update_kwargs)
        return rows_updated

    abulk_update.alters_data = True

    async def aexists(self):
        """
        Return True if the QuerySet would have any results, False otherwise.
        """
        if self._result_cache is None:
            return await self.query.has_results(using=self.db)
        return bool(self._result_cache)

    async def _prefetch_related_objects(self):
        # This method can only be called once the result cache has been filled.
        await prefetch_related_objects(self._result_cache, *self._prefetch_related_lookups)
        self._prefetch_done = True

    async def acontains(self, obj):
        """
        Return True if the QuerySet contains the provided obj,
        False otherwise.
        """
        self._not_support_combined_queries("contains")
        if self._fields is not None:
            raise TypeError("Cannot call QuerySet.contains() after .values() or .values_list().")
        try:
            if obj._meta.concrete_model != self.model._meta.concrete_model:
                return False
        except AttributeError:
            raise TypeError("'obj' must be a model instance.")
        if not obj._is_pk_set():
            raise ValueError("QuerySet.contains() cannot be used on unsaved objects.")
        if self._result_cache is not None:
            return obj in self._result_cache
        return await self.filter(pk=obj.pk).aexists()

    async def aiterator(self, chunk_size=2000):
        """
        An async iterator over the results from applying this QuerySet to
        the database. Results are streamed without caching.
        """
        if chunk_size <= 0:
            raise ValueError("Chunk size must be strictly positive.")
        iterable = self._iterable_class(
            self,
            chunked_fetch=False,
            chunk_size=chunk_size,
        )
        async for obj in iterable:
            yield obj

    async def aexplain(self, *, format=None, **options):
        """
        Runs an EXPLAIN on the SQL query this QuerySet would perform, and
        returns the results.
        """
        return await self.query.explain(using=self.db, format=format, **options)

    def araw(self, raw_query, params=(), translations=None, using=None):
        """
        Return a RawQuerySet for performing raw SQL queries.

        The RawQuerySet supports async iteration via `async for`.
        """
        if using is None:
            using = self.db
        qs = RawQuerySet(
            raw_query,
            model=self.model,
            params=params,
            translations=translations,
            using=using,
        )
        qs._prefetch_related_lookups = self._prefetch_related_lookups[:]
        return qs

    def _values(self, *fields, **expressions):
        clone = self._chain()
        if expressions:
            # RemovedInDjango70Warning: When the deprecation ends, deindent as:
            # clone = clone.annotate(**expressions)
            with warnings.catch_warnings(action="ignore", category=RemovedInDjango70Warning):
                clone = clone.annotate(**expressions)
        clone._fields = fields
        clone.query.set_values(fields)
        return clone

    def values(self, *fields, **expressions):
        fields += tuple(expressions)
        clone = self._values(*fields, **expressions)
        clone._iterable_class = ValuesIterable
        return clone

    def values_list(self, *fields, flat=False, named=False):
        if flat and named:
            raise TypeError("'flat' and 'named' can't be used together.")
        if flat:
            if len(fields) > 1:
                raise TypeError("'flat' is not valid when values_list is called with more than one field.")
            if not fields:
                fields = [self.model._meta.concrete_fields[0].attname]

        field_names = {f: False for f in fields if not hasattr(f, "resolve_expression")}
        _fields = []
        expressions = {}
        counter = 1
        for field in fields:
            field_name = field
            expression = None
            if hasattr(field, "resolve_expression"):
                field_name = getattr(field, "default_alias", field.__class__.__name__.lower())
                expression = field
                # For backward compatibility reasons expressions are always
                # prefixed with the counter even if their default alias doesn't
                # collide with field names. Changing this logic could break
                # some usage of named=True.
                seen = True
            elif seen := field_names[field_name]:
                expression = F(field_name)
            if seen:
                field_name_prefix = field_name
                while (field_name := f"{field_name_prefix}{counter}") in field_names:
                    counter += 1
            if expression is not None:
                expressions[field_name] = expression
            field_names[field_name] = True
            _fields.append(field_name)

        clone = self._values(*_fields, **expressions)
        clone._iterable_class = (
            NamedValuesListIterable if named else FlatValuesListIterable if flat else ValuesListIterable
        )
        return clone

    def none(self):
        """Return an empty QuerySet."""
        clone = self._chain()
        clone.query.set_empty()
        return clone

    ##################################################################
    # PUBLIC METHODS THAT ALTER ATTRIBUTES AND RETURN A NEW QUERYSET #
    ##################################################################

    def all(self):
        """
        Return a new QuerySet that is a copy of the current one. This allows a
        QuerySet to proxy for a model manager in some cases.
        """
        return self._chain()

    def filter(self, *args, **kwargs):
        """
        Return a new QuerySet instance with the args ANDed to the existing
        set.
        """
        self._not_support_combined_queries("filter")
        return self._filter_or_exclude(False, args, kwargs)

    def exclude(self, *args, **kwargs):
        """
        Return a new QuerySet instance with NOT (args) ANDed to the existing
        set.
        """
        self._not_support_combined_queries("exclude")
        return self._filter_or_exclude(True, args, kwargs)

    def _filter_or_exclude(self, negate, args, kwargs):
        if (args or kwargs) and self.query.is_sliced:
            raise TypeError("Cannot filter a query once a slice has been taken.")
        clone = self._chain()
        if self._defer_next_filter:
            self._defer_next_filter = False
            clone._deferred_filter = negate, args, kwargs
        else:
            clone._filter_or_exclude_inplace(negate, args, kwargs)
        return clone

    def _filter_or_exclude_inplace(self, negate, args, kwargs):
        if invalid_kwargs := PROHIBITED_FILTER_KWARGS.intersection(kwargs):
            invalid_kwargs_str = ", ".join(f"'{k}'" for k in sorted(invalid_kwargs))
            raise TypeError(f"The following kwargs are invalid: {invalid_kwargs_str}")
        if negate:
            self._query.add_q(~Q(*args, **kwargs))
        else:
            self._query.add_q(Q(*args, **kwargs))

    def complex_filter(self, filter_obj):
        """
        Return a new QuerySet instance with filter_obj added to the filters.

        filter_obj can be a Q object or a dictionary of keyword lookup
        arguments.

        This exists to support framework features such as 'limit_choices_to',
        and usually it will be more natural to use other methods.
        """
        if isinstance(filter_obj, Q):
            clone = self._chain()
            clone.query.add_q(filter_obj)
            return clone
        return self._filter_or_exclude(False, args=(), kwargs=filter_obj)

    def _combinator_query(self, combinator, *other_qs, all=False):
        # Clone the query to inherit the select list and everything
        clone = self._chain()
        # Clear limits and ordering so they can be reapplied
        clone.query.clear_ordering(force=True)
        clone.query.clear_limits()
        clone.query.combined_queries = (
            self.query,
            *(qs.query for qs in other_qs),
        )
        clone.query.combinator = combinator
        clone.query.combinator_all = all
        return clone

    def union(self, *other_qs, all=False):
        # If the query is an EmptyQuerySet, combine all nonempty querysets.
        if isinstance(self, EmptyQuerySet):
            qs = [q for q in other_qs if not isinstance(q, EmptyQuerySet)]
            if not qs:
                return self
            if len(qs) == 1:
                return qs[0]
            return qs[0]._combinator_query("union", *qs[1:], all=all)
        if not other_qs:
            return self
        return self._combinator_query("union", *other_qs, all=all)

    def intersection(self, *other_qs):
        # If any query is an EmptyQuerySet, return it.
        if isinstance(self, EmptyQuerySet):
            return self
        for other in other_qs:
            if isinstance(other, EmptyQuerySet):
                return other
        return self._combinator_query("intersection", *other_qs)

    def difference(self, *other_qs):
        # If the query is an EmptyQuerySet, return it.
        if isinstance(self, EmptyQuerySet):
            return self
        return self._combinator_query("difference", *other_qs)

    def select_related(self, *fields):
        """
        Return a new QuerySet instance that will select related objects.

        If fields are specified, they must be ForeignKey fields and only those
        related objects are included in the selection.

        If select_related(None) is called, clear the list.
        """
        self._not_support_combined_queries("select_related")
        if self._fields is not None:
            raise TypeError("Cannot call select_related() after .values() or .values_list()")

        obj = self._chain()
        if fields == (None,):
            obj.query.select_related = False
        elif fields:
            obj.query.add_select_related(fields)
        else:
            obj.query.select_related = True
        return obj

    def prefetch_related(self, *lookups):
        """
        Return a new QuerySet instance that will prefetch the specified
        Many-To-One and Many-To-Many related objects when the QuerySet is
        evaluated.

        When prefetch_related(None) is called, clear the list of prefetch
        lookups.
        """
        self._not_support_combined_queries("prefetch_related")
        clone = self._chain()
        if lookups == (None,):
            clone._prefetch_related_lookups = ()
        else:
            for lookup in lookups:
                if isinstance(lookup, Prefetch):
                    lookup = lookup.prefetch_to
                    lookup = lookup.split(LOOKUP_SEP, 1)[0]
                if lookup in self.query._filtered_relations:
                    raise ValueError("prefetch_related() is not supported with FilteredRelation.")
            clone._prefetch_related_lookups = clone._prefetch_related_lookups + lookups
        return clone

    def annotate(self, *args, **kwargs):
        """
        Return a query set in which the returned objects have been annotated
        with extra data or aggregations.
        """
        self._not_support_combined_queries("annotate")
        return self._annotate(args, kwargs, select=True)

    def _annotate(self, args, kwargs, select=True):
        self._validate_values_are_expressions(args + tuple(kwargs.values()), method_name="annotate")
        annotations = {}
        for arg in args:
            # The default_alias property raises TypeError if default_alias
            # can't be set automatically or AttributeError if it isn't an
            # attribute.
            try:
                if arg.default_alias in kwargs:
                    raise ValueError(
                        "The named annotation '%s' conflicts with the "
                        "default name for another annotation." % arg.default_alias,
                    )
            except TypeError, AttributeError:
                raise TypeError("Complex annotations require an alias")
            annotations[arg.default_alias] = arg
        annotations.update(kwargs)

        clone = self._chain()
        names = self._fields
        if names is None:
            names = set(
                chain.from_iterable(
                    ((field.name, field.attname) if hasattr(field, "attname") else (field.name,))
                    for field in self.model._meta.get_fields()
                ),
            )

        for alias, annotation in annotations.items():
            if alias in names:
                raise ValueError("The annotation '%s' conflicts with a field on the model." % alias)
            if isinstance(annotation, FilteredRelation):
                clone.query.add_filtered_relation(annotation, alias)
            else:
                clone.query.add_annotation(
                    annotation,
                    alias,
                    select=select,
                )
        for alias, annotation in clone.query.annotations.items():
            if alias in annotations and annotation.contains_aggregate:
                if clone._fields is None:
                    clone.query.group_by = True
                else:
                    clone.query.set_group_by()
                break

        return clone

    def order_by(self, *field_names):
        """Return a new QuerySet instance with the ordering changed."""
        if self.query.is_sliced:
            raise TypeError("Cannot reorder a query once a slice has been taken.")
        obj = self._chain()
        obj.query.clear_ordering(force=True, clear_default=False)
        obj.query.add_ordering(*field_names)
        return obj

    def distinct(self, *field_names):
        """
        Return a new QuerySet instance that will select only distinct results.
        """
        self._not_support_combined_queries("distinct")
        if self.query.is_sliced:
            raise TypeError("Cannot create distinct fields once a slice has been taken.")
        obj = self._chain()
        obj.query.add_distinct_fields(*field_names)
        return obj

    def extra(
        self,
        select=None,
        where=None,
        params=None,
        tables=None,
        order_by=None,
        select_params=None,
    ):
        """Add extra SQL fragments to the query."""
        self._not_support_combined_queries("extra")
        if self.query.is_sliced:
            raise TypeError("Cannot change a query once a slice has been taken.")
        clone = self._chain()
        clone.query.add_extra(select, select_params, where, params, tables, order_by)
        return clone

    def reverse(self):
        """Reverse the ordering of the QuerySet."""
        if self.query.is_sliced:
            raise TypeError("Cannot reverse a query once a slice has been taken.")
        clone = self._chain()
        clone.query.standard_ordering = not clone.query.standard_ordering
        return clone

    def defer(self, *fields):
        """
        Defer the loading of data for certain fields until they are accessed.
        """
        self._not_support_combined_queries("defer")
        if self._fields is not None:
            raise TypeError("Cannot call defer() after .values() or .values_list()")
        clone = self._chain()
        if fields == (None,):
            clone.query.clear_deferred_loading()
        else:
            clone.query.add_deferred_loading(fields)
        return clone

    def only(self, *fields):
        """
        Essentially, the opposite of defer(). Only the fields passed into this
        method and that are not already specified as deferred are loaded
        immediately when the queryset is evaluated.
        """
        self._not_support_combined_queries("only")
        if self._fields is not None:
            raise TypeError("Cannot call only() after .values() or .values_list()")
        if fields == (None,):
            raise TypeError("Cannot pass None as an argument to only().")
        for field in fields:
            field = field.split(LOOKUP_SEP, 1)[0]
            if field in self.query._filtered_relations:
                raise ValueError("only() is not supported with FilteredRelation.")
        clone = self._chain()
        clone.query.add_immediate_loading(fields)
        return clone

    def select_for_update(self, nowait=False, skip_locked=False, of=(), no_key=False):
        """
        Return a new QuerySet instance that will select objects with a
        FOR UPDATE lock.
        """
        if nowait and skip_locked:
            raise ValueError("The nowait option cannot be used with skip_locked.")
        obj = self._chain()
        obj._for_write = True
        obj.query.select_for_update = True
        obj.query.select_for_update_nowait = nowait
        obj.query.select_for_update_skip_locked = skip_locked
        obj.query.select_for_update_of = of
        obj.query.select_for_no_key_update = no_key
        return obj

    def alias(self, *args, **kwargs):
        """
        Return a query set with added aliases for extra data or aggregations.
        """
        self._not_support_combined_queries("alias")
        return self._annotate(args, kwargs, select=False)

    def dates(self, field_name, kind, order="ASC"):
        """
        Return a list of date objects representing all available dates for
        the given field_name, scoped to 'kind'.
        """
        if kind not in ("year", "month", "week", "day"):
            raise ValueError("'kind' must be one of 'year', 'month', 'week', or 'day'.")
        if order not in ("ASC", "DESC"):
            raise ValueError("'order' must be either 'ASC' or 'DESC'.")
        return (
            self.annotate(
                datefield=Trunc(field_name, kind, output_field=DateField()),
                plain_field=F(field_name),
            )
            .values_list("datefield", flat=True)
            .distinct()
            .filter(plain_field__isnull=False)
            .order_by(("-" if order == "DESC" else "") + "datefield")
        )

    def datetimes(self, field_name, kind, order="ASC", tzinfo=None):
        """
        Return a list of datetime objects representing all available
        datetimes for the given field_name, scoped to 'kind'.
        """
        if kind not in (
            "year",
            "month",
            "week",
            "day",
            "hour",
            "minute",
            "second",
        ):
            raise ValueError("'kind' must be one of 'year', 'month', 'week', 'day', 'hour', 'minute', or 'second'.")
        if order not in ("ASC", "DESC"):
            raise ValueError("'order' must be either 'ASC' or 'DESC'.")
        if settings.USE_TZ:
            if tzinfo is None:
                tzinfo = timezone.get_current_timezone()
        else:
            tzinfo = None
        return (
            self.annotate(
                datetimefield=Trunc(
                    field_name,
                    kind,
                    output_field=DateTimeField(),
                    tzinfo=tzinfo,
                ),
                plain_field=F(field_name),
            )
            .values_list("datetimefield", flat=True)
            .distinct()
            .filter(plain_field__isnull=False)
            .order_by(("-" if order == "DESC" else "") + "datetimefield")
        )

    def using(self, alias):
        """Select which database this QuerySet should execute against."""
        clone = self._chain()
        clone._db = alias
        return clone

    ###################################
    # PUBLIC INTROSPECTION ATTRIBUTES #
    ###################################

    @property
    def ordered(self):
        """
        Return True if the QuerySet is ordered -- i.e. has an order_by()
        clause or a default ordering on the model (or is empty).
        """
        if isinstance(self, EmptyQuerySet):
            return True
        if (
            self.query.extra_order_by
            or self.query.order_by
            or (
                self.query.default_ordering
                and self.query.get_meta().ordering
                and
                # A default ordering doesn't affect GROUP BY queries.
                not self.query.group_by
            )
        ):
            return True
        return False

    @property
    def db(self):
        """Return the database used if this query is executed now."""
        if self._for_write:
            return self._db or router.db_for_write(self.model, **self._hints)
        return self._db or router.db_for_read(self.model, **self._hints)

    ###################
    # PRIVATE METHODS #
    ###################

    async def _insert(
        self,
        objs,
        fields,
        returning_fields=None,
        raw=False,
        using=None,
        on_conflict=None,
        update_fields=None,
        unique_fields=None,
    ):
        """
        Insert a new record for the given model. This provides an interface to
        the InsertQuery class and is how Model.save() is implemented.
        """
        self._for_write = True
        if using is None:
            using = self.db
        query = sql.InsertQuery(
            self.model,
            on_conflict=on_conflict,
            update_fields=update_fields,
            unique_fields=unique_fields,
        )
        query.insert_values(fields, objs, raw=raw)
        connection = async_connections[using]
        compiler = connection.ops.compiler(query.compiler)(query, connection, using)
        return await compiler.execute_sql(returning_fields)

    _insert.alters_data = True
    _insert.queryset_only = False

    async def _batched_insert(
        self,
        objs,
        fields,
        batch_size,
        on_conflict=None,
        update_fields=None,
        unique_fields=None,
    ):
        """
        Helper method for bulk_create() to insert objs one batch at a time.
        """
        connection = async_connections[self.db]
        ops = connection.ops
        max_batch_size = max(ops.bulk_batch_size(fields, objs), 1)
        batch_size = min(batch_size, max_batch_size) if batch_size else max_batch_size
        inserted_rows = []
        returning_fields = (
            self.model._meta.db_returning_fields
            if (
                connection.features.can_return_rows_from_bulk_insert
                and (on_conflict is None or on_conflict == OnConflict.UPDATE)
            )
            else None
        )
        batches = [objs[i : i + batch_size] for i in range(0, len(objs), batch_size)]
        if len(batches) > 1:
            context = async_atomic(using=self.db, savepoint=False)
        else:
            context = nullcontext()
        async with context:
            for item in batches:
                inserted_rows.extend(
                    await self._insert(
                        item,
                        fields=fields,
                        using=self.db,
                        on_conflict=on_conflict,
                        update_fields=update_fields,
                        unique_fields=unique_fields,
                        returning_fields=returning_fields,
                    ),
                )
        return inserted_rows

    async def abulk_create(
        self,
        objs,
        batch_size=None,
        ignore_conflicts=False,
        update_conflicts=False,
        update_fields=None,
        unique_fields=None,
    ):
        """
        Insert each of the instances into the database. Does not call save()
        on each of the instances, does not send any pre/post_save signals,
        and does not set the primary key attribute if it is an autoincrement
        field (except if features.can_return_rows_from_bulk_insert=True).
        Multi-table models are not supported.
        """
        if batch_size is not None and batch_size <= 0:
            raise ValueError("Batch size must be a positive integer.")
        for parent in self.model._meta.all_parents:
            if parent._meta.concrete_model is not self.model._meta.concrete_model:
                raise ValueError("Can't bulk create a multi-table inherited model")
        if not objs:
            return objs

        opts = self.model._meta
        if unique_fields:
            unique_fields = [opts.get_field(opts.pk.name if name == "pk" else name) for name in unique_fields]
        if update_fields:
            update_fields = [opts.get_field(name) for name in update_fields]
        on_conflict = self._check_bulk_create_options(
            ignore_conflicts,
            update_conflicts,
            update_fields,
            unique_fields,
        )
        self._for_write = True
        fields = [f for f in opts.concrete_fields if not f.generated]
        objs = list(objs)
        objs_with_pk, objs_without_pk = self._prepare_for_bulk_create(objs)

        async with async_atomic(using=self.db, savepoint=False):
            if objs_with_pk:
                returned_columns = await self._batched_insert(
                    objs_with_pk,
                    fields,
                    batch_size,
                    on_conflict=on_conflict,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
                for obj_with_pk, results in zip(objs_with_pk, returned_columns):
                    for result, field in zip(results, opts.db_returning_fields):
                        if field != opts.pk:
                            setattr(obj_with_pk, field.attname, result)
                for obj_with_pk in objs_with_pk:
                    obj_with_pk._state.adding = False
                    obj_with_pk._state.db = self.db

            if objs_without_pk:
                insert_fields = [f for f in fields if not isinstance(f, AutoField)]
                returned_columns = await self._batched_insert(
                    objs_without_pk,
                    insert_fields,
                    batch_size,
                    on_conflict=on_conflict,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
                connection = async_connections[self.db]
                if connection.features.can_return_rows_from_bulk_insert and on_conflict is None:
                    assert len(returned_columns) == len(objs_without_pk)
                for obj_without_pk, results in zip(objs_without_pk, returned_columns):
                    for result, field in zip(results, opts.db_returning_fields):
                        setattr(obj_without_pk, field.attname, result)
                    obj_without_pk._state.adding = False
                    obj_without_pk._state.db = self.db

        return objs

    abulk_create.alters_data = True

    def _chain(self):
        """
        Return a copy of the current QuerySet that's ready for another
        operation.
        """
        obj = self._clone()
        if obj._sticky_filter:
            obj.query.filter_is_sticky = True
            obj._sticky_filter = False
        return obj

    def _clone(self):
        """
        Return a copy of the current QuerySet. A lightweight alternative
        to deepcopy().
        """
        c = self.__class__(
            model=self.model,
            query=self.query.chain(),
            using=self._db,
            hints=self._hints,
        )
        c._sticky_filter = self._sticky_filter
        c._for_write = self._for_write
        c._prefetch_related_lookups = self._prefetch_related_lookups[:]
        c._known_related_objects = self._known_related_objects
        c._iterable_class = self._iterable_class
        c._fields = self._fields
        return c

    async def _fetch_all(self):
        if self._result_cache is None:
            self._result_cache = list([i async for i in self._iterable_class(self)])
        if self._prefetch_related_lookups and not self._prefetch_done:
            await self._prefetch_related_objects()

    def _next_is_sticky(self):
        """
        Indicate that the next filter call and the one following that should
        be treated as a single filter. This is only important when it comes to
        determining when to reuse tables for many-to-many filters. Required so
        that we can filter naturally on the results of related managers.

        This doesn't return a clone of the current QuerySet (it returns
        "self"). The method is only used internally and should be immediately
        followed by a filter() that does create a clone.
        """
        self._sticky_filter = True
        return self

    def _merge_sanity_check(self, other):
        """Check that two QuerySet classes may be merged."""
        if self._fields is not None and (
            set(self.query.values_select) != set(other.query.values_select)
            or set(self.query.extra_select) != set(other.query.extra_select)
            or set(self.query.annotation_select) != set(other.query.annotation_select)
        ):
            raise TypeError("Merging '%s' classes must involve the same values in each case." % self.__class__.__name__)

    def _merge_known_related_objects(self, other):
        """
        Keep track of all known related objects from either QuerySet instance.
        """
        for field, objects in other._known_related_objects.items():
            self._known_related_objects.setdefault(field, {}).update(objects)

    def resolve_expression(self, *args, **kwargs):
        query = self.query.resolve_expression(*args, **kwargs)
        query._db = self._db
        return query

    resolve_expression.queryset_only = True

    def _add_hints(self, **hints):
        """
        Update hinting information for use by routers. Add new key/values or
        overwrite existing key/values.
        """
        self._hints.update(hints)

    def _has_filters(self):
        """
        Check if this QuerySet has any filtering going on. This isn't
        equivalent with checking if all objects are present in results, for
        example, qs[1:]._has_filters() -> False.
        """
        return self.query.has_filters()

    @staticmethod
    def _validate_values_are_expressions(values, method_name):
        invalid_args = sorted(str(arg) for arg in values if not hasattr(arg, "resolve_expression"))
        if invalid_args:
            raise TypeError(
                "QuerySet.%s() received non-expression(s): %s."
                % (
                    method_name,
                    ", ".join(invalid_args),
                ),
            )

    def _not_support_combined_queries(self, operation_name):
        if self.query.combinator:
            raise NotSupportedError(
                "Calling QuerySet.%s() after %s() is not supported." % (operation_name, self.query.combinator),
            )

    def _check_operator_queryset(self, other, operator_):
        if self.query.combinator or other.query.combinator:
            raise TypeError(f"Cannot use {operator_} operator with combined queryset.")

    def _check_ordering_first_last_queryset_aggregation(self, method):
        if (
            isinstance(self.query.group_by, tuple)
            # Raise if the pk fields are not in the group_by.
            and self.model._meta.pk not in {col.output_field for col in self.query.group_by}
            and set(self.model._meta.pk_fields).difference({col.target for col in self.query.group_by})
        ):
            raise TypeError(
                f"Cannot use QuerySet.{method}() on an unordered queryset performing "
                f"aggregation. Add an ordering with order_by().",
            )


class InstanceCheckMeta(type):
    def __instancecheck__(self, instance):
        return isinstance(instance, QuerySet) and instance.query.is_empty()


class EmptyQuerySet(metaclass=InstanceCheckMeta):
    """
    Marker class to checking if a queryset is empty by .none():
        isinstance(qs.none(), EmptyQuerySet) -> True
    """

    def __init__(self, *args, **kwargs):
        raise TypeError("EmptyQuerySet can't be instantiated")


class RawQuerySet:
    """
    Provide an iterator which converts the results of raw SQL queries into
    annotated model instances.
    """

    def __init__(
        self,
        raw_query,
        model=None,
        query=None,
        params=(),
        translations=None,
        using=None,
        hints=None,
    ):
        self.raw_query = raw_query
        self.model = model
        self._db = using
        self._hints = hints or {}
        self.query = query or async_sql.RawQuery(sql=raw_query, using=self.db, params=params)
        self.params = params
        self.translations = translations or {}
        self._result_cache = None
        self._prefetch_related_lookups = ()
        self._prefetch_done = False

    def resolve_model_init_order(self):
        """Resolve the init field names and value positions."""
        converter = async_connections[self.db].introspection.identifier_converter
        model_init_fields = [field for column_name, field in self.model_fields.items() if column_name in self.columns]
        annotation_fields = [
            (column, pos) for pos, column in enumerate(self.columns) if column not in self.model_fields
        ]
        model_init_order = [self.columns.index(converter(f.column)) for f in model_init_fields]
        model_init_names = [f.attname for f in model_init_fields]
        return model_init_names, model_init_order, annotation_fields

    async def aresolve_model_init_order(self):
        """Async version of resolve_model_init_order."""
        converter = async_connections[self.db].introspection.identifier_converter
        columns = await self.aget_columns()
        model_init_fields = [field for column_name, field in self.model_fields.items() if column_name in columns]
        annotation_fields = [(column, pos) for pos, column in enumerate(columns) if column not in self.model_fields]
        model_init_order = [columns.index(converter(f.column)) for f in model_init_fields]
        model_init_names = [f.attname for f in model_init_fields]
        return model_init_names, model_init_order, annotation_fields

    async def aget_columns(self):
        """Async version of columns property. Executes query to get column info."""
        columns = await self.query.aget_columns()
        for query_name, model_name in self.translations.items():
            try:
                index = columns.index(query_name)
            except ValueError:
                pass
            else:
                columns[index] = model_name
        return columns

    def prefetch_related(self, *lookups):
        """Same as QuerySet.prefetch_related()"""
        clone = self._clone()
        if lookups == (None,):
            clone._prefetch_related_lookups = ()
        else:
            clone._prefetch_related_lookups = clone._prefetch_related_lookups + lookups
        return clone

    def _prefetch_related_objects(self):
        prefetch_related_objects(self._result_cache, *self._prefetch_related_lookups)
        self._prefetch_done = True

    def _clone(self):
        """Same as QuerySet._clone()"""
        c = self.__class__(
            self.raw_query,
            model=self.model,
            query=self.query,
            params=self.params,
            translations=self.translations,
            using=self._db,
            hints=self._hints,
        )
        c._prefetch_related_lookups = self._prefetch_related_lookups[:]
        return c

    def _fetch_all(self):
        if self._result_cache is None:
            self._result_cache = list(self.iterator())
        if self._prefetch_related_lookups and not self._prefetch_done:
            self._prefetch_related_objects()

    def __len__(self):
        self._fetch_all()
        return len(self._result_cache)

    def __bool__(self):
        self._fetch_all()
        return bool(self._result_cache)

    def __iter__(self):
        self._fetch_all()
        return iter(self._result_cache)

    def __aiter__(self):
        async def generator():
            await self._afetch_all()
            for item in self._result_cache:
                yield item

        return generator()

    async def _afetch_all(self):
        if self._result_cache is None:
            self._result_cache = [obj async for obj in RawModelIterable(self)]
        if self._prefetch_related_lookups and not self._prefetch_done:
            self._prefetch_related_objects()

    def iterator(self):
        yield from RawModelIterable(self)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.query)

    def __getitem__(self, k):
        return list(self)[k]

    @property
    def db(self):
        """Return the database used if this query is executed now."""
        return self._db or router.db_for_read(self.model, **self._hints)

    def using(self, alias):
        """Select the database this RawQuerySet should execute against."""
        return RawQuerySet(
            self.raw_query,
            model=self.model,
            query=self.query.chain(using=alias),
            params=self.params,
            translations=self.translations,
            using=alias,
        )

    @cached_property
    def columns(self):
        """
        A list of model field names in the order they'll appear in the
        query results.
        """
        columns = self.query.get_columns()
        # Adjust any column names which don't match field names
        for query_name, model_name in self.translations.items():
            # Ignore translations for nonexistent column names
            try:
                index = columns.index(query_name)
            except ValueError:
                pass
            else:
                columns[index] = model_name
        return columns

    @cached_property
    def model_fields(self):
        """A dict mapping column names to model field names."""
        converter = async_connections[self.db].introspection.identifier_converter
        return {
            converter(field.column): field
            for field in self.model._meta.fields
            # Fields with None "column" should be ignored
            # (e.g. CompositePrimaryKey).
            if field.column
        }


class Prefetch:
    def __init__(self, lookup, queryset=None, to_attr=None):
        # `prefetch_through` is the path we traverse to perform the prefetch.
        self.prefetch_through = lookup
        # `prefetch_to` is the path to the attribute that stores the result.
        self.prefetch_to = lookup
        if queryset is not None and (
            isinstance(queryset, RawQuerySet)
            or (hasattr(queryset, "_iterable_class") and not issubclass(queryset._iterable_class, ModelIterable))
        ):
            raise ValueError("Prefetch querysets cannot use raw(), values(), and values_list().")
        if to_attr:
            self.prefetch_to = LOOKUP_SEP.join(lookup.split(LOOKUP_SEP)[:-1] + [to_attr])

        self.queryset = queryset
        self.to_attr = to_attr

    def __getstate__(self):
        obj_dict = self.__dict__.copy()
        if self.queryset is not None:
            queryset = self.queryset._chain()
            # Prevent the QuerySet from being evaluated
            queryset._result_cache = []
            queryset._prefetch_done = True
            obj_dict["queryset"] = queryset
        return obj_dict

    def add_prefix(self, prefix):
        self.prefetch_through = prefix + LOOKUP_SEP + self.prefetch_through
        self.prefetch_to = prefix + LOOKUP_SEP + self.prefetch_to

    def get_current_prefetch_to(self, level):
        return LOOKUP_SEP.join(self.prefetch_to.split(LOOKUP_SEP)[: level + 1])

    def get_current_to_attr(self, level):
        parts = self.prefetch_to.split(LOOKUP_SEP)
        to_attr = parts[level]
        as_attr = self.to_attr and level == len(parts) - 1
        return to_attr, as_attr

    def get_current_querysets(self, level):
        if self.get_current_prefetch_to(level) == self.prefetch_to and self.queryset is not None:
            return [self.queryset]
        return None

    def __eq__(self, other):
        if not isinstance(other, Prefetch):
            return NotImplemented
        return self.prefetch_to == other.prefetch_to

    def __hash__(self):
        return hash((self.__class__, self.prefetch_to))


def normalize_prefetch_lookups(lookups, prefix=None):
    """Normalize lookups into Prefetch objects."""
    ret = []
    for lookup in lookups:
        if not isinstance(lookup, Prefetch):
            lookup = Prefetch(lookup)
        if prefix:
            lookup.add_prefix(prefix)
        ret.append(lookup)
    return ret


async def prefetch_related_objects(model_instances, *related_lookups):
    """
    Populate prefetched object caches for an iterable of model instances based
    on the lookups/Prefetch instances given.

    Independent prefetch lookups are executed concurrently via asyncio.gather()
    at every level of the lookup tree. For example, with::

        prefetch_related("author__country", "author__publisher", "tags")

    Level 0 runs "author" and "tags" concurrently, then level 1 runs
    "country" and "publisher" concurrently.
    """
    if not model_instances:
        return

    done_queries = {}
    auto_lookups = set()
    followed_descriptors = set()

    all_lookups = normalize_prefetch_lookups(reversed(related_lookups))

    async def _process_level(lookups, obj_list, level):
        """Process one level of a group of lookups that share the same attr
        at this level, then recurse for deeper levels with parallel fan-out."""
        if not lookups or not obj_list:
            return []

        # All lookups share the same through_attr at this level.
        representative = lookups[0]
        through_attrs = representative.prefetch_through.split(LOOKUP_SEP)
        through_attr = through_attrs[level]
        prefetch_to = representative.get_current_prefetch_to(level)

        additional_from_level = []

        if prefetch_to in done_queries:
            obj_list = done_queries[prefetch_to]
        else:
            # Prepare objects
            good_objects = True
            for obj in obj_list:
                if not hasattr(obj, "_prefetched_objects_cache"):
                    try:
                        obj._prefetched_objects_cache = {}
                    except AttributeError, TypeError:
                        good_objects = False
                        break
            if not good_objects:
                return []

            first_obj = next(iter(obj_list))
            to_attr = representative.get_current_to_attr(level)[0]
            prefetcher, descriptor, attr_found, is_fetched = get_prefetcher(
                first_obj,
                through_attr,
                to_attr,
            )

            if not attr_found:
                raise AttributeError(
                    "Cannot find '%s' on %s object, '%s' is an invalid "
                    "parameter to prefetch_related()"
                    % (through_attr, first_obj.__class__.__name__, representative.prefetch_through),
                )

            # Check if terminal lookups have a valid prefetcher
            for lk in lookups:
                lk_attrs = lk.prefetch_through.split(LOOKUP_SEP)
                if level == len(lk_attrs) - 1 and prefetcher is None:
                    raise ValueError(
                        "'%s' does not resolve to an item that supports "
                        "prefetching - this is an invalid parameter to "
                        "prefetch_related()." % lk.prefetch_through,
                    )

            obj_to_fetch = None
            if prefetcher is not None:
                obj_to_fetch = [obj for obj in obj_list if not is_fetched(obj)]

            if obj_to_fetch:
                obj_list, additional_lookups = await prefetch_one_level(
                    obj_to_fetch,
                    prefetcher,
                    representative,
                    level,
                )
                if not (
                    prefetch_to in done_queries
                    and representative in auto_lookups
                    and descriptor in followed_descriptors
                ):
                    done_queries[prefetch_to] = obj_list
                    new_lookups = normalize_prefetch_lookups(
                        reversed(additional_lookups),
                        prefetch_to,
                    )
                    auto_lookups.update(new_lookups)
                    additional_from_level.extend(new_lookups)
                followed_descriptors.add(descriptor)
            else:
                # Traverse through already-fetched attribute
                new_obj_list = []
                for obj in obj_list:
                    if through_attr in getattr(obj, "_prefetched_objects_cache", ()):
                        new_obj = list(obj._prefetched_objects_cache.get(through_attr))
                    else:
                        try:
                            new_obj = getattr(obj, through_attr)
                        except exceptions.ObjectDoesNotExist:
                            continue
                    if new_obj is None:
                        continue
                    if isinstance(new_obj, list):
                        new_obj_list.extend(new_obj)
                    else:
                        new_obj_list.append(new_obj)
                obj_list = new_obj_list

        # Collect lookups that go deeper (have more levels beyond this one)
        deeper = [lk for lk in lookups if len(lk.prefetch_through.split(LOOKUP_SEP)) > level + 1]

        if deeper and obj_list:
            # Re-group by next-level attr and recurse with parallel fan-out
            next_groups: dict[str, list] = {}
            for lk in deeper:
                next_attr = lk.prefetch_through.split(LOOKUP_SEP)[level + 1]
                next_groups.setdefault(next_attr, []).append(lk)

            if len(next_groups) == 1:
                additional_from_level.extend(
                    await _process_level(
                        next(iter(next_groups.values())),
                        obj_list,
                        level + 1,
                    ),
                )
            else:
                results = await asyncio.gather(
                    *[_process_level(g, obj_list, level + 1) for g in next_groups.values()],
                )
                for r in results:
                    additional_from_level.extend(r)

        return additional_from_level

    while all_lookups:
        # Group lookups by first through_attr — independent groups run in parallel.
        groups: dict[str, list] = {}
        for lookup in all_lookups:
            if lookup.prefetch_to in done_queries:
                if lookup.queryset is not None:
                    raise ValueError(
                        "'%s' lookup was already seen with a different queryset. "
                        "You may need to adjust the ordering of your lookups." % lookup.prefetch_to,
                    )
                continue
            first_attr = lookup.prefetch_through.split(LOOKUP_SEP)[0]
            groups.setdefault(first_attr, []).append(lookup)

        all_lookups = []

        if not groups:
            break
        elif len(groups) == 1:
            only_group = next(iter(groups.values()))
            all_lookups.extend(await _process_level(only_group, model_instances, 0))
        else:
            results = await asyncio.gather(
                *[_process_level(g, model_instances, 0) for g in groups.values()],
            )
            for additional in results:
                all_lookups.extend(additional)


def get_prefetcher(instance, through_attr, to_attr):
    """
    For the attribute 'through_attr' on the given instance, find
    an object that has a get_prefetch_querysets().
    Return a 4 tuple containing:
    (the object with get_prefetch_querysets (or None),
     the descriptor object representing this relationship (or None),
     a boolean that is False if the attribute was not found at all,
     a function that takes an instance and returns a boolean that is True if
     the attribute has already been fetched for that instance)
    """

    def is_to_attr_fetched(model, to_attr):
        # Special case cached_property instances because hasattr() triggers
        # attribute computation and assignment.
        if isinstance(getattr(model, to_attr, None), cached_property):

            def has_cached_property(instance):
                return to_attr in instance.__dict__

            return has_cached_property

        def has_to_attr_attribute(instance):
            return hasattr(instance, to_attr)

        return has_to_attr_attribute

    prefetcher = None
    is_fetched = is_to_attr_fetched(instance.__class__, to_attr)

    # For singly related objects, we have to avoid getting the attribute
    # from the object, as this will trigger the query. So we first try
    # on the class, in order to get the descriptor object.
    rel_obj_descriptor = getattr(instance.__class__, through_attr, None)
    if rel_obj_descriptor is None:
        attr_found = hasattr(instance, through_attr)
    else:
        attr_found = True
        if rel_obj_descriptor:
            # singly related object, descriptor object has the
            # get_prefetch_querysets() method.
            if hasattr(rel_obj_descriptor, "get_prefetch_querysets"):
                prefetcher = rel_obj_descriptor
                # If to_attr is set, check if the value has already been set,
                # which is done with has_to_attr_attribute(). Do not use the
                # method from the descriptor, as the cache_name it defines
                # checks the field name, not the to_attr value.
                if through_attr == to_attr:
                    is_fetched = rel_obj_descriptor.is_cached
            else:
                # descriptor doesn't support prefetching, so we go ahead and
                # get the attribute on the instance rather than the class to
                # support many related managers
                rel_obj = getattr(instance, through_attr)
                if hasattr(rel_obj, "get_prefetch_querysets"):
                    prefetcher = rel_obj
                if through_attr == to_attr:

                    def in_prefetched_cache(instance):
                        return through_attr in instance._prefetched_objects_cache

                    is_fetched = in_prefetched_cache
    return prefetcher, rel_obj_descriptor, attr_found, is_fetched


async def _prefetch_fk_queryset(instances, prefetcher, lookup, level, *, forward=False):
    """Build and evaluate an FK prefetch queryset without sync iteration.

    Django's get_prefetch_querysets() for FK relations iterates the queryset
    synchronously to set cached FK values. We replicate that logic here
    with async evaluation, for both forward and reverse FK.
    """
    from django.db.models.fields.related_descriptors import _filter_prefetch_queryset
    from django.db.models.manager import BaseManager

    field = prefetcher.field

    current_querysets = lookup.get_current_querysets(level)
    if current_querysets:
        rel_qs = current_querysets[0]
    elif forward:
        # Forward FK: use our async QuerySet on the target model.
        target_model = field.remote_field.model
        db = router.db_for_read(target_model, instance=instances[0])
        rel_qs = QuerySet(model=target_model, using=db)
    else:
        # Reverse FK: use the base manager's queryset.
        rel_qs = BaseManager.get_queryset(prefetcher)

    rel_qs._add_hints(instance=instances[0])
    if hasattr(prefetcher, "db"):
        rel_qs = rel_qs.using(rel_qs._db or prefetcher.db)

    if forward:
        rel_obj_attr = field.get_foreign_related_value
        instance_attr = field.get_local_related_value
        single = True
        cache_name = field.cache_name

        # Filter by FK values from instances.
        instances_dict = {instance_attr(inst): inst for inst in instances}
        fk_values = [v[0] for v in instances_dict if v[0] is not None]
        target_field = field.foreign_related_fields[0]
        rel_qs = rel_qs.filter(**{f"{target_field.name}__in": fk_values})
        rel_qs.query.clear_ordering()
    else:
        rel_obj_attr = field.get_local_related_value
        instance_attr = field.get_foreign_related_value
        single = False
        cache_name = field.remote_field.cache_name

        instances_dict = {instance_attr(inst): inst for inst in instances}
        rel_qs = _filter_prefetch_queryset(rel_qs, field.name, instances)

    if hasattr(rel_qs, "_fetch_all") and rel_qs._result_cache is None:
        await rel_qs._fetch_all()

    # Set cached FK values on related objects (mirrors Django's sync loop).
    remote_field = field.remote_field
    for rel_obj in rel_qs._result_cache:
        if forward:
            if not remote_field.multiple:
                instance = instances_dict.get(rel_obj_attr(rel_obj))
                if instance is not None:
                    remote_field.set_cached_value(rel_obj, instance)
        elif not field.is_cached(rel_obj):
            instance = instances_dict.get(rel_obj_attr(rel_obj))
            if instance is not None:
                field.set_cached_value(rel_obj, instance)

    return rel_qs, rel_obj_attr, instance_attr, single, cache_name, False


async def prefetch_one_level(instances, prefetcher, lookup, level):
    """
    Helper function for prefetch_related_objects().

    Run prefetches on all instances using the prefetcher object,
    assigning results to relevant caches in instance.

    Return the prefetched objects along with any additional prefetches that
    must be done due to prefetch_related lookups found from default managers.
    """
    # Django's get_prefetch_querysets() iterates the queryset synchronously
    # for FK relations (to set cached reverse FK values). M2M relations
    # don't iterate inline. We handle FK prefetchers manually to avoid
    # the sync iteration, and use the generic interface for everything else.
    from django.db.models.fields.related_descriptors import ForwardManyToOneDescriptor
    from django.db.models.manager import BaseManager

    is_reverse_fk = (
        isinstance(prefetcher, BaseManager) and hasattr(prefetcher, "field") and not hasattr(prefetcher, "through")
    )
    is_forward_fk = isinstance(prefetcher, ForwardManyToOneDescriptor)

    if is_reverse_fk:
        # Reverse FK manager — build queryset manually to avoid sync iteration
        rel_qs, rel_obj_attr, instance_attr, single, cache_name, is_descriptor = await _prefetch_fk_queryset(
            instances,
            prefetcher,
            lookup,
            level,
        )
    elif is_forward_fk:
        # Forward FK descriptor — also iterates sync internally
        rel_qs, rel_obj_attr, instance_attr, single, cache_name, is_descriptor = await _prefetch_fk_queryset(
            instances,
            prefetcher,
            lookup,
            level,
            forward=True,
        )
    else:
        # M2M prefetcher — use Django's generic interface (doesn't iterate inline)
        (
            rel_qs,
            rel_obj_attr,
            instance_attr,
            single,
            cache_name,
            is_descriptor,
        ) = prefetcher.get_prefetch_querysets(instances, lookup.get_current_querysets(level))

    additional_lookups = [
        copy.copy(additional_lookup) for additional_lookup in getattr(rel_qs, "_prefetch_related_lookups", ())
    ]
    if additional_lookups:
        rel_qs._prefetch_related_lookups = ()

    # Populate cache asynchronously, then read from it.
    if hasattr(rel_qs, "_fetch_all") and rel_qs._result_cache is None:
        await rel_qs._fetch_all()
        all_related_objects = list(rel_qs._result_cache)
    else:
        all_related_objects = list(rel_qs)

    rel_obj_cache = {}
    for rel_obj in all_related_objects:
        rel_attr_val = rel_obj_attr(rel_obj)
        rel_obj_cache.setdefault(rel_attr_val, []).append(rel_obj)

    to_attr, as_attr = lookup.get_current_to_attr(level)
    if as_attr and instances:
        model = instances[0].__class__
        try:
            model._meta.get_field(to_attr)
        except exceptions.FieldDoesNotExist:
            pass
        else:
            msg = "to_attr={} conflicts with a field on the {} model."
            raise ValueError(msg.format(to_attr, model.__name__))

    leaf = len(lookup.prefetch_through.split(LOOKUP_SEP)) - 1 == level

    for obj in instances:
        instance_attr_val = instance_attr(obj)
        vals = rel_obj_cache.get(instance_attr_val, [])

        if single:
            val = vals[0] if vals else None
            if as_attr:
                setattr(obj, to_attr, val)
            elif is_descriptor:
                setattr(obj, cache_name, val)
            else:
                obj._state.fields_cache[cache_name] = val
        elif as_attr:
            setattr(obj, to_attr, vals)
        else:
            manager = getattr(obj, to_attr)
            if leaf and lookup.queryset is not None:
                qs = manager._apply_rel_filters(lookup.queryset)
            else:
                qs = manager.get_queryset()
            qs._result_cache = vals
            qs._prefetch_done = True
            obj._prefetched_objects_cache[cache_name] = qs
    return all_related_objects, additional_lookups


class RelatedPopulator:
    """
    RelatedPopulator is used for select_related() object instantiation.

    The idea is that each select_related() model will be populated by a
    different RelatedPopulator instance. The RelatedPopulator instances get
    klass_info and select (computed in SQLCompiler) plus the used db as
    input for initialization. That data is used to compute which columns
    to use, how to instantiate the model, and how to populate the links
    between the objects.

    The actual creation of the objects is done in populate() method. This
    method gets row and from_obj as input and populates the select_related()
    model instance.
    """

    def __init__(self, klass_info, select, db):
        self.db = db
        # Pre-compute needed attributes. The attributes are:
        #  - model_cls: the possibly deferred model class to instantiate
        #  - either:
        #    - cols_start, cols_end: usually the columns in the row are
        #      in the same order model_cls.__init__ expects them, so we
        #      can instantiate by model_cls(*row[cols_start:cols_end])
        #    - reorder_for_init: When select_related descends to a child
        #      class, then we want to reuse the already selected parent
        #      data. However, in this case the parent data isn't necessarily
        #      in the same order that Model.__init__ expects it to be, so
        #      we have to reorder the parent data. The reorder_for_init
        #      attribute contains a function used to reorder the field data
        #      in the order __init__ expects it.
        #  - pk_idx: the index of the primary key field in the reordered
        #    model data. Used to check if a related object exists at all.
        #  - init_list: the field attnames fetched from the database. For
        #    deferred models this isn't the same as all attnames of the
        #    model's fields.
        #  - related_populators: a list of RelatedPopulator instances if
        #    select_related() descends to related models from this model.
        #  - local_setter, remote_setter: Methods to set cached values on
        #    the object being populated and on the remote object. Usually
        #    these are Field.set_cached_value() methods.
        select_fields = klass_info["select_fields"]
        from_parent = klass_info["from_parent"]
        if not from_parent:
            self.cols_start = select_fields[0]
            self.cols_end = select_fields[-1] + 1
            self.init_list = [f[0].target.attname for f in select[self.cols_start : self.cols_end]]
            self.reorder_for_init = None
        else:
            attname_indexes = {select[idx][0].target.attname: idx for idx in select_fields}
            model_init_attnames = (f.attname for f in klass_info["model"]._meta.concrete_fields)
            self.init_list = [attname for attname in model_init_attnames if attname in attname_indexes]
            self.reorder_for_init = operator.itemgetter(*[attname_indexes[attname] for attname in self.init_list])

        self.model_cls = klass_info["model"]
        # A primary key must have all of its constituents not-NULL as
        # NULL != NULL and thus NULL cannot be referenced through a foreign
        # relationship. Therefore checking for a single member of the primary
        # key is enough to determine if the referenced object exists or not.
        self.pk_idx = self.init_list.index(self.model_cls._meta.pk_fields[0].attname)
        self.related_populators = get_related_populators(klass_info, select, self.db)
        self.local_setter = klass_info["local_setter"]
        self.remote_setter = klass_info["remote_setter"]

    def populate(self, row, from_obj):
        if self.reorder_for_init:
            obj_data = self.reorder_for_init(row)
        else:
            obj_data = row[self.cols_start : self.cols_end]
        if obj_data[self.pk_idx] is None:
            obj = None
        else:
            obj = self.model_cls.from_db(self.db, self.init_list, obj_data)
            for rel_iter in self.related_populators:
                rel_iter.populate(row, obj)
        self.local_setter(from_obj, obj)
        if obj is not None:
            self.remote_setter(obj, from_obj)


def get_related_populators(klass_info, select, db):
    iterators = []
    related_klass_infos = klass_info.get("related_klass_infos", [])
    for rel_klass_info in related_klass_infos:
        rel_cls = RelatedPopulator(rel_klass_info, select, db)
        iterators.append(rel_cls)
    return iterators
