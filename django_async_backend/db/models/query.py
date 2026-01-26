# flake8: noqa
import operator
from functools import wraps
from weakref import ref as weak_ref

from django.db import NotSupportedError
from django.db.models.fetch_modes import FETCH_ONE
from django.db.models.query import (  # RawModelIterable,
    MAX_GET_RESULTS,
    FlatValuesListIterable,
    ModelIterable,
    NamedValuesListIterable,
    QuerySet,
    ValuesIterable,
    ValuesListIterable,
    get_related_populators,
)
from django.db.models.sql.constants import GET_ITERATOR_CHUNK_SIZE
from django.db.models.utils import create_namedtuple_class

from django_async_backend.db import async_connections
from django_async_backend.db.models.sql.query import AsyncQuery
from django_async_backend.utils.decorators import method_decorators


class BaseIterable:
    def __init__(
        self, queryset, chunked_fetch=False, chunk_size=GET_ITERATOR_CHUNK_SIZE
    ):
        self.queryset = queryset
        self.chunked_fetch = chunked_fetch
        self.chunk_size = chunk_size


class AsyncModelIterable(BaseIterable):
    """Iterable that yields a model instance for each row."""

    async def __aiter__(self):
        queryset = self.queryset
        db = queryset.db
        compiler = queryset.query.get_compiler(using=db)
        fetch_mode = queryset._fetch_mode
        # Execute the query. This will also fill compiler.select, klass_info,
        # and annotations.
        results = await compiler.execute_sql(
            chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size
        )
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
        init_list = [
            f[0].target.attname
            for f in select[model_fields_start:model_fields_end]
        ]
        related_populators = get_related_populators(
            klass_info, select, db, fetch_mode
        )
        known_related_objects = [
            (
                field,
                related_objs,
                operator.attrgetter(
                    *[
                        (
                            field.attname
                            if from_field == "self"
                            else queryset.model._meta.get_field(
                                from_field
                            ).attname
                        )
                        for from_field in field.from_fields
                    ]
                ),
            )
            for field, related_objs in queryset._known_related_objects.items()
        ]
        peers = []
        for row in await compiler.results_iter(results):
            obj = model_cls.from_db(
                db,
                init_list,
                row[model_fields_start:model_fields_end],
                fetch_mode=fetch_mode,
            )
            if fetch_mode.track_peers:
                peers.append(weak_ref(obj))
                obj._state.peers = peers
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


# class AsyncRawModelIterable(BaseIterable):
#     """
#     Iterable that yields a model instance for each row from a raw queryset.
#     """

#     def __iter__(self):
#         # Cache some things for performance reasons outside the loop.
#         db = self.queryset.db
#         query = self.queryset.query
#         connection = connections[db]
#         compiler = connection.ops.compiler("SQLCompiler")(query, connection, db)
#         query_iterator = iter(query)

#         try:
#             (
#                 model_init_names,
#                 model_init_pos,
#                 annotation_fields,
#             ) = self.queryset.resolve_model_init_order()
#             model_cls = self.queryset.model
#             if any(
#                 f.attname not in model_init_names for f in model_cls._meta.pk_fields
#             ):
#                 raise exceptions.FieldDoesNotExist(
#                     "Raw query must include the primary key"
#                 )
#             fields = [self.queryset.model_fields.get(c) for c in self.queryset.columns]
#             cols = [f.get_col(f.model._meta.db_table) if f else None for f in fields]
#             converters = compiler.get_converters(cols)
#             if converters:
#                 query_iterator = compiler.apply_converters(query_iterator, converters)
#             if compiler.has_composite_fields(cols):
#                 query_iterator = compiler.composite_fields_to_tuples(
#                     query_iterator, cols
#                 )
#             fetch_mode = self.queryset._fetch_mode
#             peers = []
#             for values in query_iterator:
#                 # Associate fields to values
#                 model_init_values = [values[pos] for pos in model_init_pos]
#                 instance = model_cls.from_db(
#                     db, model_init_names, model_init_values, fetch_mode=fetch_mode
#                 )
#                 if fetch_mode.track_peers:
#                     peers.append(weak_ref(instance))
#                     instance._state.peers = peers
#                 if annotation_fields:
#                     for column, pos in annotation_fields:
#                         setattr(instance, column, values[pos])
#                 yield instance
#         finally:
#             # Done iterating the Query. If it has its own cursor, close it.
#             if hasattr(query, "cursor") and query.cursor:
#                 query.cursor.close()


class AsyncValuesIterable(BaseIterable):
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
        for row in await compiler.results_iter(
            chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size
        ):
            yield {names[i]: row[i] for i in indexes}


class AsyncValuesListIterable(BaseIterable):
    """
    Iterable returned by QuerySet.values_list(flat=False) that yields a tuple
    for each row.
    """

    async def __aiter__(self):
        queryset = self.queryset
        query = queryset.query
        compiler = query.get_compiler(queryset.db)
        for obj in await compiler.results_iter(
            tuple_expected=True,
            chunked_fetch=self.chunked_fetch,
            chunk_size=self.chunk_size,
        ):
            yield obj


class AsyncNamedValuesListIterable(AsyncValuesListIterable):
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


class AsyncFlatValuesListIterable(BaseIterable):
    """
    Iterable returned by QuerySet.values_list(flat=True) that yields single
    values.
    """

    async def __aiter__(self):
        queryset = self.queryset
        compiler = queryset.query.get_compiler(queryset.db)
        for row in await compiler.results_iter(
            chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size
        ):
            yield row[0]


_iterator_mapping = {
    ModelIterable: AsyncModelIterable,
    # RawModelIterable: AsyncRawModelIterable,
    ValuesIterable: AsyncValuesIterable,
    ValuesListIterable: AsyncValuesListIterable,
    NamedValuesListIterable: AsyncNamedValuesListIterable,
    FlatValuesListIterable: AsyncFlatValuesListIterable,
}


def iterable_class_patcher(fn):
    @wraps(fn)
    def decorator(*args, **kwargs):
        query = fn(*args, **kwargs)
        iterable_class = _iterator_mapping.get(query._iterable_class)

        if iterable_class is None:
            raise NotImplementedError(
                f"{query._iterable_class.__name__} is not supported"
            )

        print(iterable_class)

        query._iterable_class = iterable_class
        return query

    return decorator


def not_implemented_method(reason):
    def decorator(fn):
        def inner(*args, **kwargs):
            raise NotImplementedError(f"{reason} <{fn.__name__}>")

        return inner

    return decorator


@method_decorators(
    not_implemented_method("This method is not permitted for AsyncQuerySet"),
    names=[
        # Synchronous methods
        "get",
        "create",
        "bulk_create",
        "bulk_update",
        "get_or_create",
        "update_or_create",
        "aggregate",
        "count",
        "exists",
        "first",
        "last",
        "earliest",
        "latest",
        "in_bulk",
        "delete",
        "update",
        "iterator",
        "__iter__",
        "__len__",
        "__bool__",
        "__getstate__",
        "__setstate__",
        "explain",
    ],
)
@method_decorators(
    not_implemented_method("This method is not implemented yet"),
    names=[
        # Asynchronous methods
        "acreate",
        "abulk_create",
        "abulk_update",
        "aget_or_create",
        "aupdate_or_create",
        "aearliest",
        "alatest",
        "ain_bulk",
        "adelete",
        "aupdate",
        "aiterator",
    ],
)
@method_decorators(
    iterable_class_patcher,
    names=[
        "values_list",
        "raw",
        "values",
    ],
)
class AsyncQuerySet(QuerySet):
    """Represents a lazy database lookup for a set of objects."""

    def __init__(self, model=None, query=None, using=None, hints=None):
        self.model = model
        self._db = using
        self._hints = hints or {}
        self._query = query or AsyncQuery(self.model)
        self._result_cache = None
        self._sticky_filter = False
        self._for_write = False
        self._prefetch_related_lookups = ()
        self._prefetch_done = False
        self._known_related_objects = {}  # {rel_field: {pk: rel_obj}}
        self._iterable_class = AsyncModelIterable
        self._fetch_mode = FETCH_ONE
        self._fields = None
        self._defer_next_filter = False
        self._deferred_filter = None

    async def _fetch_all(self):
        if self._result_cache is None:
            self._result_cache = [i async for i in self._iterable_class(self)]
        # if self._prefetch_related_lookups and not self._prefetch_done:
        #     self._prefetch_related_objects()

    async def __aiter__(self):
        await self._fetch_all()

        for item in self._result_cache:
            yield item

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"

    def as_manager(cls):
        # Address the circular dependency between `Queryset` and `Manager`.
        from django_async_backend.db.models.manager import AsyncManager

        manager = AsyncManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager

    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)

    def __getitem__(self, k):
        """Retrieve an item or slice from the set of results."""
        if not isinstance(k, (int, slice)):
            raise TypeError(
                "QuerySet indices must be integers or slices, not %s."
                % type(k).__name__
            )
        if (isinstance(k, int) and k < 0) or (
            isinstance(k, slice)
            and (
                (k.start is not None and k.start < 0)
                or (k.stop is not None and k.stop < 0)
            )
        ):
            raise ValueError("Negative indexing is not supported.")

        if self._result_cache is not None:

            async def get_item_from_cache():
                # Even if the object exists in the cache, we return a coroutine
                # to maintain consistent behavior when the object is not in
                # the cache.
                return self._result_cache[k]

            return get_item_from_cache()

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
            if k.step:
                raise ValueError("Step is not supported.")

            return qs

        qs = self._chain()
        qs.query.set_limits(k, k + 1)

        async def get_item():
            await qs._fetch_all()
            return qs._result_cache[0]

        return get_item()

    async def aget(self, *args, **kwargs):
        """
        Perform the query and return a single object matching the given
        keyword arguments.
        """
        if self.query.combinator and (args or kwargs):
            raise NotSupportedError(
                "Calling QuerySet.get(...) with filters after %s() is not "
                "supported." % self.query.combinator
            )
        clone = (
            self._chain()
            if self.query.combinator
            else self.filter(*args, **kwargs)
        )
        if self.query.can_filter() and not self.query.distinct_fields:
            clone = clone.order_by()
        limit = None
        if (
            not clone.query.select_for_update
            or async_connections[
                clone.db
            ].features.supports_select_for_update_with_limit
        ):
            limit = MAX_GET_RESULTS
            clone.query.set_limits(high=limit)
        num = len([i async for i in clone])
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self.model.DoesNotExist(
                "%s matching query does not exist."
                % self.model._meta.object_name
            )
        raise self.model.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!"
            % (
                self.model._meta.object_name,
                (
                    num
                    if not limit or num < limit
                    else "more than %s" % (limit - 1)
                ),
            )
        )

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

    async def aexists(self):
        """
        Return True if the QuerySet would have any results, False otherwise.
        """
        if self._result_cache is None:
            return await self.query.has_results(using=self.db)
        return bool(self._result_cache)

    async def aexplain(self, *, format=None, **options):
        """
        Runs an EXPLAIN on the SQL query this QuerySet would perform, and
        returns the results.
        """
        return await self.query.explain(
            using=self.db, format=format, **options
        )

    async def aaggregate(self, *args, **kwargs):
        return await super().aggregate(*args, **kwargs)

    async def afirst(self):
        """Return the first object of a query or None if no match is found."""
        if self.ordered:
            queryset = self
        else:
            self._check_ordering_first_last_queryset_aggregation(
                method="first"
            )
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
