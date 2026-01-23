# flake8: noqa
from weakref import ref as weak_ref

from django.db.models.fetch_modes import FETCH_ONE
from django.db.models.query import QuerySet
from django.db.models.sql.constants import GET_ITERATOR_CHUNK_SIZE

from django_async_backend.db.models.sql.query import AsyncQuery
from django_async_backend.utils.decorators import method_decorators


class BaseIterable:
    def __init__(
        self, queryset, chunked_fetch=False, chunk_size=GET_ITERATOR_CHUNK_SIZE
    ):
        self.queryset = queryset
        self.chunked_fetch = chunked_fetch
        self.chunk_size = chunk_size

    # async def _async_generator(self):
    #     # Generators don't actually start running until the first time you call
    #     # next() on them, so make the generator object in the async thread and
    #     # then repeatedly dispatch to it in a sync thread.
    #     sync_generator = self.__iter__()

    #     def next_slice(gen):
    #         return list(islice(gen, self.chunk_size))

    #     while True:
    #         chunk = await sync_to_async(next_slice)(sync_generator)
    #         for item in chunk:
    #             yield item
    #         if len(chunk) < self.chunk_size:
    #             break

    # # __aiter__() is a *synchronous* method that has to then return an
    # # *asynchronous* iterator/generator. Thus, nest an async generator inside
    # # it.
    # # This is a generic iterable converter for now, and is going to suffer a
    # # performance penalty on large sets of items due to the cost of crossing
    # # over the sync barrier for each chunk. Custom __aiter__() methods should
    # # be added to each Iterable subclass, but that needs some work in the
    # # Compiler first.
    # def __aiter__(self):
    #     return self._async_generator()


class AsyncModelIterable(BaseIterable):
    """Iterable that yields a model instance for each row."""

    async def __aiter__(self):
        queryset = self.queryset
        db = queryset.db
        compiler = queryset.query.get_compiler(using=db)
        fetch_mode = queryset._fetch_mode
        # Execute the query. This will also fill compiler.select, klass_info,
        # and annotations.
        results = await compiler.execute_sql(  #########################
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
        # related_populators = get_related_populators(klass_info, select, db, fetch_mode)
        # known_related_objects = [
        #     (
        #         field,
        #         related_objs,
        #         operator.attrgetter(
        #             *[
        #                 (
        #                     field.attname
        #                     if from_field == "self"
        #                     else queryset.model._meta.get_field(from_field).attname
        #                 )
        #                 for from_field in field.from_fields
        #             ]
        #         ),
        #     )
        #     for field, related_objs in queryset._known_related_objects.items()
        # ]
        peers = []
        for row in compiler.results_iter(results):  #########################
            obj = model_cls.from_db(
                db,
                init_list,
                row[model_fields_start:model_fields_end],
                fetch_mode=fetch_mode,
            )
            if fetch_mode.track_peers:
                peers.append(weak_ref(obj))
                obj._state.peers = peers
            # for rel_populator in related_populators:
            #     rel_populator.populate(row, obj)
            # if annotation_col_map:
            #     for attr_name, col_pos in annotation_col_map.items():
            #         setattr(obj, attr_name, row[col_pos])

            # Add the known related objects to the model.
            # for field, rel_objs, rel_getter in known_related_objects:
            #     # Avoid overwriting objects loaded by, e.g., select_related().
            #     if field.is_cached(obj):
            #         continue
            #     rel_obj_id = rel_getter(obj)
            #     try:
            #         rel_obj = rel_objs[rel_obj_id]
            #     except KeyError:
            #         pass  # May happen in qs1 | qs2 scenarios.
            #     else:
            #         setattr(obj, field.name, rel_obj)

            yield obj


def not_implemented_method(reason):
    def decorator(fn):
        def inner(*args, **kwargs):
            raise NotImplementedError(reason)

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
        "__getitem__",
        "explain",
    ],
)
@method_decorators(
    not_implemented_method("This method is not implemented yet"),
    names=[
        # Asynchronous methods
        "aget",
        "acreate",
        "abulk_create",
        "abulk_update",
        "aget_or_create",
        "aupdate_or_create",
        "aaggregate",
        "acount",
        "aexists",
        "afirst",
        "alast",
        "aearliest",
        "alatest",
        "ain_bulk",
        "adelete",
        "aupdate",
        "aiterator",
        "aexplain",
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
