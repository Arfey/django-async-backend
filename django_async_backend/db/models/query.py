from django.db.models.query import QuerySet

from django_async_backend.utils.decorators import method_decorators


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
