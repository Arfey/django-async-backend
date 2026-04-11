from django.db.models.manager import BaseManager

from django_async_backend.db.models.query import QuerySet


class AsyncManager(BaseManager.from_queryset(QuerySet)):  # type: ignore[misc]
    pass
