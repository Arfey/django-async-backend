from django.db.models.manager import BaseManager

from django_async_backend.db.models.query import AsyncQuerySet


class AsyncManager(BaseManager.from_queryset(AsyncQuerySet)):
    pass
