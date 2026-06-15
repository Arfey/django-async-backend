from django.db.models.manager import BaseManager

from django_async_backend.db.models.query import QuerySet


class AsyncManager(BaseManager.from_queryset(QuerySet)):
    async def acreate(self, **kwargs):
        """PROTOTYPE: build + asave, so acreate honours the same async save
        contract as asave (see django_async_backend.db.models.base). Requires
        the model to inherit AsyncSaveModel; otherwise obj.asave is Django's
        stock sync_to_async(save) and you get vanilla semantics.
        """
        obj = self.model(**kwargs)
        await obj.asave(force_insert=True)
        return obj

    acreate.alters_data = True
