from django.db.models.query import QuerySet


class SyncQuerySetNotImplementedMixin:

    def get(self, *args, **kwargs):
        raise NotImplementedError

    def create(self, **kwargs):
        raise NotImplementedError

    def bulk_create(
        self,
        objs,
        batch_size=None,
        ignore_conflicts=False,
        update_conflicts=False,
        update_fields=None,
        unique_fields=None,
    ):
        raise NotImplementedError

    def bulk_update(self, objs, fields, batch_size=None):
        raise NotImplementedError

    def get_or_create(self, defaults=None, **kwargs):
        raise NotImplementedError

    def update_or_create(self, defaults=None, create_defaults=None, **kwargs):
        raise NotImplementedError

    def aggregate(self, *args, **kwargs):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    def first(self):
        raise NotImplementedError

    def last(self):
        raise NotImplementedError

    def earliest(self, *fields):
        raise NotImplementedError

    def latest(self, *fields):
        raise NotImplementedError

    def in_bulk(self, id_list=None, *, field_name="pk"):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def update(self, **kwargs):
        raise NotImplementedError

    def iterator(self, chunk_size=None):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def __bool__(self):
        raise NotImplementedError

    def __getitem__(self, k):
        raise NotImplementedError

    def explain(self, *, format=None, **options):
        raise NotImplementedError


class AsyncQuerySetNotImplementedMixin:
    async def aget(self, *args, **kwargs):
        raise NotImplementedError

    async def acreate(self, **kwargs):
        raise NotImplementedError

    async def abulk_create(
        self,
        objs,
        batch_size=None,
        ignore_conflicts=False,
        update_conflicts=False,
        update_fields=None,
        unique_fields=None,
    ):
        raise NotImplementedError

    async def abulk_update(self, objs, fields, batch_size=None):
        raise NotImplementedError

    async def aget_or_create(self, defaults=None, **kwargs):
        raise NotImplementedError

    async def aupdate_or_create(
        self, defaults=None, create_defaults=None, **kwargs
    ):
        raise NotImplementedError

    async def aaggregate(self, *args, **kwargs):
        raise NotImplementedError

    async def acount(self):
        raise NotImplementedError

    async def aexists(self):
        raise NotImplementedError

    async def afirst(self):
        raise NotImplementedError

    async def alast(self):
        raise NotImplementedError

    async def aearliest(self, *fields):
        raise NotImplementedError

    async def alatest(self, *fields):
        raise NotImplementedError

    async def ain_bulk(self, id_list=None, *, field_name="pk"):
        raise NotImplementedError

    async def adelete(self):
        raise NotImplementedError

    async def aupdate(self, **kwargs):
        raise NotImplementedError

    async def aiterator(self, chunk_size=2000):
        raise NotImplementedError

    async def aexplain(self, *, format=None, **options):
        raise NotImplementedError


class AsyncQuerySet(
    SyncQuerySetNotImplementedMixin, AsyncQuerySetNotImplementedMixin, QuerySet
):
    """Represent a lazy database lookup for a set of objects."""
