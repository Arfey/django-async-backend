"""PROTOTYPE: a coherent async save contract.

This is a design proposal, not production code. It implements the contract we
want to put to upstream: on async paths, ``asave`` is the single source of
truth, and ``acreate`` is ``build + asave`` so the two finally agree.

Contract (``await obj.asave()`` / ``await Model.async_object.acreate()``):

* override ``asave``    -> run it; its ``super().asave()`` does the native
  async write and never calls ``save`` -> no double-run.
* override ``save`` only -> honour it via ``sync_to_async`` (identical to
  vanilla Django; correct, but not native).
* override neither     -> native async write.
* override both        -> run ``asave``; ``save`` is ignored.

Compare vanilla Django (proven empirically), starting from ``foo == 0`` with
``save``/``asave`` each doing ``self.foo += 1``:

    overrides    | await obj.asave() | await acreate()
    -------------|-------------------|----------------
    nothing      | 0                 | 0
    save only    | 1                 | 1
    asave only   | 1                 | 0   <- asave silently dropped
    both         | 2  <- DOUBLE      | 1   <- different from asave!

This contract instead yields a coherent ``0 / 1 / 1 / 1`` for *both* entry
points: no double-run, and ``acreate`` agrees with ``asave``.

Prototype scope (documented limitations): single-table models only (no
multi-table inheritance), no signals, no m2m, Postgres only. A production
version routes the native write through the async ``SQLInsertCompiler`` and
``aupdate`` rather than raw SQL, and fires async signals.
"""

from asgiref.sync import sync_to_async
from django.db import DEFAULT_DB_ALIAS, models, router
from django.db import connections as sync_connections
from django.db.models import AutoField

from django_async_backend.db import async_connections


class AsyncSaveModel(models.Model):
    class Meta:
        abstract = True

    async def asave(
        self,
        *,
        force_insert=False,
        force_update=False,
        using=None,
        update_fields=None,
    ):
        cls = type(self)
        overrode_save = cls.save is not models.Model.save
        overrode_asave = cls.asave is not AsyncSaveModel.asave
        if overrode_save and not overrode_asave:
            # The user wrote a sync ``save`` override but no ``asave``. Honour
            # it on a worker thread -- exactly what Django's stock ``asave``
            # does. Correct, though it runs on the sync connection (not native).
            return await sync_to_async(self.save)(
                force_insert=force_insert,
                force_update=force_update,
                using=using,
                update_fields=update_fields,
            )
        # Reached directly (no override) or via ``super().asave()`` from a user
        # ``asave``. Either way the user's async logic, if any, has already run;
        # we never call ``save`` here, so there is no double-run.
        return await self._asave_native(
            force_insert=force_insert,
            using=using,
            update_fields=update_fields,
        )

    asave.alters_data = True

    async def _asave_native(
        self, *, force_insert=False, using=None, update_fields=None
    ):
        meta = self._meta
        alias = (
            using
            or router.db_for_write(type(self), instance=self)
            or DEFAULT_DB_ALIAS
        )
        aconn = async_connections[alias]
        # Value adaptation / identifier quoting only -- no I/O, so using the
        # sync wrapper here is safe and avoids depending on the async wrapper's
        # ops surface.
        prep = sync_connections[alias]
        qn = prep.ops.quote_name

        if self._is_pk_set() and not force_insert:
            fields = [f for f in meta.concrete_fields if not f.primary_key]
            if update_fields is not None:
                wanted = set(update_fields)
                fields = [
                    f
                    for f in fields
                    if f.name in wanted or f.attname in wanted
                ]
            assignments, params = [], []
            for f in fields:
                assignments.append(f"{qn(f.column)} = %s")
                params.append(f.get_db_prep_save(f.pre_save(self, False), prep))
            params.append(self.pk)
            sql = (
                f"UPDATE {qn(meta.db_table)} SET {', '.join(assignments)} "
                f"WHERE {qn(meta.pk.column)} = %s"
            )
            async with await aconn.cursor() as cur:
                await cur.execute(sql, params)
        else:
            fields = [
                f for f in meta.concrete_fields if not isinstance(f, AutoField)
            ]
            columns, placeholders, params = [], [], []
            for f in fields:
                columns.append(qn(f.column))
                placeholders.append("%s")
                params.append(f.get_db_prep_save(f.pre_save(self, True), prep))
            sql = (
                f"INSERT INTO {qn(meta.db_table)} ({', '.join(columns)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"RETURNING {qn(meta.pk.column)}"
            )
            async with await aconn.cursor() as cur:
                await cur.execute(sql, params)
                row = await cur.fetchone()
            setattr(self, meta.pk.attname, row[0])

        self._state.adding = False
        self._state.db = alias
