import json
from itertools import chain

from django.core.exceptions import EmptyResultSet
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.constants import (
    CURSOR,
    GET_ITERATOR_CHUNK_SIZE,
    MULTI,
    NO_RESULTS,
    ROW_COUNT,
    SINGLE,
)


async def empty_aiter():
    if False:
        yield


class AsyncSQLCompiler(SQLCompiler):

    async def execute_sql(
        self,
        result_type=MULTI,
        chunked_fetch=False,
        chunk_size=GET_ITERATOR_CHUNK_SIZE,
    ):
        """
        Run the query against the database and return the result(s). The
        return value depends on the value of result_type.

        When result_type is:
        - MULTI: Retrieves all rows using fetchmany(). Wraps in an iterator for
        chunked reads when supported.
        - SINGLE: Retrieves a single row using fetchone().
        - ROW_COUNT: Retrieves the number of rows in the result.
        - CURSOR: Runs the query, and returns the cursor object. It is the
        caller's responsibility to close the cursor.
        """

        result_type = result_type or NO_RESULTS
        try:
            sql, params = self.as_sql()
            if not sql:
                raise EmptyResultSet
        except EmptyResultSet:
            if result_type == MULTI:
                return empty_aiter()
            else:
                return
        if chunked_fetch:
            cursor_ctx = self.connection.chunked_cursor()
        else:
            cursor_ctx = self.connection.cursor()

        async with cursor_ctx as cursor:
            await cursor.execute(sql, params)

            if result_type == ROW_COUNT:
                return cursor.rowcount
            if result_type == CURSOR:
                raise NotImplementedError
            if result_type == SINGLE:
                val = await cursor.fetchone()
                if val:
                    return val[0 : self.col_count]  # noqa
                return val
            if result_type == NO_RESULTS:
                return

            result = async_cursor_iter(
                cursor,
                self.connection.features.empty_fetchmany_value,
                self.col_count if self.has_extra_select else None,
                chunk_size,
            )
            if (
                not chunked_fetch
                or not self.connection.features.can_use_chunked_reads
            ):
                # If we are using non-chunked reads, we return the same data
                # structure as normally, but ensure it is all read into memory
                # before going any further. Use chunked_fetch if requested,
                # unless the database doesn't support it.
                return [i async for i in result]
            return result

    async def has_results(self):
        """
        Backends (e.g. NoSQL) can override this in order to use optimized
        versions of "query has any results."
        """
        return bool(await self.execute_sql(SINGLE))

    async def explain_query(self):
        result = list(await self.execute_sql())
        # Some backends return 1 item tuples with strings, and others return
        # tuples with integers and strings. Flatten them out into strings.
        format_ = self.query.explain_info.format
        output_formatter = (
            json.dumps if format_ and format_.lower() == "json" else str
        )
        for row in result:
            for value in row:
                if not isinstance(value, str):
                    yield " ".join([output_formatter(c) for c in value])
                else:
                    yield value

    async def results_iter(
        self,
        results=None,
        tuple_expected=False,
        chunked_fetch=False,
        chunk_size=GET_ITERATOR_CHUNK_SIZE,
    ):
        """Return an iterator over the results from executing this query."""
        if results is None:
            results = await self.execute_sql(
                MULTI, chunked_fetch=chunked_fetch, chunk_size=chunk_size
            )
        fields = [s[0] for s in self.select[0 : self.col_count]]  # noqa
        converters = self.get_converters(fields)
        rows = chain.from_iterable(results)
        if converters:
            rows = self.apply_converters(rows, converters)
        if self.has_composite_fields(fields):
            rows = self.composite_fields_to_tuples(rows, fields)
        if tuple_expected:
            rows = map(tuple, rows)
        return rows


async def async_cursor_iter(cursor, sentinel, col_count, itersize):
    """
    Yield blocks of rows from a cursor and ensure the cursor is closed when
    done.
    """

    async def fetchmany_iter():
        while True:
            rows = await cursor.fetchmany(itersize)
            if rows == sentinel:
                break
            yield rows

    async for rows in fetchmany_iter():
        yield rows if col_count is None else [r[:col_count] for r in rows]
