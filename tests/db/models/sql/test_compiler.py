import re
from unittest import mock

from django.db import DatabaseError
from django.db.models import F
from test_app.models import (
    SaveModel,
    TestModel,
)

from django_async_backend.db.models.sql import (
    InsertQuery,
    UpdateQuery,
)
from django_async_backend.test import AsyncioTestCase


class TestQuoteName(AsyncioTestCase):
    """Every identifier is quoted, whatever its origin, so special characters
    cannot escape into the surrounding SQL. Quoted values are memoized.
    """

    def _compiler(self):
        return TestModel.async_objects.all().query.get_compiler("default")

    async def test_plain_name_is_quoted(self):
        self.assertEqual(self._compiler().quote_name("name"), '"name"')

    async def test_dollar_sign_is_quoted_rather_than_rejected(self):
        # Unconditional quoting replaced the old ValueError guard, so names
        # a backend once refused as aliases are now simply quoted.
        self.assertEqual(self._compiler().quote_name("we$rd"), '"we$rd"')

    async def test_table_alias_is_quoted_in_generated_sql(self):
        # Table aliases used to be passed through unquoted; they no longer
        # are, so they reach the database quoted.
        compiler = self._compiler()
        table = TestModel._meta.db_table
        sql, _ = compiler.as_sql()

        self.assertIn(f'"{table}"', sql)
        self.assertNotRegex(sql, rf"(?<!\"){re.escape(table)}(?!\")")

    async def test_quoted_name_is_memoized(self):
        compiler = self._compiler()

        with mock.patch.object(
            compiler.connection.ops,
            "quote_name",
            wraps=compiler.connection.ops.quote_name,
        ) as quote_name:
            self.assertEqual(compiler.quote_name("name"), '"name"')
            self.assertEqual(compiler.quote_name("name"), '"name"')

        # The second call is served from quote_cache.
        quote_name.assert_called_once_with("name")
        self.assertEqual(compiler.quote_cache["name"], '"name"')

    async def test_quote_name_unless_alias_is_an_alias_of_quote_name(self):
        # Kept upstream for backward compatibility pending deprecation.
        compiler = self._compiler()

        self.assertEqual(compiler.quote_name_unless_alias("we$rd"), '"we$rd"')


class TestExtraOrderByQuoting(AsyncioTestCase):
    """A dotted extra(order_by=...) entry is split into table and column. The
    table is quoted; the column is passed through verbatim as raw SQL.
    """

    def _sql(self, **extra):
        qs = TestModel.async_objects.extra(**extra)
        return qs.query.get_compiler("default").as_sql()[0]

    async def test_dotted_order_by_quotes_only_the_table(self):
        table = TestModel._meta.db_table
        sql = self._sql(order_by=[f"{table}.value"])

        # The table half is quoted, the column half is left verbatim.
        self.assertIn(f'("{table}".value) ASC', sql)

    async def test_descending_dotted_order_by_is_quoted(self):
        table = TestModel._meta.db_table
        sql = self._sql(order_by=[f"-{table}.value"])

        self.assertIn(f'("{table}".value) DESC', sql)

    async def test_dotted_order_by_orders_rows(self):
        # The emitted SQL is accepted by the database and actually orders.
        table = TestModel._meta.db_table
        await TestModel.async_objects.acreate(name="b", value=2)
        await TestModel.async_objects.acreate(name="a", value=1)

        self.assertEqual(
            [
                value
                async for value in TestModel.async_objects.extra(
                    order_by=[f"-{table}.value"]
                ).values_list("value", flat=True)
            ],
            [2, 1],
        )

    async def test_undotted_order_by_skips_the_branch(self):
        # Without a dot the entry is not split, so no table is quoted into
        # the ORDER BY clause.
        sql = self._sql(order_by=["value"])

        self.assertIn("ORDER BY", sql)
        self.assertNotIn(f'"{TestModel._meta.db_table}".value)', sql)


class TestExtraTablesQuoting(AsyncioTestCase):
    """extra(tables=...) appends each table to the FROM clause quoted, but
    only when that alias is not already being joined.
    """

    def _sql(self, **extra):
        qs = TestModel.async_objects.extra(**extra)
        return qs.query.get_compiler("default").as_sql()[0]

    async def test_extra_table_is_quoted_in_from_clause(self):
        sql = self._sql(tables=["save_model"])

        self.assertIn('FROM "test_model" , "save_model"', sql)

    async def test_extra_table_needing_quoting_is_quoted(self):
        # A name that is not a bare identifier still reaches the FROM clause
        # wrapped rather than spliced in raw. (quote_name() only wraps; it
        # does not escape embedded quotes, so this stays a plain name.)
        sql = self._sql(tables=["we rd"])

        self.assertIn('"we rd"', sql)

    async def test_already_quoted_extra_table_is_not_double_quoted(self):
        sql = self._sql(tables=['"save_model"'])

        self.assertIn('"save_model"', sql)
        self.assertNotIn('""save_model""', sql)

    async def test_already_joined_table_is_not_appended_twice(self):
        # The base table is already in alias_map with a refcount above one,
        # so the guard skips it and the FROM clause names it once.
        table = TestModel._meta.db_table
        sql = self._sql(tables=[table])

        self.assertEqual(sql.count(f'"{table}"'), sql.count(f'"{table}".') + 1)
        self.assertNotIn(f'"{table}" , "{table}"', sql)

    async def test_extra_table_query_executes(self):
        # The generated cross join is valid SQL the database accepts.
        await TestModel.async_objects.acreate(name="only", value=1)
        await SaveModel.async_objects.acreate(name="other", value=2)

        self.assertEqual(
            await TestModel.async_objects.extra(
                tables=["save_model"]
            ).acount(),
            1,
        )


class TestExecuteSqlCursorClosing(AsyncioTestCase):
    """When execute() fails the cursor may already be closed, so closing it
    again can fail too. That secondary failure must not mask or decorate the
    original error.
    """

    def _compiler(self):
        return TestModel.async_objects.all().query.get_compiler("default")

    def _cursor_mock(self, execute_error, close_error=None):
        cursor = mock.AsyncMock()
        cursor.execute.side_effect = execute_error
        cursor.close.side_effect = close_error
        return cursor

    async def test_close_failure_is_suppressed(self):
        compiler = self._compiler()
        execute_err = DatabaseError("execute failed")
        cursor = self._cursor_mock(execute_err, DatabaseError("close failed"))

        with mock.patch.object(
            compiler.connection, "cursor", return_value=self._awaitable(cursor)
        ):
            with self.assertRaises(DatabaseError) as ctx:
                await compiler.execute_sql()

        # The original execute() error propagates, with no irrelevant context
        # from trying to close an already-closed cursor.
        exc = ctx.exception
        self.assertIs(exc, execute_err)
        self.assertIsNone(exc.__cause__)
        self.assertTrue(exc.__suppress_context__)

    async def test_execute_error_propagates_when_close_succeeds(self):
        compiler = self._compiler()
        execute_err = DatabaseError("execute failed")
        cursor = self._cursor_mock(execute_err)

        with mock.patch.object(
            compiler.connection, "cursor", return_value=self._awaitable(cursor)
        ):
            with self.assertRaises(DatabaseError) as ctx:
                await compiler.execute_sql()

        self.assertIs(ctx.exception, execute_err)
        cursor.close.assert_awaited_once()

    async def test_non_database_error_while_closing_is_not_suppressed(self):
        # Only DatabaseError is caught; anything else surfaces so genuine
        # bugs in close() are not hidden.
        compiler = self._compiler()
        cursor = self._cursor_mock(
            DatabaseError("execute failed"), ValueError("boom")
        )

        with mock.patch.object(
            compiler.connection, "cursor", return_value=self._awaitable(cursor)
        ):
            with self.assertRaises(ValueError):
                await compiler.execute_sql()

    @staticmethod
    def _awaitable(value):
        async def _coro():
            return value

        return _coro()


class GetPlaceholderSqlMixin:
    """Fields such as geo fields declare get_placeholder_sql() to wrap their
    value in a database function. No field in the test app needs that, so the
    hook is attached to a plain IntegerField for the duration of a test.
    """

    def placeholder_hook(self, sql="ABS(%s)"):
        """Return (hook, calls) where hook mimics a field's
        get_placeholder_sql() and records how it was invoked.
        """
        calls = []

        def get_placeholder_sql(value, compiler, connection):
            calls.append((value, compiler, connection))
            return sql, [value]

        return get_placeholder_sql, calls

    def patch_field(self, model, field_name, hook):
        field = model._meta.get_field(field_name)
        return mock.patch.object(
            field, "get_placeholder_sql", hook, create=True
        )


class TestInsertFieldAsSqlPlaceholder(GetPlaceholderSqlMixin, AsyncioTestCase):
    """SQLInsertCompiler.field_as_sql() must delegate to the field's
    get_placeholder_sql() instead of emitting the plain '%s' placeholder.
    """

    async def test_placeholder_sql_is_used_for_insert(self):
        hook, calls = self.placeholder_hook()

        with self.patch_field(TestModel, "value", hook):
            obj = await TestModel.async_objects.acreate(
                name="Placeholder", value=-5
            )

        # The hook received the prepared value, this compiler and its
        # connection, and its SQL was applied by the database: ABS(-5) == 5.
        self.assertEqual(len(calls), 1)
        value, compiler, connection = calls[0]
        self.assertEqual(value, -5)
        self.assertIs(connection, compiler.connection)
        self.assertEqual(
            await TestModel.async_objects.values_list("value", flat=True)
            .filter(pk=obj.pk)
            .aget(),
            5,
        )

    async def test_placeholder_sql_appears_in_generated_sql(self):
        hook, _ = self.placeholder_hook()
        query = TestModel.async_objects.all().query

        with self.patch_field(TestModel, "value", hook):
            compiler = InsertQuery(TestModel).get_compiler("default")
            compiler.query.insert_values(
                [
                    TestModel._meta.get_field("name"),
                    TestModel._meta.get_field("value"),
                ],
                [TestModel(name="Sql", value=-3)],
            )
            ((sql, params),) = compiler.as_sql()

        self.assertIn("ABS(%s)", sql)
        self.assertIn(-3, params)
        # The unpatched field keeps the ordinary placeholder.
        self.assertIsNone(
            getattr(
                query.model._meta.get_field("value"),
                "get_placeholder_sql",
                None,
            )
        )

    async def test_placeholder_sql_used_for_every_bulk_created_row(self):
        hook, calls = self.placeholder_hook()

        with self.patch_field(TestModel, "value", hook):
            await TestModel.async_objects.abulk_create(
                [
                    TestModel(name="Bulk1", value=-1),
                    TestModel(name="Bulk2", value=-2),
                ]
            )

        # assemble_as_sql() calls field_as_sql() once per row.
        self.assertEqual([call[0] for call in calls], [-1, -2])
        self.assertEqual(
            {
                name: value
                async for name, value in TestModel.async_objects.values_list(
                    "name", "value"
                ).filter(name__startswith="Bulk")
            },
            {"Bulk1": 1, "Bulk2": 2},
        )

    async def test_field_without_hook_uses_plain_placeholder(self):
        # The elif is only taken when the field defines the hook; the common
        # case still goes through the '%s' branch.
        obj = await TestModel.async_objects.acreate(name="Plain", value=-5)

        self.assertEqual(
            await TestModel.async_objects.values_list("value", flat=True)
            .filter(pk=obj.pk)
            .aget(),
            -5,
        )


class TestUpdatePlaceholderSql(GetPlaceholderSqlMixin, AsyncioTestCase):
    """SQLUpdateCompiler.as_sql() must route a field's get_placeholder_sql()
    into the SET clause ahead of the expression and plain-value branches.
    """

    async def asyncSetUp(self):
        self.obj = await TestModel.async_objects.acreate(
            name="Update", value=1
        )

    async def _value(self):
        return (
            await TestModel.async_objects.values_list("value", flat=True)
            .filter(pk=self.obj.pk)
            .aget()
        )

    async def test_placeholder_sql_is_used_for_update(self):
        hook, calls = self.placeholder_hook()

        with self.patch_field(TestModel, "value", hook):
            updated = await TestModel.async_objects.filter(
                pk=self.obj.pk
            ).aupdate(value=-9)

        self.assertEqual(updated, 1)
        self.assertEqual(len(calls), 1)
        value, compiler, connection = calls[0]
        self.assertEqual(value, -9)
        self.assertIs(connection, compiler.connection)
        # SET "value" = ABS(-9) rather than SET "value" = -9.
        self.assertEqual(await self._value(), 9)

    async def test_placeholder_sql_appears_in_set_clause(self):
        hook, _ = self.placeholder_hook()
        query = TestModel.async_objects.filter(pk=self.obj.pk).query.chain(
            UpdateQuery
        )
        query.add_update_values({"value": -4})

        with self.patch_field(TestModel, "value", hook):
            sql, params = query.get_compiler("default").as_sql()

        self.assertIn('"value" = ABS(%s)', sql)
        self.assertEqual(params[0], -4)

    async def test_placeholder_sql_takes_precedence_over_expression(self):
        # An F() expression has as_sql(), but the hook is checked first.
        hook, calls = self.placeholder_hook()

        with self.patch_field(TestModel, "value", hook):
            query = TestModel.async_objects.filter(pk=self.obj.pk).query.chain(
                UpdateQuery
            )
            query.add_update_values({"value": F("value") - 10})
            sql, _ = query.get_compiler("default").as_sql()

        self.assertIn('"value" = ABS(%s)', sql)
        self.assertEqual(len(calls), 1)

    async def test_other_fields_keep_plain_placeholder(self):
        # Only the field carrying the hook is rewritten; siblings in the same
        # SET clause still use '%s'.
        hook, _ = self.placeholder_hook()

        with self.patch_field(TestModel, "value", hook):
            query = TestModel.async_objects.filter(pk=self.obj.pk).query.chain(
                UpdateQuery
            )
            query.add_update_values({"value": -2, "name": "Renamed"})
            sql, _ = query.get_compiler("default").as_sql()

        self.assertIn('"value" = ABS(%s)', sql)
        self.assertIn('"name" = %s', sql)

    async def test_field_without_hook_uses_plain_placeholder(self):
        await TestModel.async_objects.filter(pk=self.obj.pk).aupdate(value=-9)

        self.assertEqual(await self._value(), -9)
