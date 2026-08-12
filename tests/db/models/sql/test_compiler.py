from unittest import mock

from django.db import DatabaseError
from django.db.models import F
from test_app.models import TestModel

from django_async_backend.db.models.sql import (
    InsertQuery,
    UpdateQuery,
)
from django_async_backend.test import AsyncioTestCase


class TestQuoteNameUnlessAlias(AsyncioTestCase):
    """Backends that prohibit dollar signs in column aliases must reject them
    rather than emitting SQL the database will not accept.
    """

    def _compiler(self):
        return TestModel.async_objects.all().query.get_compiler("default")

    async def test_dollar_sign_alias_raises(self):
        compiler = self._compiler()
        self.assertTrue(
            compiler.connection.features.prohibits_dollar_signs_in_column_aliases  # noqa
        )

        with self.assertRaises(ValueError) as ctx:
            compiler.quote_name_unless_alias("we$rd")

        self.assertEqual(
            str(ctx.exception),
            "Dollar signs are not permitted in column aliases on "
            f"{compiler.connection.display_name}.",
        )

    async def test_plain_name_is_quoted(self):
        compiler = self._compiler()

        self.assertEqual(compiler.quote_name_unless_alias("name"), '"name"')

    async def test_dollar_sign_allowed_when_feature_permits(self):
        # Backends without the restriction must keep accepting such names.
        compiler = self._compiler()

        with mock.patch.object(
            type(compiler.connection.features),
            "prohibits_dollar_signs_in_column_aliases",
            False,
        ):
            self.assertEqual(
                compiler.quote_name_unless_alias("we$rd"), '"we$rd"'
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
