from unittest import mock

from django.db import DatabaseError
from test_app.models import TestModel

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
