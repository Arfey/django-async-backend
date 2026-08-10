from unittest import mock

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
