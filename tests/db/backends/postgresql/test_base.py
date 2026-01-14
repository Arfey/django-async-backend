import warnings

from django.db import DEFAULT_DB_ALIAS
from django.test import TestCase

from django_async_backend.db import async_connections
from django_async_backend.db.backends.postgresql.base import DatabaseWrapper


class TestDatabaseWrapper(TestCase):
    def test_warning_with_pool(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        settings_dict = connection.settings_dict.copy()
        settings_dict["OPTIONS"] = {"pool": True}

        with self.assertWarns(RuntimeWarning):
            DatabaseWrapper(settings_dict).get_connection_params()

    def test_no_warning_without_pool(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        settings_dict = connection.settings_dict.copy()
        settings_dict["OPTIONS"] = {"pool": False}

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RuntimeWarning)
            DatabaseWrapper(settings_dict).get_connection_params()

        self.assertFalse(
            any(
                issubclass(warning.category, RuntimeWarning)
                for warning in caught
            ),
            "RuntimeWarning was raised even though pool option is not enabled.",
        )

    def test_no_warning_when_pool_warning_disabled(self):
        connection = async_connections[DEFAULT_DB_ALIAS]

        settings_dict = connection.settings_dict.copy()
        settings_dict["OPTIONS"] = {"pool": True}

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RuntimeWarning)
            with self.settings(ASYNC_BACKEND_DISABLE_POOL_WARNING=True):
                DatabaseWrapper(settings_dict).get_connection_params()

        self.assertFalse(
            any(
                issubclass(warning.category, RuntimeWarning)
                for warning in caught
            ),
            "RuntimeWarning was raised even though "
            "ASYNC_BACKEND_DISABLE_POOL_WARNING=True.",
        )
