import warnings

import pytest
from django.db import DEFAULT_DB_ALIAS
from django.test import override_settings

from django_async_backend.db import async_connections
from django_async_backend.db.backends.postgresql.base import DatabaseWrapper


def test_warning_with_pool():
    connection = async_connections[DEFAULT_DB_ALIAS]
    settings_dict = connection.settings_dict.copy()
    settings_dict["OPTIONS"] = {"pool": True}

    with pytest.warns(RuntimeWarning):
        DatabaseWrapper(settings_dict).get_connection_params()


def test_no_warning_without_pool():
    connection = async_connections[DEFAULT_DB_ALIAS]
    settings_dict = connection.settings_dict.copy()
    settings_dict["OPTIONS"] = {"pool": False}

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        DatabaseWrapper(settings_dict).get_connection_params()

    assert not any(issubclass(w.category, RuntimeWarning) for w in caught)


def test_no_warning_when_pool_warning_disabled():
    connection = async_connections[DEFAULT_DB_ALIAS]
    settings_dict = connection.settings_dict.copy()
    settings_dict["OPTIONS"] = {"pool": True}

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        with override_settings(ASYNC_BACKEND_DISABLE_POOL_WARNING=True):
            DatabaseWrapper(settings_dict).get_connection_params()

    assert not any(issubclass(w.category, RuntimeWarning) for w in caught)
