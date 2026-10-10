from django.core.exceptions import ImproperlyConfigured
from django.test import (
    SimpleTestCase,
    override_settings,
)

from django_async_backend.apps import (
    MIDDLEWARE_PATH,
    check_middleware,
)


class CheckMiddlewareTest(SimpleTestCase):
    @override_settings(MIDDLEWARE=[MIDDLEWARE_PATH])
    def test_passes_when_middleware_installed(self):
        check_middleware()

    @override_settings(MIDDLEWARE=[])
    def test_raises_when_middleware_missing(self):
        with self.assertRaisesMessage(ImproperlyConfigured, MIDDLEWARE_PATH):
            check_middleware()

    @override_settings(
        MIDDLEWARE=[], ASYNC_BACKEND_DISABLE_MIDDLEWARE_CHECK=True
    )
    def test_setting_disables_check(self):
        check_middleware()
