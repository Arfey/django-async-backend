import warnings

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
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            check_middleware()

    @override_settings(MIDDLEWARE=[])
    def test_warns_when_middleware_missing(self):
        with self.assertWarnsMessage(UserWarning, MIDDLEWARE_PATH):
            check_middleware()

    @override_settings(
        MIDDLEWARE=[], ASYNC_BACKEND_DISABLE_MIDDLEWARE_CHECK=True
    )
    def test_setting_disables_check(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            check_middleware()
