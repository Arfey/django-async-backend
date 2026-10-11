from django.test import (
    SimpleTestCase,
    override_settings,
)

from django_async_backend.checks import (
    MIDDLEWARE_PATH,
    check_middleware,
)


class CheckMiddlewareTest(SimpleTestCase):
    @override_settings(MIDDLEWARE=[MIDDLEWARE_PATH])
    def test_passes_when_middleware_installed(self):
        self.assertEqual(check_middleware(None), [])

    @override_settings(MIDDLEWARE=[])
    def test_warns_when_middleware_missing(self):
        [warning] = check_middleware(None)

        self.assertEqual(warning.id, "django_async_backend.W001")
        self.assertIn(MIDDLEWARE_PATH, warning.msg)
