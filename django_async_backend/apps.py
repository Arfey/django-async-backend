from django.apps import AppConfig
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

MIDDLEWARE_PATH = "django_async_backend.middleware.close_async_connections"


def check_middleware():
    if getattr(settings, "ASYNC_BACKEND_DISABLE_MIDDLEWARE_CHECK", False):
        return

    if MIDDLEWARE_PATH not in settings.MIDDLEWARE:
        raise ImproperlyConfigured(
            f"Add '{MIDDLEWARE_PATH}' to MIDDLEWARE. It releases async "
            "connections at the end of each request; without it every "
            "request leaks its connection. If you release them another "
            "way, set ASYNC_BACKEND_DISABLE_MIDDLEWARE_CHECK = True."
        )


class DjangoAsyncBackendConfig(AppConfig):
    name = "django_async_backend"
    verbose_name = "Django async backend"

    def ready(self):
        from django_async_backend.db.models.patch import _patch_model

        _patch_model()
        check_middleware()
