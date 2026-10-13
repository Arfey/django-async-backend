import warnings

from django.apps import AppConfig
from django.conf import settings

MIDDLEWARE_PATH = "django_async_backend.middleware.close_async_connections"


def check_middleware():
    if getattr(settings, "ASYNC_BACKEND_DISABLE_MIDDLEWARE_CHECK", False):
        return

    if MIDDLEWARE_PATH not in settings.MIDDLEWARE:
        warnings.warn(
            f"Add '{MIDDLEWARE_PATH}' to MIDDLEWARE. Under ASGI it releases "
            "async connections at the end of each request; without it every "
            "request leaks its connection. If you release them another way, "
            "set ASYNC_BACKEND_DISABLE_MIDDLEWARE_CHECK = True.",
            stacklevel=2,
        )


class DjangoAsyncBackendConfig(AppConfig):
    name = "django_async_backend"
    verbose_name = "Django async backend"

    def ready(self):
        from django_async_backend.db.models.patch import _patch_model

        _patch_model()
        check_middleware()
