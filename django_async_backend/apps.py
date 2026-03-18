from django.apps import AppConfig
from django.conf import settings
from django.core import signals

from django_async_backend.db import close_old_async_connections


class DjangoAsyncBackendConfig(AppConfig):
    name = "django_async_backend"
    verbose_name = "Django async backend"

    def ready(self):
        if not getattr(
            settings, "ASYNC_BACKEND_DISABLE_REQUEST_SIGNALS", False
        ):
            signals.request_started.connect(close_old_async_connections)
            signals.request_finished.connect(close_old_async_connections)
