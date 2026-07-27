from django.apps import AppConfig


class DjangoAsyncBackendConfig(AppConfig):
    name = "django_async_backend"
    verbose_name = "Django async backend"

    def ready(self):
        from django_async_backend.db.models.patch import _patch_model

        _patch_model()
