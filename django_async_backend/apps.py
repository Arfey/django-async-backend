from django.apps import AppConfig


class DjangoAsyncBackendConfig(AppConfig):
    name = "django_async_backend"
    verbose_name = "Django async backend"

    def ready(self):
        # Patch async methods onto related managers for all AsyncModel subclasses.
        # This runs after all models are loaded, so _meta.get_fields() is safe.
        from django.apps import apps

        from django_async_backend.db.models.base import AsyncModel
        from django_async_backend.db.models.related_managers import register_async_related_managers

        for model in apps.get_models():
            if issubclass(model, AsyncModel):
                register_async_related_managers(model)
