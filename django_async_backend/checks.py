from django.conf import settings
from django.core.checks import Warning

MIDDLEWARE_PATH = "django_async_backend.middleware.close_async_connections"


def check_middleware(app_configs, **kwargs):
    if MIDDLEWARE_PATH in settings.MIDDLEWARE:
        return []

    return [
        Warning(
            f"'{MIDDLEWARE_PATH}' is not in MIDDLEWARE.",
            hint=(
                "Under ASGI it releases async connections at the end of each "
                "request; without it every request leaks its connection. If "
                "you release them another way, add 'django_async_backend.W001' "
                "to SILENCED_SYSTEM_CHECKS."
            ),
            id="django_async_backend.W001",
        )
    ]
