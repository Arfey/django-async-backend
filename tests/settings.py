import os

_pg_host = os.environ.get("POSTGRES_HOST", "localhost")
_pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))

DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": _pg_host,
        "PORT": _pg_port,
    },
    "other": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": _pg_host,
        "PORT": _pg_port,
    },
}

INSTALLED_APPS = [
    "shared",
    "lookup",
    "expressions",
]

USE_TZ = False
