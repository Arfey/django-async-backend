import django

DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": "localhost",
        "PORT": 5432,
    },
    "other": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": "localhost",
        "PORT": 5432,
    }
}

USE_TZ = False


django.setup()
