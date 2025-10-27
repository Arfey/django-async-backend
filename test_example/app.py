import os

from django.db import DEFAULT_DB_ALIAS
from django.http import (
    HttpRequest,
    HttpResponse,
)
from django.urls import path

from django_async_backend.db import async_connections

# settings
ROOT_URLCONF = "app"
ALLOWED_HOSTS = ["*"]
SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "unsafe-default-key")


DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": "localhost",
        "PORT": 5432,
        "OPTIONS": {
            "pool": {
                "min_size": 2,
                "max_size": 4,
                "timeout": 60,
            }
        },
    }
}


# routes
async def index(request: HttpRequest) -> HttpResponse:
    connection = async_connections[DEFAULT_DB_ALIAS]

    async with connection.cursor() as cursor:
        await cursor.execute("select 1")

    # to release a connection back to the pool
    await connection.close()

    return HttpResponse(content="ok")


urlpatterns = [
    path("", index),
]


# PYTHONPATH=. poetry run django-admin runserver 0.0.0.0:8000 --settings=app
