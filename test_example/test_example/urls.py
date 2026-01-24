from books.models import Book
from django.db import DEFAULT_DB_ALIAS
from django.http import (
    HttpRequest,
    HttpResponse,
)
from django.urls import path

from django_async_backend.db import async_connections


async def index(request: HttpRequest) -> HttpResponse:
    connection = async_connections[DEFAULT_DB_ALIAS]

    async with connection.cursor() as cursor:
        await cursor.execute("select 1")

    query = (
        Book.async_object.select_related("author")
        .filter(id__gte=0)
        .exclude(id=1)[:1]
    )

    print(query.query)  # noqa

    async for i in query:
        print(i, i.author)  # noqa

    print("aget", await Book.async_object.aget(id=1))  # noqa

    # to release a connection back to the pool
    await connection.close()

    return HttpResponse(content="ok")


urlpatterns = [
    path("", index),
]

# poetry run python manage.py runserver 0.0.0.0:8000
# PYTHONPATH=. DJANGO_SETTINGS_MODULE='test_example.settings' poetry run uvicorn test_example.asgi:application --reload --host 0.0.0.0 --port 8000  # noqa
