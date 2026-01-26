import json

from books.models import Book
from django.db import DEFAULT_DB_ALIAS
from django.db.models import (
    Count,
    Max,
)
from django.http import (
    HttpRequest,
    HttpResponse,
)
from django.urls import path

from django_async_backend.db import async_connections


async def index(request: HttpRequest) -> HttpResponse:
    connection = async_connections[DEFAULT_DB_ALIAS]
    result = {}

    async with connection.cursor() as cursor:
        await cursor.execute("select 1")

    query = (
        Book.async_object.select_related("author")
        .filter(id__gte=0)
        .exclude(id=1)[:1]
    )
    result["query"] = str(query.query)

    result["books"] = []
    async for i in query:
        result["books"].append(
            {
                "id": i.id,
                "name": i.name,
                "author": getattr(i.author, "name", None),
            }
        )

    result["aget"] = str(await Book.async_object.aget(id=1))

    annotated_query = Book.async_object.select_related("author").annotate(
        author_book_count=Count("author__book")
    )
    result["annotated_books"] = []
    async for book in annotated_query:
        result["annotated_books"].append(
            {
                "id": book.id,
                "name": book.name,
                "author": getattr(book.author, "name", None),
                "author_book_count": book.author_book_count,
            }
        )

    result["exist"] = await Book.async_object.filter(id=1).aexists()
    result["count"] = await Book.async_object.filter(author_id=1).acount()
    result["explain"] = await Book.async_object.filter(author_id=1).aexplain()
    result["max_id"] = await Book.async_object.aaggregate(max_id=Max("id"))
    result["first"] = str(await Book.async_object.afirst())
    result["alast"] = str(await Book.async_object.alast())
    result["only"] = str(await Book.async_object.only("id").afirst())
    result["defer"] = str(await Book.async_object.defer("name").afirst())

    result["values_list_flat"] = [
        i async for i in Book.async_object.values_list("id", flat=True)
    ]
    result["values_list"] = [
        i async for i in Book.async_object.values_list("id")
    ]
    result["values"] = [i async for i in Book.async_object.values("id")]
    result["values_list_named"] = [
        i async for i in Book.async_object.values_list("id", named=True)
    ]

    await connection.close()

    return HttpResponse(
        content=json.dumps(result), content_type="application/json"
    )


urlpatterns = [
    path("", index),
]

# poetry run python manage.py runserver 0.0.0.0:8000
# PYTHONPATH=. DJANGO_SETTINGS_MODULE='test_example.settings' poetry run uvicorn test_example.asgi:application --reload --host 0.0.0.0 --port 8000  # noqa
