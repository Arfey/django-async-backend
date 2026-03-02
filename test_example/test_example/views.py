import json

from books.models import Book
from django.db import DEFAULT_DB_ALIAS
from django.http import (
    HttpRequest,
    HttpResponse,
)

from django_async_backend.db import async_connections


async def index(request: HttpRequest) -> HttpResponse:
    connection = async_connections[DEFAULT_DB_ALIAS]
    result = {}

    async with await connection.cursor() as cursor:
        await cursor.execute("select 1")

    print("count", await Book.async_object.acount())
    # print('first', [i async for i in Book.async_object.all()])

    await connection.close()

    return HttpResponse(
        content=json.dumps(result), content_type="application/json"
    )
