import json

from django.db import DEFAULT_DB_ALIAS
from django.http import (
    HttpRequest,
    HttpResponse,
)

from django_async_backend.db import async_connections


async def index(request: HttpRequest) -> HttpResponse:
    connection = async_connections[DEFAULT_DB_ALIAS]
    result = {}

    async with connection.cursor() as cursor:
        await cursor.execute("select 1")

    await connection.close()

    return HttpResponse(
        content=json.dumps(result), content_type="application/json"
    )
