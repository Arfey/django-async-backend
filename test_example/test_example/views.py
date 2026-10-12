import asyncio
import json

from books.models import Book
from django.http import (
    HttpRequest,
    HttpResponse,
    StreamingHttpResponse,
)

from django_async_backend.db import async_acquire


async def index(request: HttpRequest) -> HttpResponse:
    result = {}

    async with async_acquire() as connection:
        async with await connection.cursor() as cursor:
            await cursor.execute("select 1")

        print("aget", await Book.async_object.aget(id=1))  # noqa
        print("afirst", await Book.async_object.afirst())  # noqa
        print("count", await Book.async_object.acount())  # noqa
        async for i in Book.async_object.select_related("author").all():
            print(i, i.author)  # noqa

    return HttpResponse(
        content=json.dumps(result), content_type="application/json"
    )


async def stream(request: HttpRequest) -> StreamingHttpResponse:
    async def body():
        task = asyncio.current_task()
        print(f"[body] start task={task.get_name() if task else None}")  # noqa
        async with async_acquire() as conn:
            for i in range(5):
                count = await Book.async_object.acount()
                print(  # noqa
                    f"[body] chunk={i} count={count} "
                    f"open={conn.connection is not None} wrapper={id(conn):#x}"
                )
                yield f"chunk {i}: count={count}\n".encode()
                await asyncio.sleep(1)
        print("[body] done")  # noqa

    return StreamingHttpResponse(body(), content_type="text/plain")
