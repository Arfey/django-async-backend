from django.db import models

from django_async_backend.db.models.manager import AsyncManager


class Book(models.Model):
    name = models.CharField(max_length=100)

    async_object = AsyncManager()

    class Meta:
        db_table = "books"


# create table books (id integer, name text);
# insert books (id, name) values (1, 'name');
