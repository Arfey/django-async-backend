from django.db import models

from django_async_backend.db.models.manager import AsyncManager


class Book(models.Model):
    id = models.AutoField(primary_key=True)
    author = models.ForeignKey("Author", on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=100)

    async_object = AsyncManager()

    class Meta:
        db_table = "books"


class Author(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)

    async_object = AsyncManager()

    class Meta:
        db_table = "authors"


# create table authors (id integer primary key, name text);
# create table books (id integer primary key, author_id integer references
# authors(id), name text);
# insert into authors (id, name) values (1, 'author name');
# insert into books (id, name, author_id) values (1, 'name1', 1);
# insert into books (id, name) values (2, 'name2');
