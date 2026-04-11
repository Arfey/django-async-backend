from django.db import models

from django_async_backend.db.models.base import AsyncModel
from django_async_backend.db.models.manager import AsyncManager


class AbstractBaseModel(AsyncModel, models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    async_object = AsyncManager()

    class Meta:
        abstract = True


class TestModel(AbstractBaseModel):
    __test__ = False  # prevent pytest collection

    relative = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="relatives",
    )

    class Meta:
        db_table = "test_model"


class GetLatestByModel(AbstractBaseModel):
    class Meta:
        db_table = "latest_by"
        get_latest_by = "id"


# ── Models for testing on_delete behaviors ──────────────────────────────


class Author(AsyncModel, models.Model):
    name = models.CharField(max_length=255)

    async_object = AsyncManager()

    class Meta:
        db_table = "test_author"


class Book(AsyncModel, models.Model):
    title = models.CharField(max_length=255)
    author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name="books")

    async_object = AsyncManager()

    class Meta:
        db_table = "test_book"


class Review(AsyncModel, models.Model):
    """CASCADE chain: Author → Book → Review"""

    text = models.CharField(max_length=255)
    book = models.ForeignKey(Book, on_delete=models.CASCADE, related_name="reviews")

    async_object = AsyncManager()

    class Meta:
        db_table = "test_review"


class EditorNote(AsyncModel, models.Model):
    """SET_NULL: when book is deleted, editor_note.book becomes NULL"""

    note = models.CharField(max_length=255)
    book = models.ForeignKey(Book, on_delete=models.SET_NULL, null=True, related_name="editor_notes")

    async_object = AsyncManager()

    class Meta:
        db_table = "test_editor_note"


class ProtectedComment(AsyncModel, models.Model):
    """PROTECT: cannot delete book if it has protected comments"""

    text = models.CharField(max_length=255)
    book = models.ForeignKey(Book, on_delete=models.PROTECT, related_name="protected_comments")

    async_object = AsyncManager()

    class Meta:
        db_table = "test_protected_comment"


class RestrictedTag(AsyncModel, models.Model):
    """RESTRICT: like PROTECT but allows deletion if tag is also being deleted"""

    label = models.CharField(max_length=255)
    book = models.ForeignKey(Book, on_delete=models.RESTRICT, related_name="restricted_tags")

    async_object = AsyncManager()

    class Meta:
        db_table = "test_restricted_tag"


class Event(AsyncModel, models.Model):
    """Model with date/datetime fields for dates()/datetimes() tests."""

    title = models.CharField(max_length=255)
    date = models.DateField()
    timestamp = models.DateTimeField()

    async_object = AsyncManager()

    class Meta:
        db_table = "test_event"
