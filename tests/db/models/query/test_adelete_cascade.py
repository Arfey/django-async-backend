"""Tests for AsyncCollector edge cases: CASCADE chains, SET_NULL, PROTECT, RESTRICT."""

import pytest
from django.db.models.deletion import ProtectedError, RestrictedError
from shared.models import Author, Book, EditorNote, ProtectedComment, RestrictedTag, Review, TestModel

# --- CASCADE chain: Author -> Book -> Review ---


async def test_delete_author_cascades_books_and_reviews(async_db):
    author = await Author.async_object.acreate(name="Tolkien")
    book1 = await Book.async_object.acreate(title="LOTR", author=author)
    book2 = await Book.async_object.acreate(title="Hobbit", author=author)
    await Review.async_object.acreate(text="Great", book=book1)
    await Review.async_object.acreate(text="Amazing", book=book1)
    await Review.async_object.acreate(text="Classic", book=book2)

    count, _per_model = await Author.async_object.filter(pk=author.pk).adelete()
    assert await Author.async_object.acount() == 0
    assert await Book.async_object.acount() == 0
    assert await Review.async_object.acount() == 0
    assert count >= 6  # 1 author + 2 books + 3 reviews


async def test_delete_book_cascades_reviews_only(async_db):
    author = await Author.async_object.acreate(name="Tolkien")
    book1 = await Book.async_object.acreate(title="LOTR", author=author)
    book2 = await Book.async_object.acreate(title="Hobbit", author=author)
    await Review.async_object.acreate(text="Great", book=book1)
    await Review.async_object.acreate(text="Amazing", book=book1)
    await Review.async_object.acreate(text="Classic", book=book2)

    _count, _ = await Book.async_object.filter(pk=book1.pk).adelete()
    assert await Author.async_object.acount() == 1  # author untouched
    assert await Book.async_object.acount() == 1  # book2 remains
    assert await Review.async_object.acount() == 1  # only review3 remains


async def test_delete_review_no_cascade(async_db):
    author = await Author.async_object.acreate(name="Tolkien")
    book1 = await Book.async_object.acreate(title="LOTR", author=author)
    book2 = await Book.async_object.acreate(title="Hobbit", author=author)
    review1 = await Review.async_object.acreate(text="Great", book=book1)
    await Review.async_object.acreate(text="Amazing", book=book1)
    await Review.async_object.acreate(text="Classic", book=book2)

    count, _ = await Review.async_object.filter(pk=review1.pk).adelete()
    assert count == 1
    assert await Book.async_object.acount() == 2
    assert await Review.async_object.acount() == 2


async def test_model_instance_delete_cascades(async_db):
    """Model.adelete() should also cascade."""
    author = await Author.async_object.acreate(name="Tolkien")
    book1 = await Book.async_object.acreate(title="LOTR", author=author)
    await Book.async_object.acreate(title="Hobbit", author=author)
    await Review.async_object.acreate(text="Great", book=book1)

    await author.adelete()
    assert author.pk is None
    assert await Book.async_object.acount() == 0
    assert await Review.async_object.acount() == 0


# --- SET_NULL: deleting book nullifies editor_note.book ---


async def test_delete_book_nullifies_editor_note(async_db):
    author = await Author.async_object.acreate(name="Author1")
    book = await Book.async_object.acreate(title="Book1", author=author)
    note = await EditorNote.async_object.acreate(note="Good", book=book)

    await Book.async_object.filter(pk=book.pk).adelete()
    await note.arefresh_from_db()
    assert note.book_id is None
    assert await EditorNote.async_object.acount() == 1  # note still exists


async def test_delete_author_cascades_book_and_nullifies_note(async_db):
    author = await Author.async_object.acreate(name="Author1")
    book = await Book.async_object.acreate(title="Book1", author=author)
    note = await EditorNote.async_object.acreate(note="Good", book=book)

    await Author.async_object.filter(pk=author.pk).adelete()
    assert await Book.async_object.acount() == 0
    await note.arefresh_from_db()
    assert note.book_id is None


# --- PROTECT: cannot delete book if it has protected comments ---


async def test_delete_book_with_protected_comment_raises(async_db):
    author = await Author.async_object.acreate(name="Author2")
    book = await Book.async_object.acreate(title="Book2", author=author)
    await ProtectedComment.async_object.acreate(text="Protected", book=book)

    with pytest.raises(ProtectedError):
        await Book.async_object.filter(pk=book.pk).adelete()
    # Book and comment should still exist
    assert await Book.async_object.acount() == 1
    assert await ProtectedComment.async_object.acount() == 1


async def test_delete_book_after_removing_comment(async_db):
    author = await Author.async_object.acreate(name="Author2")
    book = await Book.async_object.acreate(title="Book2", author=author)
    comment = await ProtectedComment.async_object.acreate(text="Protected", book=book)

    await ProtectedComment.async_object.filter(pk=comment.pk).adelete()
    count, _ = await Book.async_object.filter(pk=book.pk).adelete()
    assert count >= 1
    assert await Book.async_object.acount() == 0


# --- RESTRICT: like PROTECT but allows if the restricted object is also being deleted ---


async def test_delete_book_with_restricted_tag_raises(async_db):
    author = await Author.async_object.acreate(name="Author3")
    book = await Book.async_object.acreate(title="Book3", author=author)
    await RestrictedTag.async_object.acreate(label="Fiction", book=book)

    with pytest.raises(RestrictedError):
        await Book.async_object.filter(pk=book.pk).adelete()
    assert await Book.async_object.acount() == 1


async def test_delete_book_after_removing_tag(async_db):
    author = await Author.async_object.acreate(name="Author3")
    book = await Book.async_object.acreate(title="Book3", author=author)
    tag = await RestrictedTag.async_object.acreate(label="Fiction", book=book)

    await RestrictedTag.async_object.filter(pk=tag.pk).adelete()
    count, _ = await Book.async_object.filter(pk=book.pk).adelete()
    assert count >= 1


# --- Mixed on_delete behaviours ---


async def test_delete_book_cascades_reviews_and_nullifies_notes(async_db):
    """Without PROTECT comments, deletion should cascade reviews and nullify notes."""
    author = await Author.async_object.acreate(name="MixedAuthor")
    book = await Book.async_object.acreate(title="MixedBook", author=author)
    await Review.async_object.acreate(text="Good", book=book)
    note = await EditorNote.async_object.acreate(note="Fine", book=book)

    _count, _ = await Book.async_object.filter(pk=book.pk).adelete()
    assert await Review.async_object.acount() == 0  # cascaded
    await note.arefresh_from_db()
    assert note.book_id is None  # nullified


async def test_delete_blocked_by_protect(async_db):
    """Adding a PROTECT comment should block deletion entirely."""
    author = await Author.async_object.acreate(name="MixedAuthor")
    book = await Book.async_object.acreate(title="MixedBook", author=author)
    await Review.async_object.acreate(text="Good", book=book)
    await EditorNote.async_object.acreate(note="Fine", book=book)

    await ProtectedComment.async_object.acreate(text="Blocks", book=book)
    with pytest.raises(ProtectedError):
        await Book.async_object.filter(pk=book.pk).adelete()
    # Everything should still exist
    assert await Review.async_object.acount() == 1
    assert await EditorNote.async_object.acount() == 1


# --- Self-referential deep CASCADE (grandchildren) ---


async def test_delete_root_cascades_all(async_db):
    root = await TestModel.async_object.acreate(name="Root", value=0)
    child = await TestModel.async_object.acreate(name="Child", value=1, relative=root)
    await TestModel.async_object.acreate(name="Grandchild", value=2, relative=child)

    count, _ = await TestModel.async_object.filter(pk=root.pk).adelete()
    assert count == 3
    assert await TestModel.async_object.acount() == 0


async def test_delete_child_cascades_grandchild(async_db):
    root = await TestModel.async_object.acreate(name="Root", value=0)
    child = await TestModel.async_object.acreate(name="Child", value=1, relative=root)
    await TestModel.async_object.acreate(name="Grandchild", value=2, relative=child)

    count, _ = await TestModel.async_object.filter(pk=child.pk).adelete()
    assert count == 2
    assert await TestModel.async_object.acount() == 1  # only root
