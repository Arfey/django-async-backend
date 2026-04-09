"""Tests for AsyncCollector edge cases: CASCADE chains, SET_NULL, PROTECT, RESTRICT."""

from django.db.models.deletion import ProtectedError, RestrictedError
from test_app.models import Author, Book, EditorNote, ProtectedComment, RestrictedTag, Review

from django_async_backend.test import AsyncioTestCase


class TestCascadeChain(AsyncioTestCase):
    """Test multi-level CASCADE: Author → Book → Review"""

    async def asyncSetUp(self):
        self.author = await Author.async_object.acreate(name="Tolkien")
        self.book1 = await Book.async_object.acreate(title="LOTR", author=self.author)
        self.book2 = await Book.async_object.acreate(title="Hobbit", author=self.author)
        self.review1 = await Review.async_object.acreate(text="Great", book=self.book1)
        self.review2 = await Review.async_object.acreate(text="Amazing", book=self.book1)
        self.review3 = await Review.async_object.acreate(text="Classic", book=self.book2)

    async def test_delete_author_cascades_books_and_reviews(self):
        count, _per_model = await Author.async_object.filter(pk=self.author.pk).adelete()
        self.assertEqual(await Author.async_object.acount(), 0)
        self.assertEqual(await Book.async_object.acount(), 0)
        self.assertEqual(await Review.async_object.acount(), 0)
        self.assertGreaterEqual(count, 6)  # 1 author + 2 books + 3 reviews

    async def test_delete_book_cascades_reviews_only(self):
        _count, _ = await Book.async_object.filter(pk=self.book1.pk).adelete()
        self.assertEqual(await Author.async_object.acount(), 1)  # author untouched
        self.assertEqual(await Book.async_object.acount(), 1)  # book2 remains
        self.assertEqual(await Review.async_object.acount(), 1)  # only review3 remains

    async def test_delete_review_no_cascade(self):
        count, _ = await Review.async_object.filter(pk=self.review1.pk).adelete()
        self.assertEqual(count, 1)
        self.assertEqual(await Book.async_object.acount(), 2)
        self.assertEqual(await Review.async_object.acount(), 2)

    async def test_model_instance_delete_cascades(self):
        """Model.adelete() should also cascade."""
        await self.author.adelete()
        self.assertIsNone(self.author.pk)
        self.assertEqual(await Book.async_object.acount(), 0)
        self.assertEqual(await Review.async_object.acount(), 0)


class TestSetNull(AsyncioTestCase):
    """Test SET_NULL: deleting book nullifies editor_note.book"""

    async def asyncSetUp(self):
        self.author = await Author.async_object.acreate(name="Author1")
        self.book = await Book.async_object.acreate(title="Book1", author=self.author)
        self.note = await EditorNote.async_object.acreate(note="Good", book=self.book)

    async def test_delete_book_nullifies_editor_note(self):
        await Book.async_object.filter(pk=self.book.pk).adelete()
        await self.note.arefresh_from_db()
        self.assertIsNone(self.note.book_id)
        self.assertEqual(await EditorNote.async_object.acount(), 1)  # note still exists

    async def test_delete_author_cascades_book_and_nullifies_note(self):
        await Author.async_object.filter(pk=self.author.pk).adelete()
        self.assertEqual(await Book.async_object.acount(), 0)
        await self.note.arefresh_from_db()
        self.assertIsNone(self.note.book_id)


class TestProtect(AsyncioTestCase):
    """Test PROTECT: cannot delete book if it has protected comments"""

    async def asyncSetUp(self):
        self.author = await Author.async_object.acreate(name="Author2")
        self.book = await Book.async_object.acreate(title="Book2", author=self.author)
        self.comment = await ProtectedComment.async_object.acreate(text="Protected", book=self.book)

    async def test_delete_book_with_protected_comment_raises(self):
        with self.assertRaises(ProtectedError):
            await Book.async_object.filter(pk=self.book.pk).adelete()
        # Book and comment should still exist
        self.assertEqual(await Book.async_object.acount(), 1)
        self.assertEqual(await ProtectedComment.async_object.acount(), 1)

    async def test_delete_book_after_removing_comment(self):
        await ProtectedComment.async_object.filter(pk=self.comment.pk).adelete()
        count, _ = await Book.async_object.filter(pk=self.book.pk).adelete()
        self.assertGreaterEqual(count, 1)
        self.assertEqual(await Book.async_object.acount(), 0)


class TestRestrict(AsyncioTestCase):
    """Test RESTRICT: like PROTECT but allows if the restricted object is also being deleted"""

    async def asyncSetUp(self):
        self.author = await Author.async_object.acreate(name="Author3")
        self.book = await Book.async_object.acreate(title="Book3", author=self.author)
        self.tag = await RestrictedTag.async_object.acreate(label="Fiction", book=self.book)

    async def test_delete_book_with_restricted_tag_raises(self):
        with self.assertRaises(RestrictedError):
            await Book.async_object.filter(pk=self.book.pk).adelete()
        self.assertEqual(await Book.async_object.acount(), 1)

    async def test_delete_book_after_removing_tag(self):
        await RestrictedTag.async_object.filter(pk=self.tag.pk).adelete()
        count, _ = await Book.async_object.filter(pk=self.book.pk).adelete()
        self.assertGreaterEqual(count, 1)


class TestMixedOnDelete(AsyncioTestCase):
    """Test a book with CASCADE reviews, SET_NULL notes, and PROTECT comments simultaneously"""

    async def asyncSetUp(self):
        self.author = await Author.async_object.acreate(name="MixedAuthor")
        self.book = await Book.async_object.acreate(title="MixedBook", author=self.author)
        self.review = await Review.async_object.acreate(text="Good", book=self.book)
        self.note = await EditorNote.async_object.acreate(note="Fine", book=self.book)

    async def test_delete_book_cascades_reviews_and_nullifies_notes(self):
        """Without PROTECT comments, deletion should cascade reviews and nullify notes."""
        _count, _ = await Book.async_object.filter(pk=self.book.pk).adelete()
        self.assertEqual(await Review.async_object.acount(), 0)  # cascaded
        await self.note.arefresh_from_db()
        self.assertIsNone(self.note.book_id)  # nullified

    async def test_delete_blocked_by_protect(self):
        """Adding a PROTECT comment should block deletion entirely."""
        await ProtectedComment.async_object.acreate(text="Blocks", book=self.book)
        with self.assertRaises(ProtectedError):
            await Book.async_object.filter(pk=self.book.pk).adelete()
        # Everything should still exist
        self.assertEqual(await Review.async_object.acount(), 1)
        self.assertEqual(await EditorNote.async_object.acount(), 1)


class TestSelfReferentialDeepCascade(AsyncioTestCase):
    """Test deep self-referential CASCADE (grandchildren)."""

    async def asyncSetUp(self):
        self.root = await TestModel.async_object.acreate(name="Root", value=0)
        self.child = await TestModel.async_object.acreate(name="Child", value=1, relative=self.root)
        self.grandchild = await TestModel.async_object.acreate(name="Grandchild", value=2, relative=self.child)

    async def test_delete_root_cascades_all(self):
        count, _ = await TestModel.async_object.filter(pk=self.root.pk).adelete()
        self.assertEqual(count, 3)
        self.assertEqual(await TestModel.async_object.acount(), 0)

    async def test_delete_child_cascades_grandchild(self):
        count, _ = await TestModel.async_object.filter(pk=self.child.pk).adelete()
        self.assertEqual(count, 2)
        self.assertEqual(await TestModel.async_object.acount(), 1)  # only root


# Import TestModel here to avoid issues with test collection
from test_app.models import TestModel  # noqa: E402
