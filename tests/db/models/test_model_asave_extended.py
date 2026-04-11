import pytest
from django.db import IntegrityError
from test_app.models import Author, Book, Review, TestModel


async def test_asave_force_update(async_db):
    """force_update=True only does UPDATE, never INSERT."""
    obj = await TestModel.async_object.acreate(name="ForceUp", value=1)
    obj.value = 99
    await obj.asave(force_update=True)

    fetched = await TestModel.async_object.aget(pk=obj.pk)
    assert fetched.value == 99


async def test_asave_force_update_no_pk_raises(async_db):
    """force_update on object without pk raises."""
    obj = TestModel(name="NoPk", value=1)
    with pytest.raises(ValueError):
        await obj.asave(force_update=True)


async def test_asave_force_insert_duplicate_raises(async_db):
    """force_insert with an existing pk raises IntegrityError."""
    obj = await TestModel.async_object.acreate(name="Exists", value=1)
    dupe = TestModel(name="Exists2", value=2)
    dupe.pk = obj.pk
    with pytest.raises(IntegrityError):
        await dupe.asave(force_insert=True)


async def test_asave_force_insert_and_update_mutex(async_db):
    """Cannot use both force_insert and force_update."""
    obj = TestModel(name="Both", value=1)
    with pytest.raises(ValueError):
        await obj.asave(force_insert=True, force_update=True)


async def test_asave_update_fields_empty_list(async_db):
    """update_fields=[] skips the update entirely."""
    obj = await TestModel.async_object.acreate(name="NoUpdate", value=1)
    obj.value = 999
    await obj.asave(update_fields=[])

    fetched = await TestModel.async_object.aget(pk=obj.pk)
    assert fetched.value == 1  # not updated


async def test_asave_sets_state_adding_false(async_db):
    """After save, _state.adding should be False."""
    obj = TestModel(name="StateTest", value=1)
    assert obj._state.adding is True
    await obj.asave()
    assert obj._state.adding is False


async def test_asave_sets_state_db(async_db):
    """After save, _state.db should be set."""
    obj = TestModel(name="DbState", value=1)
    assert obj._state.db is None
    await obj.asave()
    assert obj._state.db == "default"


async def test_arefresh_from_db_clears_cached_relations(async_db):
    """arefresh_from_db clears cached FK objects."""
    a1 = await Author.async_object.acreate(name="Original")
    a2 = await Author.async_object.acreate(name="New")
    book = await Book.async_object.acreate(title="B", author=a1)

    # Access the FK to cache it
    books = [b async for b in Book.async_object.select_related("author").filter(pk=book.pk)]
    book = books[0]
    assert book.author.name == "Original"

    # Update the FK in the database
    await Book.async_object.filter(pk=book.pk).aupdate(author=a2)

    # Refresh should clear the cached author
    await book.arefresh_from_db()
    assert book.author_id == a2.pk


async def test_arefresh_from_db_all_fields(async_db):
    """arefresh_from_db with no fields refreshes everything."""
    obj = await TestModel.async_object.acreate(name="Full", value=1)
    await TestModel.async_object.filter(pk=obj.pk).aupdate(name="Changed", value=99)

    await obj.arefresh_from_db()
    assert obj.name == "Changed"
    assert obj.value == 99


async def test_adelete_with_cascade(async_db):
    """Deleting an author cascades to books and reviews."""
    author = await Author.async_object.acreate(name="Gone")
    book = await Book.async_object.acreate(title="Also Gone", author=author)
    await Review.async_object.acreate(text="Poof", book=book)

    count, per_model = await author.adelete()
    assert count == 3
    assert not await Author.async_object.filter(pk=author.pk).aexists()
    assert not await Book.async_object.filter(pk=book.pk).aexists()
    assert await Review.async_object.acount() == 0
