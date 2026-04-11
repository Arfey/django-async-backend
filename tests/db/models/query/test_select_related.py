from shared.models import Author, Book, EditorNote, Review, TestModel


async def test_select_related_basic(async_db):
    """select_related follows FK and caches the related object."""
    author = await Author.async_object.acreate(name="Author1")
    await Book.async_object.acreate(title="Book1", author=author)

    books = [b async for b in Book.async_object.select_related("author")]
    assert len(books) == 1
    # The author should be cached; accessing it should not need a query
    assert books[0].author.name == "Author1"


async def test_select_related_none_clears(async_db):
    """select_related(None) clears the select_related."""
    qs = Book.async_object.select_related("author").select_related(None)
    assert qs.query.select_related is False


async def test_select_related_all(async_db):
    """select_related() with no args follows all non-null FKs."""
    author = await Author.async_object.acreate(name="A")
    await Book.async_object.acreate(title="B", author=author)

    books = [b async for b in Book.async_object.select_related()]
    assert len(books) == 1
    assert books[0].author.name == "A"


async def test_select_related_chained(async_db):
    """select_related through multiple levels: Review -> Book -> Author."""
    author = await Author.async_object.acreate(name="Deep")
    book = await Book.async_object.acreate(title="DeepBook", author=author)
    await Review.async_object.acreate(text="Good", book=book)

    reviews = [r async for r in Review.async_object.select_related("book__author")]
    assert len(reviews) == 1
    assert reviews[0].book.title == "DeepBook"
    assert reviews[0].book.author.name == "Deep"


async def test_select_related_with_filter(async_db):
    """select_related combined with filter."""
    a1 = await Author.async_object.acreate(name="A1")
    a2 = await Author.async_object.acreate(name="A2")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a2)

    books = [b async for b in Book.async_object.select_related("author").filter(author__name="A1")]
    assert len(books) == 1
    assert books[0].author.name == "A1"


async def test_select_related_nullable_fk(async_db):
    """select_related with a nullable FK (EditorNote.book can be NULL)."""
    author = await Author.async_object.acreate(name="A")
    book = await Book.async_object.acreate(title="B", author=author)
    await EditorNote.async_object.acreate(note="Has book", book=book)
    await EditorNote.async_object.acreate(note="No book", book=None)

    notes = [n async for n in EditorNote.async_object.select_related("book").order_by("note")]
    assert len(notes) == 2
    assert notes[0].book.title == "B"
    assert notes[1].book is None


async def test_select_related_self_referential(async_db):
    """select_related on a self-referential FK."""
    parent = await TestModel.async_object.acreate(name="Parent", value=1)
    child = await TestModel.async_object.acreate(name="Child", value=2, relative=parent)

    children = [t async for t in TestModel.async_object.select_related("relative").filter(name="Child")]
    assert len(children) == 1
    assert children[0].relative.name == "Parent"


async def test_select_related_with_order_by(async_db):
    """select_related with ordering."""
    a1 = await Author.async_object.acreate(name="Z")
    a2 = await Author.async_object.acreate(name="A")
    await Book.async_object.acreate(title="B1", author=a1)
    await Book.async_object.acreate(title="B2", author=a2)

    books = [b async for b in Book.async_object.select_related("author").order_by("author__name")]
    assert len(books) == 2
    assert books[0].author.name == "A"
    assert books[1].author.name == "Z"
