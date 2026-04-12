"""Tests for async related manager methods (aadd, aremove, aclear, aset, acreate, etc.)."""

import pytest
from django.db.models import signals
from lookup.models import Article, Author, Game, Season, Tag

# ---------------------------------------------------------------------------
# Reverse FK (Article.author is FK to Author, nullable)
# ---------------------------------------------------------------------------


async def test_reverse_fk_aadd_bulk(async_db):
    author = await Author.async_object.acreate(name="Alice")
    a1 = await Article.async_object.acreate(headline="A1", pub_date="2025-01-01")
    a2 = await Article.async_object.acreate(headline="A2", pub_date="2025-01-02")

    await author.article_set.aadd(a1, a2)

    await a1.arefresh_from_db()
    await a2.arefresh_from_db()
    assert a1.author_id == author.pk
    assert a2.author_id == author.pk


async def test_reverse_fk_aadd_non_bulk(async_db):
    author = await Author.async_object.acreate(name="Bob")
    a1 = await Article.async_object.acreate(headline="B1", pub_date="2025-01-01")

    await author.article_set.aadd(a1, bulk=False)

    await a1.arefresh_from_db()
    assert a1.author_id == author.pk


async def test_reverse_fk_aremove_bulk(async_db):
    author = await Author.async_object.acreate(name="Carol")
    a1 = await Article.async_object.acreate(headline="C1", pub_date="2025-01-01", author=author)

    await author.article_set.aremove(a1)

    await a1.arefresh_from_db()
    assert a1.author_id is None


async def test_reverse_fk_aremove_non_bulk(async_db):
    author = await Author.async_object.acreate(name="Dave")
    a1 = await Article.async_object.acreate(headline="D1", pub_date="2025-01-01", author=author)

    await author.article_set.aremove(a1, bulk=False)

    await a1.arefresh_from_db()
    assert a1.author_id is None


async def test_reverse_fk_aclear_bulk(async_db):
    author = await Author.async_object.acreate(name="Eve")
    await Article.async_object.acreate(headline="E1", pub_date="2025-01-01", author=author)
    await Article.async_object.acreate(headline="E2", pub_date="2025-01-02", author=author)

    await author.article_set.aclear()

    assert await Article.async_object.filter(author=author).acount() == 0


async def test_reverse_fk_aclear_non_bulk(async_db):
    author = await Author.async_object.acreate(name="Frank")
    await Article.async_object.acreate(headline="F1", pub_date="2025-01-01", author=author)

    await author.article_set.aclear(bulk=False)

    assert await Article.async_object.filter(author=author).acount() == 0


async def test_reverse_fk_aset(async_db):
    author = await Author.async_object.acreate(name="Grace")
    a1 = await Article.async_object.acreate(headline="G1", pub_date="2025-01-01", author=author)
    a2 = await Article.async_object.acreate(headline="G2", pub_date="2025-01-02")
    a3 = await Article.async_object.acreate(headline="G3", pub_date="2025-01-03")

    await author.article_set.aset([a2, a3])

    await a1.arefresh_from_db()
    assert a1.author_id is None
    await a2.arefresh_from_db()
    assert a2.author_id == author.pk
    await a3.arefresh_from_db()
    assert a3.author_id == author.pk


async def test_reverse_fk_aset_clear(async_db):
    author = await Author.async_object.acreate(name="Heidi")
    a1 = await Article.async_object.acreate(headline="H1", pub_date="2025-01-01", author=author)
    a2 = await Article.async_object.acreate(headline="H2", pub_date="2025-01-02")

    await author.article_set.aset([a2], clear=True)

    await a1.arefresh_from_db()
    assert a1.author_id is None
    await a2.arefresh_from_db()
    assert a2.author_id == author.pk


async def test_reverse_fk_acreate(async_db):
    author = await Author.async_object.acreate(name="Ivan")

    article = await author.article_set.acreate(headline="I1", pub_date="2025-01-01")

    assert article.author_id == author.pk
    assert await Article.async_object.filter(author=author).acount() == 1


async def test_reverse_fk_aget_or_create(async_db):
    author = await Author.async_object.acreate(name="Judy")

    article, created = await author.article_set.aget_or_create(
        headline="J1", defaults={"pub_date": "2025-01-01"}
    )
    assert created is True
    assert article.author_id == author.pk

    article2, created2 = await author.article_set.aget_or_create(
        headline="J1", defaults={"pub_date": "2025-01-01"}
    )
    assert created2 is False
    assert article2.pk == article.pk


async def test_reverse_fk_aupdate_or_create(async_db):
    author = await Author.async_object.acreate(name="Karl")

    article, created = await author.article_set.aupdate_or_create(
        headline="K1", defaults={"pub_date": "2025-01-01"}
    )
    assert created is True
    assert article.author_id == author.pk


# ---------------------------------------------------------------------------
# M2M (Tag.articles is M2M to Article, Player.games is M2M to Game)
# ---------------------------------------------------------------------------


async def test_m2m_aadd(async_db):
    a1 = await Article.async_object.acreate(headline="M1", pub_date="2025-01-01")
    a2 = await Article.async_object.acreate(headline="M2", pub_date="2025-01-02")
    tag = await Tag.async_object.acreate(name="tech")

    await tag.articles.aadd(a1, a2)

    assert await tag.articles.acount() == 2


async def test_m2m_aadd_idempotent(async_db):
    a1 = await Article.async_object.acreate(headline="M3", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="science")

    await tag.articles.aadd(a1)
    await tag.articles.aadd(a1)  # should not raise

    assert await tag.articles.acount() == 1


async def test_m2m_aremove(async_db):
    a1 = await Article.async_object.acreate(headline="M4", pub_date="2025-01-01")
    a2 = await Article.async_object.acreate(headline="M5", pub_date="2025-01-02")
    tag = await Tag.async_object.acreate(name="art")

    await tag.articles.aadd(a1, a2)
    await tag.articles.aremove(a1)

    assert await tag.articles.acount() == 1
    assert await tag.articles.filter(pk=a2.pk).aexists()


async def test_m2m_aclear(async_db):
    a1 = await Article.async_object.acreate(headline="M6", pub_date="2025-01-01")
    a2 = await Article.async_object.acreate(headline="M7", pub_date="2025-01-02")
    tag = await Tag.async_object.acreate(name="music")

    await tag.articles.aadd(a1, a2)
    await tag.articles.aclear()

    assert await tag.articles.acount() == 0


async def test_m2m_aset(async_db):
    a1 = await Article.async_object.acreate(headline="M8", pub_date="2025-01-01")
    a2 = await Article.async_object.acreate(headline="M9", pub_date="2025-01-02")
    a3 = await Article.async_object.acreate(headline="M10", pub_date="2025-01-03")
    tag = await Tag.async_object.acreate(name="sports")

    await tag.articles.aadd(a1, a2)
    await tag.articles.aset([a2, a3])

    pks = {a.pk async for a in tag.articles.all()}
    assert pks == {a2.pk, a3.pk}


async def test_m2m_aset_clear(async_db):
    a1 = await Article.async_object.acreate(headline="M11", pub_date="2025-01-01")
    a2 = await Article.async_object.acreate(headline="M12", pub_date="2025-01-02")
    tag = await Tag.async_object.acreate(name="film")

    await tag.articles.aadd(a1)
    await tag.articles.aset([a2], clear=True)

    pks = {a.pk async for a in tag.articles.all()}
    assert pks == {a2.pk}


async def test_m2m_acreate(async_db):
    tag = await Tag.async_object.acreate(name="food")

    article = await tag.articles.acreate(headline="M13", pub_date="2025-01-01")

    assert await tag.articles.filter(pk=article.pk).aexists()


async def test_m2m_aget_or_create(async_db):
    tag = await Tag.async_object.acreate(name="travel")

    article, created = await tag.articles.aget_or_create(
        headline="M14", defaults={"pub_date": "2025-01-01"}
    )
    assert created is True
    assert await tag.articles.filter(pk=article.pk).aexists()

    article2, created2 = await tag.articles.aget_or_create(
        headline="M14", defaults={"pub_date": "2025-01-01"}
    )
    assert created2 is False
    assert article2.pk == article.pk


async def test_m2m_aupdate_or_create(async_db):
    tag = await Tag.async_object.acreate(name="health")

    article, created = await tag.articles.aupdate_or_create(
        headline="M15", defaults={"pub_date": "2025-01-01"}
    )
    assert created is True
    assert await tag.articles.filter(pk=article.pk).aexists()


async def test_m2m_signal_pre_post_add(async_db):
    a1 = await Article.async_object.acreate(headline="S1", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="sig-add")

    actions = []

    def handler(sender, action, **kwargs):
        actions.append(action)

    signals.m2m_changed.connect(handler, sender=Tag.articles.through)
    try:
        await tag.articles.aadd(a1)
    finally:
        signals.m2m_changed.disconnect(handler, sender=Tag.articles.through)

    assert "pre_add" in actions
    assert "post_add" in actions


async def test_m2m_signal_pre_post_remove(async_db):
    a1 = await Article.async_object.acreate(headline="S2", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="sig-rm")
    await tag.articles.aadd(a1)

    actions = []

    def handler(sender, action, **kwargs):
        actions.append(action)

    signals.m2m_changed.connect(handler, sender=Tag.articles.through)
    try:
        await tag.articles.aremove(a1)
    finally:
        signals.m2m_changed.disconnect(handler, sender=Tag.articles.through)

    assert "pre_remove" in actions
    assert "post_remove" in actions


async def test_m2m_signal_pre_post_clear(async_db):
    a1 = await Article.async_object.acreate(headline="S3", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="sig-clr")
    await tag.articles.aadd(a1)

    actions = []

    def handler(sender, action, **kwargs):
        actions.append(action)

    signals.m2m_changed.connect(handler, sender=Tag.articles.through)
    try:
        await tag.articles.aclear()
    finally:
        signals.m2m_changed.disconnect(handler, sender=Tag.articles.through)

    assert "pre_clear" in actions
    assert "post_clear" in actions


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


async def test_m2m_aadd_empty(async_db):
    tag = await Tag.async_object.acreate(name="empty")
    await tag.articles.aadd()  # should be a no-op
    assert await tag.articles.acount() == 0


async def test_m2m_aset_empty(async_db):
    a1 = await Article.async_object.acreate(headline="E1", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="to-clear")
    await tag.articles.aadd(a1)

    await tag.articles.aset([])

    assert await tag.articles.acount() == 0


async def test_reverse_fk_aadd_wrong_type(async_db):
    author = await Author.async_object.acreate(name="TypeCheck")
    tag = await Tag.async_object.acreate(name="wrong")

    with pytest.raises(TypeError, match="Article.*instance expected"):
        await author.article_set.aadd(tag)


async def test_reverse_fk_aremove_not_related(async_db):
    author1 = await Author.async_object.acreate(name="Auth1")
    author2 = await Author.async_object.acreate(name="Auth2")
    a1 = await Article.async_object.acreate(headline="NR1", pub_date="2025-01-01", author=author2)

    with pytest.raises(author1.DoesNotExist):
        await author1.article_set.aremove(a1)


async def test_reverse_fk_non_nullable_no_remove(async_db):
    """Reverse FK on non-nullable field should not have aremove/aclear."""
    season = await Season.async_object.acreate(year=2025, gt=100)
    await Game.async_object.acreate(season=season, home="A", away="B")

    assert not hasattr(season.games, "aremove")
    assert not hasattr(season.games, "aclear")


async def test_m2m_reverse_side(async_db):
    """M2M from the reverse side (article.tag_set) also has async methods."""
    a1 = await Article.async_object.acreate(headline="R1", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="rev")

    await a1.tag_set.aadd(tag)

    assert await a1.tag_set.acount() == 1
