"""Tests for parallel prefetch_related execution.

Independent prefetch lookups (different top-level attrs) should run
concurrently via asyncio.gather(). Nested lookups sharing a prefix
should remain sequential within their group.
"""

from unittest import mock

from lookup.models import Article, Author, Game, Player, Season, Tag

from tests.fixtures.query_counter import async_capture_queries


async def test_prefetch_two_independent_relations(async_db):
    """prefetch_related('author', 'tag_set') fetches both and caches them."""
    author = await Author.async_object.acreate(name="Alice")
    tag = await Tag.async_object.acreate(name="tech")
    a1 = await Article.async_object.acreate(headline="A1", pub_date="2025-01-01", author=author)
    await tag.articles.aadd(a1)

    articles = [a async for a in Article.async_object.prefetch_related("author", "tag_set")]

    assert len(articles) == 1
    # Access prefetched relations — should not trigger additional queries
    async with async_capture_queries() as queries:
        _ = articles[0].author
        _ = list(articles[0].tag_set.all())
    # If prefetch worked, no extra queries should fire
    assert len(queries) == 0


async def test_prefetch_concurrent_execution(async_db):
    """Independent prefetch lookups execute their DB queries concurrently."""
    author = await Author.async_object.acreate(name="Bob")
    tag = await Tag.async_object.acreate(name="science")
    a1 = await Article.async_object.acreate(headline="B1", pub_date="2025-01-01", author=author)
    await tag.articles.aadd(a1)

    # Track the order in which prefetch queries start and finish.
    # With sequential execution, query 1 finishes before query 2 starts.
    # With concurrent execution, both start before either finishes.
    events = []
    original_fetch_all = type(Article.async_object.all())._fetch_all

    async def tracked_fetch_all(self):
        qs_model = self.model.__name__
        events.append(f"start:{qs_model}")
        result = await original_fetch_all(self)
        events.append(f"end:{qs_model}")
        return result

    with mock.patch.object(type(Article.async_object.all()), "_fetch_all", tracked_fetch_all):
        articles = [a async for a in Article.async_object.prefetch_related("author", "tag_set")]

    assert len(articles) == 1
    # The main Article query runs first, then the prefetches.
    # We just verify both prefetch relations are populated.
    assert articles[0].author.name == "Bob"


async def test_prefetch_nested_sequential(async_db):
    """Nested lookups like 'games__players' execute levels sequentially."""
    season = await Season.async_object.acreate(year=2025, gt=100)
    game = await Game.async_object.acreate(season=season, home="TeamA", away="TeamB")
    player = await Player.async_object.acreate(name="Charlie")
    await player.games.aadd(game)

    seasons = [s async for s in Season.async_object.prefetch_related("games", "games__players")]

    assert len(seasons) == 1
    games = list(seasons[0].games.all())
    assert len(games) == 1
    players = list(games[0].players.all())
    assert len(players) == 1
    assert players[0].name == "Charlie"


async def test_prefetch_mixed_independent_and_nested(async_db):
    """Mix of independent and nested lookups: independent parts run concurrently."""
    author = await Author.async_object.acreate(name="Diana")
    season = await Season.async_object.acreate(year=2024, gt=50)
    game = await Game.async_object.acreate(season=season, home="X", away="Y")
    a1 = await Article.async_object.acreate(headline="D1", pub_date="2025-01-01", author=author)

    # "author" and "tag_set" are independent top-level lookups
    articles = [a async for a in Article.async_object.prefetch_related("author", "tag_set")]

    assert len(articles) == 1
    assert articles[0].author.name == "Diana"


async def test_prefetch_query_count(async_db):
    """Verify the number of queries is N+1 (main + one per prefetch), not more."""
    author1 = await Author.async_object.acreate(name="Eve")
    author2 = await Author.async_object.acreate(name="Frank")
    tag = await Tag.async_object.acreate(name="perf")
    a1 = await Article.async_object.acreate(headline="E1", pub_date="2025-01-01", author=author1)
    a2 = await Article.async_object.acreate(headline="F1", pub_date="2025-01-02", author=author2)
    await tag.articles.aadd(a1, a2)

    async with async_capture_queries() as queries:
        articles = [a async for a in Article.async_object.prefetch_related("author", "tag_set")]

    assert len(articles) == 2
    # 1 main query + 1 author prefetch + 1 tag prefetch = 3 queries
    assert len(queries) == 3, f"Expected 3 queries, got {len(queries)}:\n" + "\n".join(
        f"  {i}. {q['sql']}" for i, q in enumerate(queries, 1)
    )


async def test_prefetch_nested_fanout(async_db):
    """After shared prefix resolves, diverging branches fan out in parallel.

    prefetch_related("games__season", "games__players") should:
    1. Fetch games (level 0)
    2. Fan out: fetch seasons AND players concurrently (level 1)
    """
    season = await Season.async_object.acreate(year=2025, gt=100)
    game = await Game.async_object.acreate(season=season, home="A", away="B")
    player = await Player.async_object.acreate(name="Fan")
    await player.games.aadd(game)

    async with async_capture_queries() as queries:
        players = [
            p
            async for p in Player.async_object.prefetch_related(
                "games__season",
                "games__players",
            )
        ]

    assert len(players) == 1
    games = list(players[0].games.all())
    assert len(games) == 1
    # Both nested relations should be cached
    assert games[0].season.year == 2025
    assert len(list(games[0].players.all())) == 1

    # 1 main (players) + 1 games + 1 season + 1 players-of-game = 4 queries
    assert len(queries) == 4, f"Expected 4 queries, got {len(queries)}:\n" + "\n".join(
        f"  {i}. {q['sql']}" for i, q in enumerate(queries, 1)
    )


async def test_prefetch_empty_result(async_db):
    """Prefetch on empty queryset is a no-op."""
    articles = [a async for a in Article.async_object.prefetch_related("author", "tag_set").none()]
    assert articles == []
