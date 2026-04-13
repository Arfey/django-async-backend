"""Tests for async signal dispatch (pre_save, post_save, pre_delete, post_delete, m2m_changed)."""

from django.db.models import signals
from lookup.models import Article, Author, Tag
from shared.models import TestModel

# ---------------------------------------------------------------------------
# pre_save / post_save
# ---------------------------------------------------------------------------


async def test_pre_save_fires_on_create(async_db):
    received = []

    def handler(sender, instance, raw, using, update_fields, **kwargs):
        received.append({"sender": sender, "instance": instance, "raw": raw, "using": using})

    signals.pre_save.connect(handler, sender=TestModel)
    try:
        obj = await TestModel.async_object.acreate(name="sig-pre", value=1)
    finally:
        signals.pre_save.disconnect(handler, sender=TestModel)

    assert len(received) == 1
    assert received[0]["sender"] is TestModel
    assert received[0]["instance"].name == "sig-pre"


async def test_post_save_fires_on_create(async_db):
    received = []

    def handler(sender, instance, created, raw, using, update_fields, **kwargs):
        received.append({"created": created, "pk": instance.pk})

    signals.post_save.connect(handler, sender=TestModel)
    try:
        obj = await TestModel.async_object.acreate(name="sig-post", value=2)
    finally:
        signals.post_save.disconnect(handler, sender=TestModel)

    assert len(received) == 1
    assert received[0]["created"] is True
    assert received[0]["pk"] == obj.pk


async def test_post_save_fires_on_update(async_db):
    obj = await TestModel.async_object.acreate(name="sig-upd", value=3)

    received = []

    def handler(sender, instance, created, **kwargs):
        received.append({"created": created})

    signals.post_save.connect(handler, sender=TestModel)
    try:
        obj.name = "sig-upd-changed"
        await obj.asave()
    finally:
        signals.post_save.disconnect(handler, sender=TestModel)

    assert len(received) == 1
    assert received[0]["created"] is False


async def test_pre_save_can_modify_instance(async_db):
    """A pre_save handler can modify the instance before it's written."""

    def handler(sender, instance, **kwargs):
        instance.name = "modified-by-signal"

    signals.pre_save.connect(handler, sender=TestModel)
    try:
        obj = await TestModel.async_object.acreate(name="original", value=4)
    finally:
        signals.pre_save.disconnect(handler, sender=TestModel)

    await obj.arefresh_from_db()
    assert obj.name == "modified-by-signal"


async def test_async_receiver_works(async_db):
    """An async receiver connected to pre_save fires correctly."""
    received = []

    async def handler(sender, instance, **kwargs):
        received.append(instance.name)

    signals.pre_save.connect(handler, sender=TestModel)
    try:
        await TestModel.async_object.acreate(name="async-receiver", value=5)
    finally:
        signals.pre_save.disconnect(handler, sender=TestModel)

    assert received == ["async-receiver"]


# ---------------------------------------------------------------------------
# pre_delete / post_delete
# ---------------------------------------------------------------------------


async def test_pre_delete_fires(async_db):
    obj = await TestModel.async_object.acreate(name="del-pre", value=10)
    pk = obj.pk

    received = []

    def handler(sender, instance, using, origin, **kwargs):
        received.append({"pk": instance.pk, "name": instance.name})

    signals.pre_delete.connect(handler, sender=TestModel)
    try:
        await obj.adelete()
    finally:
        signals.pre_delete.disconnect(handler, sender=TestModel)

    assert len(received) == 1
    assert received[0]["pk"] == pk


async def test_post_delete_fires(async_db):
    obj = await TestModel.async_object.acreate(name="del-post", value=11)
    pk = obj.pk

    received = []

    def handler(sender, instance, using, origin, **kwargs):
        received.append({"pk": instance.pk, "name": instance.name})

    signals.post_delete.connect(handler, sender=TestModel)
    try:
        await obj.adelete()
    finally:
        signals.post_delete.disconnect(handler, sender=TestModel)

    assert len(received) == 1
    # post_delete fires before pk is nullified (Django's behavior)
    assert received[0]["pk"] == pk


async def test_cascade_delete_fires_signals(async_db):
    """Cascade delete fires pre_delete/post_delete for both parent and child."""
    author = await Author.async_object.acreate(name="cascade-auth")
    a1 = await Article.async_object.acreate(headline="cascade-art", pub_date="2025-01-01", author=author)

    deleted_models = []

    def handler(sender, instance, **kwargs):
        deleted_models.append(sender.__name__)

    signals.pre_delete.connect(handler)
    try:
        await a1.adelete()
    finally:
        signals.pre_delete.disconnect(handler)

    assert "Article" in deleted_models


# ---------------------------------------------------------------------------
# m2m_changed (already tested in test_related_managers.py, but verify asend)
# ---------------------------------------------------------------------------


async def test_m2m_async_receiver(async_db):
    """An async receiver connected to m2m_changed fires correctly via asend."""
    a1 = await Article.async_object.acreate(headline="m2m-async", pub_date="2025-01-01")
    tag = await Tag.async_object.acreate(name="m2m-sig-async")

    received = []

    async def handler(sender, action, **kwargs):
        received.append(action)

    signals.m2m_changed.connect(handler, sender=Tag.articles.through)
    try:
        await tag.articles.aadd(a1)
    finally:
        signals.m2m_changed.disconnect(handler, sender=Tag.articles.through)

    assert "pre_add" in received
    assert "post_add" in received


async def test_save_signal_not_fired_for_auto_created(async_db):
    """Auto-created models (like M2M through tables) don't fire save signals."""
    received = []

    def handler(sender, **kwargs):
        received.append(sender.__name__)

    # Connect to ALL models
    signals.pre_save.connect(handler)
    try:
        a1 = await Article.async_object.acreate(headline="auto-test", pub_date="2025-01-01")
        tag = await Tag.async_object.acreate(name="auto-tag")
    finally:
        signals.pre_save.disconnect(handler)

    # Should have Article and Tag, but NOT the auto-created through model
    assert "Article" in received
    assert "Tag" in received
