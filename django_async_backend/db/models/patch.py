"""
Runtime patch that gives every Django model the async behavior defined on
``django_async_backend.db.models.base.AsyncModelMixin``.

It exists to add async support to models we can't declare the mixin on:
auto-created models such as m2m "through" models, as well as undecorated and
third-party models.
"""

from django.db.models.base import Model

from django_async_backend.db.models.base import AsyncModelMixin

_ORIGINAL_MODEL_ATTRS = frozenset(dir(Model))
_ASYNC_MIXIN_MEMBERS = {
    name: value
    for name, value in vars(AsyncModelMixin).items()
    if not name.startswith("__")
}

_patched = False


def _patch_model():
    global _patched
    if _patched:  # pragma: no cover
        return
    for name, value in _ASYNC_MIXIN_MEMBERS.items():
        setattr(Model, name, value)
    _patched = True
