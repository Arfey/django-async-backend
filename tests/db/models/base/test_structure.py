import inspect
import pathlib

import libcst as cst
from django.test import SimpleTestCase

import django_async_backend.db.models.base as base_module

MIXIN = "AsyncModelMixin"

PUBLIC_API = {
    "async_objects",
    "async_save",
    "async_delete",
    "async_refresh_from_db",
}

EXEMPT = {"_meta", "_state", "NotUpdated"}


def _methods(body):
    """Yield the names of functions defined directly in ``body``."""
    for node in body:
        match node:
            case cst.FunctionDef(name=cst.Name(str() as name)):
                yield name


def _parse_mixin():
    source = pathlib.Path(inspect.getsourcefile(base_module)).read_text()

    for node in cst.parse_module(source).body:
        match node:
            case cst.ClassDef(name=cst.Name(MIXIN)):
                return node

    raise AssertionError("%s not found in %s" % (MIXIN, base_module.__name__))


class SelfCallVisitor(cst.CSTVisitor):

    def __init__(self):
        self.method = None
        self.calls = {}

    def visit_FunctionDef(self, node):
        self.method = node.name.value

    def visit_Call(self, node):
        match node.func:
            case cst.Attribute(
                value=cst.Name("self" | "cls"),
                attr=cst.Name(str() as attr),
            ):
                self.calls.setdefault(self.method, set()).add(attr)
            # self._meta.pk.get_pk_value_on_save(...) and friends: the chain
            # is rooted at an attribute of self, so record that root instead.
            case cst.Attribute(value=value):
                while True:
                    match value:
                        case cst.Attribute(
                            value=cst.Name("self" | "cls"),
                            attr=cst.Name(str() as attr),
                        ):
                            self.calls.setdefault(self.method, set()).add(attr)
                            return
                        case cst.Attribute(value=inner) | cst.Subscript(
                            value=inner
                        ):
                            value = inner
                        case _:
                            return


class AsyncModelMixinStructureTests(SimpleTestCase):
    def setUp(self):
        self.mixin = _parse_mixin()
        self.members = set(_methods(self.mixin.body.body))
        assert self.members

    def test_only_public_api_is_public(self):
        public = sorted(
            name
            for name in self.members
            if not name.startswith("_") and name not in PUBLIC_API
        )

        self.assertEqual(
            public,
            [],
            "%s may only expose %s publicly; make these private"
            % (MIXIN, ", ".join(sorted(PUBLIC_API))),
        )

    def test_members_exclude_nested_functions(self):
        # The prefix rule applies to mixin methods, not to closures defined
        # inside them (such as _is_unset in _async_is_pk_set).
        nested = set()
        for node in self.mixin.body.body:
            match node:
                case cst.FunctionDef(body=cst.BaseSuite() as body):
                    nested.update(_methods(body.body))

        assert nested, "expected at least one nested function in %s" % MIXIN
        self.assertEqual(nested & self.members, set())

    def test_private_methods_are_async_prefixed(self):
        unprefixed = sorted(
            name
            for name in self.members
            if name.startswith("_") and not name.startswith("_async_")
        )

        self.assertEqual(
            unprefixed,
            [],
            "private methods copied into %s must be renamed with an _async "
            "prefix, so a missed sync operation is obvious" % MIXIN,
        )

    def test_calls_are_defined_in_the_mixin(self):
        visitor = SelfCallVisitor()
        self.mixin.visit(visitor)

        assert visitor.calls

        external = sorted(
            "%s() -> self.%s" % (method, attr)
            for method, attrs in visitor.calls.items()
            for attr in attrs
            if attr not in self.members
            and attr not in EXEMPT
            and not attr.startswith("__")
        )

        self.assertEqual(
            external,
            [],
            "these calls are not defined in %s; the mixin must not rely on "
            "the host class (allowed: %s)"
            % (MIXIN, ", ".join(sorted(EXEMPT))),
        )
