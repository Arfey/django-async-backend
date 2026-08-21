import inspect
import pathlib

import libcst as cst
from django.test import SimpleTestCase

import django_async_backend.db.backends.base.base as base_module

THREAD_GUARD = "validate_thread_sharing"
TASK_GUARD = "validate_task_sharing"

EXEMPT = {"close"}


class ValidateThreadSharingVisitor(cst.CSTVisitor):

    def __init__(self):
        self.method = None
        self.guards = {}

    def visit_FunctionDef(self, node):
        self.method = node.name.value

    def visit_Call(self, node):
        match node.func:
            case cst.Attribute(
                value=cst.Name("self"),
                attr=cst.Name(str() as guard),
            ) if guard in (THREAD_GUARD, TASK_GUARD):
                self.guards.setdefault(self.method, set()).add(guard)


class ValidateTaskSharingStructureTests(SimpleTestCase):
    def test_thread_guard_implies_task_guard(self):
        source = pathlib.Path(inspect.getsourcefile(base_module)).read_text()
        visitor = ValidateThreadSharingVisitor()
        cst.parse_module(source).visit(visitor)

        assert visitor.guards

        missing = sorted(
            method
            for method, guards in visitor.guards.items()
            if THREAD_GUARD in guards
            and TASK_GUARD not in guards
            and method not in EXEMPT
        )

        self.assertEqual(
            missing,
            [],
            "these methods call %s() but not %s()"
            % (THREAD_GUARD, TASK_GUARD),
        )
