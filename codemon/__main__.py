from textwrap import dedent

import libcst as cst
from libcst import matchers as m

from .utils import (
    Assign,
    Call,
    Class,
    ContextManagers,
    ForStatement,
    Function,
    Method,
    Module,
    ReturnBlock,
    get_ast,
    get_configs,
    load_config,
    load_file,
    write_ast,
)

DJANGO_VERSION = "6.0"


def assignment_transformer(config: Assign) -> cst.CSTTransformer:
    class AssignmentTransformed(m.MatcherDecoratableTransformer):
        @m.leave(m.Assign())
        def remove_assignment(
            self, original_node: cst.Assign, updated_node: cst.Assign
        ) -> cst.RemovalSentinel | cst.Assign:
            if config.remove:
                return cst.RemoveFromParent()

            return updated_node

    return AssignmentTransformed()


def call_transformer(config: Call) -> cst.CSTTransformer:
    class CallTransformed(m.MatcherDecoratableTransformer):
        def visit_Call(self, node: cst.Call) -> bool:
            return False

        @m.leave(m.Call())
        def leave_call(
            self, original_node: cst.Call, updated_node: cst.Call
        ) -> cst.Await:
            if config.replace_raw:
                updated_node = (
                    cst.parse_module(dedent(config.replace_raw))
                    .body[0]
                    .body[0]
                    .value
                )

            if config.rename:
                if config.rename.func and config.rename.func.attr:
                    updated_node = updated_node.with_changes(
                        func=updated_node.func.with_changes(
                            attr=cst.Name(config.rename.func.attr)
                        )
                    )

                if config.rename.func and config.rename.func.value:
                    updated_node = updated_node.with_changes(
                        func=updated_node.func.with_changes(
                            value=cst.Name(config.rename.func.value)
                        )
                    )

                if config.rename.func and config.rename.func.name:
                    updated_node = updated_node.with_changes(
                        func=cst.Name(config.rename.func.name)
                    )

            if config.to_await:
                updated_node = cst.Await(updated_node)

            return updated_node

    return CallTransformed()


def context_manager_transformer(config: ContextManagers) -> cst.CSTTransformer:
    class WithTransformed(m.MatcherDecoratableTransformer):

        @m.leave(m.With())
        def leave_with(
            self, original_node: cst.With, updated_node: cst.With
        ) -> cst.With:

            if config.to_async:
                updated_node = updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

            return updated_node

    return WithTransformed()


def for_transformer(config: ForStatement) -> cst.CSTTransformer:
    class ForTransformed(m.MatcherDecoratableTransformer):

        @m.leave(m.For())
        def leave_for(
            self, original_node: cst.For, updated_node: cst.For
        ) -> cst.For:

            if config.to_async:
                updated_node = updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

            return updated_node

    return ForTransformed()


def return_transformer(config: ReturnBlock) -> cst.CSTTransformer:
    class ReturnTransformed(m.MatcherDecoratableTransformer):

        @m.leave(m.Return())
        def leave_return(
            self, original_node: cst.Return, updated_node: cst.Return
        ) -> cst.Return:
            if config.replace_raw:
                updated_node = updated_node.with_changes(
                    whitespace_after_return=cst.SimpleWhitespace(" "),
                    value=cst.parse_module(dedent(config.replace_raw))
                    .body[0]
                    .body[0]
                    .value,
                )

            return updated_node

    return ReturnTransformed()


def add_raw_top_to_function(updated_node, add_raw_top):
    blocks = []
    for code in add_raw_top:
        blocks.extend(
            [
                cst.EmptyLine(),
                cst.parse_module(dedent(code)).body[0],
                cst.EmptyLine(),
            ]
        )
    if m.matches(
        updated_node,
        m.FunctionDef(
            body=m.IndentedBlock(
                body=[
                    m.SimpleStatementLine(
                        body=[m.Expr(value=m.SimpleString()), m.ZeroOrMore()]
                    ),
                    m.ZeroOrMore(),
                ]
            )
        ),
    ):
        body = [
            updated_node.body.body[0],
            *blocks,
            *updated_node.body.body[1:],
        ]
    else:
        body = [*blocks, *updated_node.body.body]
    return updated_node.with_changes(
        body=updated_node.body.with_changes(body=body)
    )


def apply_return_blocks(original_node, updated_node, return_blocks):
    for return_config in return_blocks:
        if return_config.call and return_config.call.func:
            matcher = m.Return(
                m.Call(
                    func=m.Attribute(
                        attr=(
                            m.Name(return_config.call.func.attr)
                            if return_config.call.func.attr
                            else m.DoNotCare()
                        ),
                        value=(
                            m.Name(return_config.call.func.value)
                            if return_config.call.func.value
                            else m.DoNotCare()
                        ),
                    )
                )
            )
        elif return_config.call and return_config.call.name:
            matcher = m.Return(m.Call(func=m.Name(return_config.call.name)))
        else:
            matcher = m.Return()
        if m.matches(original_node, matcher):
            updated_node = updated_node.visit(
                return_transformer(return_config)
            )
            break
    return updated_node


def apply_remove(config, original_node, updated_node):
    if config.remove:
        return cst.RemoveFromParent()
    return updated_node


def apply_for_statements(original_node, updated_node, for_statements):
    for for_config in for_statements:
        if for_config.target:
            matcher = m.For(target=m.Name(for_config.target))
        else:
            matcher = m.For()
        if m.matches(original_node, matcher):
            updated_node = updated_node.visit(for_transformer(for_config))
            break
    return updated_node


def function_transformer(name: str, config: Function) -> cst.CSTTransformer:
    class FunctionTransformed(m.MatcherDecoratableTransformer):
        if config.for_statements:
            @m.leave(m.For())
            def for_statement(
                self, original_node: cst.For, updated_node: cst.For
            ) -> cst.For:
                return apply_for_statements(
                    original_node, updated_node, config.for_statements
                )

        if config.return_blocks:
            @m.leave(m.Return())
            def return_block(
                self, original_node: cst.Return, updated_node: cst.Return
            ) -> cst.Return:
                return apply_return_blocks(
                    original_node, updated_node, config.return_blocks
                )

        if config.to_async:

            @m.leave(m.FunctionDef())
            def to_async(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

        if config.rename:

            @m.leave(m.FunctionDef())
            def rename(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(name=cst.Name(config.rename))

        if config.calls:

            @m.leave(m.Call())
            def calls(
                self, original_node: cst.Call, updated_node: cst.Call
            ) -> cst.Await | cst.Call:
                for call_config in config.calls:
                    if call_config.func:
                        matcher = m.Call(
                            func=m.Attribute(
                                attr=(
                                    m.Name(call_config.func.attr)
                                    if call_config.func.attr
                                    else m.DoNotCare()
                                ),
                                value=(
                                    m.Name(call_config.func.value)
                                    if call_config.func.value
                                    else m.DoNotCare()
                                ),
                            )
                        )
                    elif call_config.name:
                        matcher = m.Call(func=m.Name(call_config.name))
                    else:
                        matcher = m.Call()

                    if m.matches(original_node, matcher):
                        updated_node = updated_node.visit(
                            call_transformer(call_config)
                        )
                        break

                return updated_node

        if config.add_raw_top:

            @m.leave(m.FunctionDef())
            def add_raw_top(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return add_raw_top_to_function(
                    updated_node, config.add_raw_top
                )

        if config.remove:

            @m.leave(m.FunctionDef())
            def remove(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.RemovalSentinel:
                return apply_remove(config, original_node, updated_node)

    return FunctionTransformed()


def method_transformer(name: str, config: Method) -> cst.CSTTransformer:
    class MethodTransformed(m.MatcherDecoratableTransformer):

        if config.return_blocks:

            @m.leave(m.Return())
            def return_block(
                self, original_node: cst.Return, updated_node: cst.Return
            ) -> cst.Return:
                return apply_return_blocks(
                    original_node, updated_node, config.return_blocks
                )

        if config.rename:

            @m.leave(m.FunctionDef())
            def rename(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(name=cst.Name(config.rename))

        if config.remove:

            @m.leave(m.FunctionDef())
            def remove(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.RemovalSentinel:
                return apply_remove(config, original_node, updated_node)

        if config.to_async:

            @m.leave(m.FunctionDef())
            def to_async(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

        if config.calls:

            @m.leave(m.Call())
            def calls(
                self, original_node: cst.Call, updated_node: cst.Call
            ) -> cst.Await | cst.Call:
                for call_config in config.calls:
                    if call_config.func:
                        matcher = m.Call(
                            func=m.Attribute(
                                attr=(
                                    m.Name(call_config.func.attr)
                                    if call_config.func.attr
                                    else m.DoNotCare()
                                ),
                                value=(
                                    m.Name(call_config.func.value)
                                    if call_config.func.value
                                    else m.DoNotCare()
                                ),
                            )
                        )
                    elif call_config.name:
                        matcher = m.Call(func=m.Name(call_config.name))
                    else:
                        matcher = m.Call()

                    if m.matches(original_node, matcher):
                        updated_node = updated_node.visit(
                            call_transformer(call_config)
                        )
                        break

                return updated_node

        if config.context_managers:

            @m.leave(m.With())
            def context_managers(
                self, original_node: cst.With, updated_node: cst.With
            ) -> cst.With:
                for context_config in config.context_managers:
                    if context_config.asname:
                        matcher = m.With(
                            items=[
                                m.WithItem(
                                    asname=m.AsName(
                                        m.Name(context_config.asname)
                                    )
                                )
                            ]
                        )
                    else:
                        matcher = m.With()

                    if m.matches(original_node, matcher):
                        updated_node = updated_node.visit(
                            context_manager_transformer(context_config)
                        )
                        break

                return updated_node

        if config.add_raw_top:

            @m.leave(m.FunctionDef())
            def add_raw_top(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return add_raw_top_to_function(
                    updated_node, config.add_raw_top
                )

        if config.for_statements:

            @m.leave(m.For())
            def for_statement(
                self, original_node: cst.For, updated_node: cst.For
            ) -> cst.For:
                return apply_for_statements(
                    original_node, updated_node, config.for_statements
                )

    return MethodTransformed()


def class_transformer(name: str, config: Class) -> cst.CSTTransformer:
    class ClassTransformed(m.MatcherDecoratableTransformer):

        if config.rename:

            @m.leave(m.ClassDef())
            def rename(
                self, original_node: cst.ClassDef, updated_node: cst.ClassDef
            ) -> cst.ClassDef:
                return updated_node.with_changes(name=cst.Name(config.rename))

        if config.methods:

            @m.leave(
                m.FunctionDef(
                    name=m.OneOf(
                        *[m.Name(name) for name in config.methods.keys()]
                    ),
                )
            )
            def method_visit(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef | cst.RemovalSentinel:
                name = original_node.name.value
                return updated_node.visit(
                    method_transformer(name, config.methods[name])
                )

        if config.assigns:

            @m.leave(m.Assign())
            def leave_assign(
                self, original_node: cst.Assign, updated_node: cst.Assign
            ) -> cst.RemovalSentinel | cst.Assign:
                for assign_config in config.assigns:
                    if assign_config.target.name:
                        matcher = m.Assign(
                            targets=[
                                m.ZeroOrMore(),
                                m.AssignTarget(
                                    m.Attribute(
                                        m.Name(assign_config.target.name)
                                    )
                                ),
                                m.ZeroOrMore(),
                            ]
                        )
                    else:
                        matcher = m.Assign()

                    if m.matches(original_node, matcher):
                        updated_node = updated_node.visit(
                            assignment_transformer(assign_config)
                        )

                return updated_node

    return ClassTransformed()


def module_transformer(config: Module) -> cst.CSTTransformer:
    class ModuleTransformed(m.MatcherDecoratableTransformer):

        def leave_ClassDef(
            self, original_node: cst.ClassDef, updated_node: cst.ClassDef
        ) -> cst.ClassDef:
            if config.classes and original_node.name.value in config.classes:
                updated_node = updated_node.visit(
                    class_transformer(
                        original_node.name.value,
                        config.classes[original_node.name.value],
                    )
                )

            return updated_node

        if config.new_imports:

            @m.leave(m.Module())
            def add_imports(
                self, original_node: cst.Module, updated_node: cst.Module
            ) -> cst.Module:
                return updated_node.with_changes(
                    body=[
                        *[
                            cst.parse_module(dedent(i)).body[0]
                            for i in config.new_imports
                        ],
                        *[i for i in updated_node.body],
                    ]
                )

        if config.import_aliases:

            @m.leave(m.ImportFrom())
            def import_aliases(
                self,
                original_node: cst.ImportFrom,
                updated_node: cst.ImportFrom,
            ) -> cst.ImportFrom:
                for alias_config in config.import_aliases:
                    if alias_config.name:
                        matcher = m.ImportFrom(
                            names=[
                                m.ZeroOrMore(),
                                m.ImportAlias(m.Name(alias_config.name)),
                                m.ZeroOrMore(),
                            ]
                        )
                    else:
                        matcher = m.ImportFrom()

                    if m.matches(original_node, matcher):
                        if alias_config.remove:
                            updated_node = updated_node.with_changes(
                                names=[
                                    name.with_changes(
                                        comma=cst.MaybeSentinel.DEFAULT
                                    )
                                    for name in updated_node.names
                                    if name.name.value != alias_config.name
                                ]
                            )

                return updated_node

        if config.add_raw_bottom:

            @m.leave(m.Module())
            def add_raw_bottom(
                self, original_node: cst.Module, updated_node: cst.Module
            ) -> cst.Module:
                blocks = []

                for code in config.add_raw_bottom:
                    blocks.extend(
                        [
                            cst.EmptyLine(),
                            cst.EmptyLine(),
                            cst.parse_module(dedent(code)).body[0],
                        ]
                    )

                return updated_node.with_changes(
                    body=[
                        *[i for i in updated_node.body],
                        *blocks,
                    ]
                )

        if config.functions:

            @m.leave(m.Module())
            def global_functions(
                self, original_node: cst.Module, updated_node: cst.Module
            ) -> cst.Module:
                pattern = m.FunctionDef(
                    name=m.OneOf(
                        *[m.Name(name) for name in config.functions.keys()]
                    ),
                )
                body = []
                for item in updated_node.body:
                    if m.matches(item, pattern):
                        name = item.name.value
                        item = item.visit(
                            function_transformer(name, config.functions[name])
                        )

                    body.append(item)

                return updated_node.with_changes(body=body)

    return ModuleTransformed()


if __name__ == "__main__":
    for config in get_configs():
        config = load_config(config)

        load_file(config=config, version=DJANGO_VERSION)
        ast = get_ast(config=config)

        ast = ast.visit(module_transformer(config.module))

        write_ast(ast, config=config, version=DJANGO_VERSION)
