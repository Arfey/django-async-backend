# flake8: noqa: C901

from textwrap import dedent

import libcst as cst
from libcst import matchers as m

from .utils import (
    Assign,
    Attr,
    Call,
    Class,
    CompForBlock,
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

DJANGO_VERSION = "9c655e98006dec4f4fd47a84c254f36404d631e4"


def attr_matcher(config: Attr) -> m.BaseMatcherNode:
    """Matcher for a reference expression: a bare Name or an Attribute."""
    if config.name:
        return m.Name(config.name)

    if config.value:
        value = m.Name(config.value)
    elif config.parent_attr:
        value = m.Attribute(attr=m.Name(config.parent_attr))
    else:
        value = m.DoNotCare()

    return m.Attribute(
        value=value,
        attr=m.Name(config.attr) if config.attr else m.DoNotCare(),
    )


def attr_has_changes(config: Attr) -> bool:
    return bool(
        config.rename
        or config.to_call_method
        or config.wrap
        or config.to_await
        or config.to_async_comp
    )


def to_async_comp(node: cst.BaseExpression, target: str) -> cst.ListComp:
    """``<iterable>`` -> ``[obj async for obj in <iterable>]``."""
    return cst.ListComp(
        elt=cst.Name(target),
        for_in=cst.CompFor(
            target=cst.Name(target),
            iter=node,
            asynchronous=cst.Asynchronous(),
        ),
    )


def call_args(args: list[str] | None) -> list[cst.Arg]:
    """Parse raw argument sources, so kwargs and unpacking keep working."""
    if not args:
        return []

    call = cst.ensure_type(
        cst.parse_expression(f"_({', '.join(args)})"), cst.Call
    )

    return list(call.args)


def apply_attr(config: Attr, updated_node: cst.BaseExpression):
    if config.rename:
        if config.rename.attr:
            updated_node = updated_node.with_changes(
                attr=cst.Name(config.rename.attr)
            )

        if config.rename.value:
            updated_node = updated_node.with_changes(
                value=cst.Name(config.rename.value)
            )

        if config.rename.name:
            updated_node = cst.Name(
                config.rename.name,
                lpar=updated_node.lpar,
                rpar=updated_node.rpar,
            )

    if config.to_call_method:
        updated_node = cst.Call(
            func=cst.Attribute(
                value=updated_node, attr=cst.Name(config.to_call_method.name)
            ),
            args=call_args(config.to_call_method.args),
        )

    if config.wrap:
        updated_node = cst.Call(
            func=cst.Name(config.wrap), args=[cst.Arg(updated_node)]
        )

    if config.to_async_comp:
        updated_node = to_async_comp(updated_node, config.to_async_comp)

    if config.to_await:
        updated_node = cst.Await(updated_node)

    return updated_node


def attr_needs_call(config: Attr) -> bool:
    """``to_async_comp`` iterates the call a reference heads, not the
    reference, so those attrs are matched on a call's ``func`` instead. A
    bare ``name`` heads no call, so it is iterated where it is referenced."""
    return bool(config.to_async_comp and not config.name)


def apply_attrs(original_node, updated_node, attrs):
    for attr_config in attrs:
        if m.matches(original_node, attr_matcher(attr_config)):
            return apply_attr(attr_config, updated_node)

    return updated_node


def apply_boolean_operations(
    original_node, updated_node, boolean_operations
) -> cst.BooleanOperation:
    for config in boolean_operations:
        for operand in config.operands or []:
            matcher = attr_matcher(operand)
            changes = {}

            if m.matches(updated_node.left, matcher):
                changes["left"] = apply_attr(operand, updated_node.left)

            if m.matches(updated_node.right, matcher):
                changes["right"] = apply_attr(operand, updated_node.right)

            if changes:
                updated_node = updated_node.with_changes(**changes)

    return updated_node


def assign_value_transformer(attrs: list[Attr]) -> cst.CSTTransformer:
    class AssignValueTransformed(m.MatcherDecoratableTransformer):
        @m.leave(m.Name())
        def rename_references(
            self, original_node: cst.Name, updated_node: cst.Name
        ) -> cst.BaseExpression:
            return apply_attrs(original_node, updated_node, attrs)

    return AssignValueTransformed()


def assignment_transformer(config: Assign) -> cst.CSTTransformer:
    class AssignmentTransformed(m.MatcherDecoratableTransformer):
        @m.leave(m.Assign())
        def remove_assignment(
            self, original_node: cst.Assign, updated_node: cst.Assign
        ) -> cst.RemovalSentinel | cst.Assign:
            if config.remove:
                return cst.RemoveFromParent()

            matcher = attr_matcher(config.target)

            updated_node = updated_node.with_changes(
                targets=[
                    (
                        target.with_changes(
                            target=apply_attr(config.target, target.target)
                        )
                        if m.matches(target.target, matcher)
                        else target
                    )
                    for target in updated_node.targets
                ]
            )

            if config.attrs:
                updated_node = updated_node.with_changes(
                    value=updated_node.value.visit(
                        assign_value_transformer(config.attrs)
                    )
                )

            return updated_node

    return AssignmentTransformed()


def comp_for_block_transformer(config: CompForBlock) -> cst.CSTTransformer:
    class CompForTransformed(m.MatcherDecoratableTransformer):
        @m.leave(m.CompFor())
        def leave_with(
            self, original_node: cst.CompFor, updated_node: cst.CompFor
        ) -> cst.CompFor:
            if config.to_async:
                updated_node = updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

            return updated_node

    return CompForTransformed()


def apply_comp_for_statements(original_node, updated_node, for_statements):
    for for_config in for_statements:
        if for_config.target:
            matcher = m.CompFor(target=m.Name(for_config.target.name))
        else:
            matcher = m.CompFor()
        if m.matches(original_node, matcher):
            updated_node = updated_node.visit(
                comp_for_block_transformer(for_config)
            )
            break
    return updated_node


def arg_matcher(config: Attr) -> m.BaseMatcherNode:
    """Matcher for an argument: a call on the reference, or the reference."""
    matcher = attr_matcher(config)

    return m.Arg(value=m.Call(func=matcher) | matcher)


def apply_args(original_node: cst.Call, updated_node: cst.Call, config: Attr):
    """Applies attr actions to the matching argument of a call."""
    return updated_node.with_changes(
        args=[
            (
                updated_arg.with_changes(
                    value=apply_attr(config, updated_arg.value)
                )
                if m.matches(original_arg, arg_matcher(config))
                else updated_arg
            )
            for original_arg, updated_arg in zip(
                original_node.args, updated_node.args
            )
        ]
    )


def call_transformer(config: Call) -> cst.CSTTransformer:
    class CallTransformed(m.MatcherDecoratableTransformer):
        def visit_Call(self, node: cst.Call) -> bool:
            return False

        @m.leave(m.Call())
        def leave_call(
            self, original_node: cst.Call, updated_node: cst.Call
        ) -> cst.BaseExpression:
            if config.replace_raw:
                updated_node = (
                    cst.parse_module(dedent(config.replace_raw))
                    .body[0]
                    .body[0]
                    .value
                )

            if config.func and attr_has_changes(config.func):
                updated_node = updated_node.with_changes(
                    func=apply_attr(config.func, updated_node.func)
                )

            for arg_config in config.args or []:
                if arg_config.func and attr_has_changes(arg_config.func):
                    updated_node = apply_args(
                        original_node, updated_node, arg_config.func
                    )

            if config.to_await:
                updated_node = cst.Await(updated_node)

            return updated_node

    return CallTransformed()


def apply_calls(original_node, updated_node, calls):
    for call_config in calls:
        args = m.DoNotCare()

        if call_config.args:
            args = [m.ZeroOrMore()]

            for arg in call_config.args:
                if isinstance(arg, Call) and arg.func:
                    args.append(arg_matcher(arg.func))
                else:
                    raise Exception("unhandled")

            args.append(m.ZeroOrMore())

        matcher = m.Call(
            func=(
                attr_matcher(call_config.func)
                if call_config.func
                else m.DoNotCare()
            ),
            args=args,
        )

        if m.matches(original_node, matcher):
            updated_node = updated_node.visit(call_transformer(call_config))
            break

    return updated_node


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
        ) -> cst.Return | cst.RemovalSentinel:
            if config.replace_raw:
                updated_node = updated_node.with_changes(
                    whitespace_after_return=cst.SimpleWhitespace(" "),
                    value=cst.parse_module(dedent(config.replace_raw))
                    .body[0]
                    .body[0]
                    .value,
                )

            if config.remove:
                return cst.RemoveFromParent()

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


def add_raw_bottom_to_function(updated_node, add_raw_top):
    blocks = []
    for code in add_raw_top:
        blocks.extend(
            [
                cst.EmptyLine(),
                cst.parse_module(dedent(code)).body[0],
                cst.EmptyLine(),
            ]
        )

    return updated_node.with_changes(
        body=updated_node.body.with_changes(
            body=[*updated_node.body.body, *blocks]
        )
    )


def returns_expression(node: cst.Return, source: str) -> bool:
    """Whether ``node`` returns the expression written as ``source``."""
    if node.value is None:
        return False

    expected = cst.parse_module(dedent(source)).body[0].body[0].value

    return node.value.deep_equals(expected)


def apply_return_blocks(original_node, updated_node, return_blocks):
    for return_config in return_blocks:
        if not m.matches(original_node, m.Return()):
            continue

        if return_config.match_raw and not returns_expression(
            original_node, return_config.match_raw
        ):
            continue

        updated_node = updated_node.visit(return_transformer(return_config))
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
    attrs = [attr for attr in config.attrs or [] if not attr_needs_call(attr)]
    name_attrs = [attr for attr in attrs if attr.name]
    node_attrs = [attr for attr in attrs if not attr.name]
    call_attrs = [attr for attr in config.attrs or [] if attr_needs_call(attr)]

    class FunctionTransformed(m.MatcherDecoratableTransformer):
        if config.for_statements:
            @m.leave(m.For())
            def for_statement(
                self, original_node: cst.For, updated_node: cst.For
            ) -> cst.For:
                return apply_for_statements(
                    original_node, updated_node, config.for_statements
                )

        if config.comp_for_blocks:

            @m.leave(m.CompFor())
            def for_statement(
                self, original_node: cst.CompFor, updated_node: cst.CompFor
            ) -> cst.CompFor:
                return apply_comp_for_statements(
                    original_node, updated_node, config.comp_for_blocks
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

            @m.leave(m.FunctionDef(name=m.Name(name)))
            def to_async(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

        if config.rename:

            @m.leave(m.FunctionDef(name=m.Name(name)))
            def rename(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(name=cst.Name(config.rename))

        if config.functions:

            @m.leave(
                m.FunctionDef(
                    name=m.OneOf(
                        *[m.Name(name) for name in config.functions.keys()]
                    ),
                )
            )
            def nested_functions(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef | cst.RemovalSentinel:
                nested_name = original_node.name.value

                if nested_name == name:
                    return updated_node

                return updated_node.visit(
                    function_transformer(
                        nested_name, config.functions[nested_name]
                    )
                )

        if name_attrs:

            @m.call_if_not_inside(m.Param())
            @m.leave(m.Name())
            def attr_names(
                self, original_node: cst.Name, updated_node: cst.Name
            ) -> cst.BaseExpression:
                return apply_attrs(original_node, updated_node, name_attrs)

        if node_attrs:

            @m.leave(m.Attribute())
            def attr_nodes(
                self,
                original_node: cst.Attribute,
                updated_node: cst.Attribute,
            ) -> cst.BaseExpression:
                return apply_attrs(original_node, updated_node, node_attrs)

        if config.boolean_operations:

            @m.leave(m.BooleanOperation())
            def boolean_operations(
                self,
                original_node: cst.BooleanOperation,
                updated_node: cst.BooleanOperation,
            ) -> cst.BooleanOperation:
                return apply_boolean_operations(
                    original_node, updated_node, config.boolean_operations
                )

        if config.calls or call_attrs:

            @m.leave(m.Call())
            def calls(
                self, original_node: cst.Call, updated_node: cst.Call
            ) -> cst.BaseExpression:
                if config.calls:
                    updated_node = apply_calls(
                        original_node, updated_node, config.calls
                    )

                return apply_attrs(
                    original_node.func, updated_node, call_attrs
                )

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
    attrs = [attr for attr in config.attrs or [] if not attr_needs_call(attr)]
    name_attrs = [attr for attr in attrs if attr.name]
    node_attrs = [attr for attr in attrs if not attr.name]
    call_attrs = [attr for attr in config.attrs or [] if attr_needs_call(attr)]

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

            @m.leave(m.FunctionDef(name=m.Name(name)))
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

            @m.leave(m.FunctionDef(name=m.Name(name)))
            def to_async(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return updated_node.with_changes(
                    asynchronous=cst.Asynchronous()
                )

        if config.calls or call_attrs:

            @m.leave(m.Call())
            def calls(
                self, original_node: cst.Call, updated_node: cst.Call
            ) -> cst.BaseExpression:
                if config.calls:
                    updated_node = apply_calls(
                        original_node, updated_node, config.calls
                    )

                return apply_attrs(
                    original_node.func, updated_node, call_attrs
                )

        if name_attrs:

            @m.call_if_not_inside(m.Param())
            @m.leave(m.Name())
            def attr_names(
                self, original_node: cst.Name, updated_node: cst.Name
            ) -> cst.BaseExpression:
                return apply_attrs(original_node, updated_node, name_attrs)

        if node_attrs:

            @m.leave(m.Attribute())
            def attr_nodes(
                self,
                original_node: cst.Attribute,
                updated_node: cst.Attribute,
            ) -> cst.BaseExpression:
                return apply_attrs(original_node, updated_node, node_attrs)

        if config.boolean_operations:

            @m.leave(m.BooleanOperation())
            def boolean_operations(
                self,
                original_node: cst.BooleanOperation,
                updated_node: cst.BooleanOperation,
            ) -> cst.BooleanOperation:
                return apply_boolean_operations(
                    original_node, updated_node, config.boolean_operations
                )

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

        if config.add_raw_bottom:

            @m.leave(m.FunctionDef())
            def add_raw_bottom(
                self,
                original_node: cst.FunctionDef,
                updated_node: cst.FunctionDef,
            ) -> cst.FunctionDef:
                return add_raw_bottom_to_function(
                    updated_node, config.add_raw_bottom
                )

        if config.for_statements:

            @m.leave(m.For())
            def for_statement(
                self, original_node: cst.For, updated_node: cst.For
            ) -> cst.For:
                return apply_for_statements(
                    original_node, updated_node, config.for_statements
                )

        if config.comp_for_blocks:

            @m.leave(m.CompFor())
            def for_statement(
                self, original_node: cst.CompFor, updated_node: cst.CompFor
            ) -> cst.CompFor:
                return apply_comp_for_statements(
                    original_node, updated_node, config.comp_for_blocks
                )

    return MethodTransformed()


def class_transformer(name: str, config: Class) -> cst.CSTTransformer:
    class ClassTransformed(m.MatcherDecoratableTransformer):

        if config.rename:

            @m.leave(m.ClassDef(name=m.Name(name)))
            def rename(
                self, original_node: cst.ClassDef, updated_node: cst.ClassDef
            ) -> cst.ClassDef:
                return updated_node.with_changes(name=cst.Name(config.rename))

        if config.clear_bases:

            @m.leave(m.ClassDef())
            def clear_bases(
                self, original_node: cst.ClassDef, updated_node: cst.ClassDef
            ) -> cst.ClassDef:
                return updated_node.with_changes(
                    bases=[],
                    keywords=[],
                    lpar=cst.MaybeSentinel.DEFAULT,
                    rpar=cst.MaybeSentinel.DEFAULT,
                )

        if config.add_raw_top:

            @m.leave(m.ClassDef())
            def add_raw_top(
                self, original_node: cst.ClassDef, updated_node: cst.ClassDef
            ) -> cst.ClassDef:
                blocks = [
                    cst.parse_module(dedent(code)).body[0]
                    for code in config.add_raw_top
                ]
                body = list(updated_node.body.body)
                # Keep a leading docstring first, if there is one.
                if m.matches(
                    updated_node,
                    m.ClassDef(
                        body=m.IndentedBlock(
                            body=[
                                m.SimpleStatementLine(
                                    body=[
                                        m.Expr(value=m.SimpleString()),
                                        m.ZeroOrMore(),
                                    ]
                                ),
                                m.ZeroOrMore(),
                            ]
                        )
                    ),
                ):
                    new_body = [body[0], *blocks, *body[1:]]
                else:
                    new_body = [*blocks, *body]
                return updated_node.with_changes(
                    body=updated_node.body.with_changes(body=new_body)
                )

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
                    matcher = m.Assign(
                        targets=[
                            m.ZeroOrMore(),
                            m.AssignTarget(attr_matcher(assign_config.target)),
                            m.ZeroOrMore(),
                        ]
                    )

                    if m.matches(original_node, matcher):
                        updated_node = updated_node.visit(
                            assignment_transformer(assign_config)
                        )
                        # Once an assign is removed there is nothing left to
                        # transform, so stop applying further configs to it.
                        if isinstance(
                            updated_node,
                            (cst.RemovalSentinel, cst.FlattenSentinel),
                        ):
                            break

                return updated_node

    return ClassTransformed()


def module_transformer(config: Module) -> cst.CSTTransformer:
    class ModuleTransformed(m.MatcherDecoratableTransformer):

        def leave_ClassDef(
            self, original_node: cst.ClassDef, updated_node: cst.ClassDef
        ) -> cst.ClassDef | cst.RemovalSentinel:
            if config.classes and original_node.name.value in config.classes:
                class_config = config.classes[original_node.name.value]

                if class_config.remove:
                    return cst.RemoveFromParent()

                updated_node = updated_node.visit(
                    class_transformer(
                        original_node.name.value,
                        class_config,
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
                            remaining = [
                                name.with_changes(
                                    comma=cst.MaybeSentinel.DEFAULT
                                )
                                for name in updated_node.names
                                if name.name.value != alias_config.name
                            ]
                            if not remaining:
                                return cst.RemoveFromParent()
                            updated_node = updated_node.with_changes(
                                names=remaining
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

        if config.assigns:

            @m.leave(m.Module())
            def global_assigns(
                self, original_node: cst.Module, updated_node: cst.Module
            ) -> cst.Module:
                body = []
                for item in updated_node.body:
                    remove_item = False
                    if isinstance(item, cst.SimpleStatementLine):
                        for assign_config in config.assigns:
                            matcher = m.Assign(
                                targets=[
                                    m.ZeroOrMore(),
                                    m.AssignTarget(
                                        attr_matcher(assign_config.target)
                                    ),
                                    m.ZeroOrMore(),
                                ]
                            )

                            if not any(
                                m.matches(stmt, matcher) for stmt in item.body
                            ):
                                continue

                            if assign_config.remove:
                                remove_item = True
                                break

                            item = item.visit(
                                assignment_transformer(assign_config)
                            )

                    if not remove_item:
                        body.append(item)

                return updated_node.with_changes(body=body)

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

                    if isinstance(
                        item, (cst.RemovalSentinel, cst.FlattenSentinel)
                    ):
                        continue

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
