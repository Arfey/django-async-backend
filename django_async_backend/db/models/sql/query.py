from django.db.models import Count
from django.db.models.expressions import Ref
from django.db.models.sql.constants import SINGLE
from django.db.models.sql.query import (
    EXPLAIN_OPTIONS_PATTERN,
    ExplainInfo,
    Query,
)

from django_async_backend.db import async_connections


class AsyncQuery(Query):
    compiler = "AsyncSQLCompiler"

    def get_compiler(self, using=None, connection=None, elide_empty=True):
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = async_connections[using]

        return connection.ops.compiler(self.compiler)(
            self, connection, using, elide_empty
        )

    async def get_aggregation(self, using, aggregate_exprs):  # noqa: C901
        """
        Return the dictionary with the values of the existing aggregations.
        """
        if not aggregate_exprs:
            return {}
        # Store annotation mask prior to temporarily adding aggregations for
        # resolving purpose to facilitate their subsequent removal.
        refs_subquery = False
        refs_window = False
        replacements = {}
        annotation_select_mask = self.annotation_select_mask
        for alias, aggregate_expr in aggregate_exprs.items():
            self.check_alias(alias)
            aggregate = aggregate_expr.resolve_expression(
                self, allow_joins=True, reuse=None, summarize=True
            )
            if not aggregate.contains_aggregate:
                raise TypeError("%s is not an aggregate expression" % alias)
            # Temporarily add aggregate to annotations to allow remaining
            # members of `aggregates` to resolve against each others.
            self.append_annotation_mask([alias])
            aggregate_refs = aggregate.get_refs()
            refs_subquery |= any(
                getattr(self.annotations[ref], "contains_subquery", False)
                for ref in aggregate_refs
            )
            refs_window |= any(
                getattr(self.annotations[ref], "contains_over_clause", True)
                for ref in aggregate_refs
            )
            aggregate = aggregate.replace_expressions(replacements)
            self.annotations[alias] = aggregate
            replacements[Ref(alias, aggregate)] = aggregate
        # Stash resolved aggregates now that they have been allowed to resolve
        # against each other.
        aggregates = {
            alias: self.annotations.pop(alias) for alias in aggregate_exprs
        }
        self.set_annotation_mask(annotation_select_mask)
        # Existing usage of aggregation can be determined by the presence of
        # selected aggregates but also by filters against aliased aggregates.
        _, having, qualify = self.where.split_having_qualify()
        has_existing_aggregation = (
            any(
                getattr(annotation, "contains_aggregate", True)
                for annotation in self.annotations.values()
            )
            or having
        )
        set_returning_annotations = {
            alias
            for alias, annotation in self.annotation_select.items()
            if getattr(annotation, "set_returning", False)
        }
        # Decide if we need to use a subquery.
        #
        # Existing aggregations would cause incorrect results as
        # get_aggregation() must produce just one result and thus must not use
        # GROUP BY.
        #
        # If the query has limit or distinct, or uses set operations, then
        # those operations must be done in a subquery so that the query
        # aggregates on the limit and/or distinct results instead of applying
        # the distinct and limit after the aggregation.
        if (
            isinstance(self.group_by, tuple)
            or self.is_sliced
            or has_existing_aggregation
            or refs_subquery
            or refs_window
            or qualify
            or self.distinct
            or self.combinator
            or set_returning_annotations
        ):
            from django.db.models.sql.subqueries import AggregateQuery

            inner_query = self.clone()
            inner_query.subquery = True
            outer_query = AggregateQuery(self.model, inner_query)
            inner_query.select_for_update = False
            inner_query.select_related = False
            inner_query.set_annotation_mask(self.annotation_select)
            # Queries with distinct_fields need ordering and when a limit is
            # applied we must take the slice from the ordered query. Otherwise
            # no need for ordering.
            if inner_query.orderby_issubset_groupby:
                inner_query.clear_ordering(force=False)
            if not inner_query.distinct:
                # If the inner query uses default select and it has some
                # aggregate annotations, then we must make sure the inner
                # query is grouped by the main model's primary key. However,
                # clearing the select clause can alter results if distinct is
                # used.
                if inner_query.default_cols and has_existing_aggregation:
                    inner_query.group_by = (
                        self.model._meta.pk.get_col(
                            inner_query.get_initial_alias()
                        ),
                    )
                inner_query.default_cols = False
                if not qualify and not self.combinator:
                    # Mask existing annotations that are not referenced by
                    # aggregates to be pushed to the outer query unless
                    # filtering against window functions or if the query is
                    # combined as both would require complex realiasing logic.
                    annotation_mask = set()
                    if isinstance(self.group_by, tuple):
                        for expr in self.group_by:
                            annotation_mask |= expr.get_refs()
                    for aggregate in aggregates.values():
                        annotation_mask |= aggregate.get_refs()
                    # Avoid eliding expressions that might have an incidence on
                    # the implicit grouping logic.
                    for (
                        annotation_alias,
                        annotation,
                    ) in self.annotation_select.items():
                        if annotation.get_group_by_cols():
                            annotation_mask.add(annotation_alias)
                    inner_query.set_annotation_mask(annotation_mask)
                    # Annotations that possibly return multiple rows cannot
                    # be masked as they might have an incidence on the query.
                    annotation_mask |= set_returning_annotations

            # Add aggregates to the outer AggregateQuery. This requires making
            # sure all columns referenced by the aggregates are selected in the
            # inner query. It is achieved by retrieving all column references
            # by the aggregates, explicitly selecting them in the inner query,
            # and making sure the aggregates are repointed to them.
            col_refs = {}
            for alias, aggregate in aggregates.items():
                replacements = {}
                for col in self._gen_cols([aggregate], resolve_refs=False):
                    if not (col_ref := col_refs.get(col)):
                        index = len(col_refs) + 1
                        col_alias = f"__col{index}"
                        col_ref = Ref(col_alias, col)
                        col_refs[col] = col_ref
                        inner_query.add_annotation(col, col_alias)
                    replacements[col] = col_ref
                outer_query.annotations[alias] = aggregate.replace_expressions(
                    replacements
                )
            if (
                inner_query.select == ()
                and not inner_query.default_cols
                and not inner_query.annotation_select_mask
            ):
                # In case of Model.objects[0:3].count(), there would be no
                # field selected in the inner query, yet we must use a
                # subquery. So, make sure at least one field is selected.
                inner_query.select = (
                    self.model._meta.pk.get_col(
                        inner_query.get_initial_alias()
                    ),
                )
        else:
            outer_query = self
            self.select = ()
            self.selected = None
            self.default_cols = False
            self.extra = {}
            if self.annotations:
                # Inline reference to existing annotations and mask them as
                # they are unnecessary given only the summarized aggregations
                # are requested.
                replacements = {
                    Ref(alias, annotation): annotation
                    for alias, annotation in self.annotations.items()
                }
                self.annotations = {
                    alias: aggregate.replace_expressions(replacements)
                    for alias, aggregate in aggregates.items()
                }
            else:
                self.annotations = aggregates
            self.set_annotation_mask(aggregates)

        empty_set_result = [
            expression.empty_result_set_value
            for expression in outer_query.annotation_select.values()
        ]
        elide_empty = not any(
            result is NotImplemented for result in empty_set_result
        )
        outer_query.clear_ordering(force=True)
        outer_query.clear_limits()
        outer_query.select_for_update = False
        outer_query.select_related = False
        compiler = outer_query.get_compiler(using, elide_empty=elide_empty)
        result = await compiler.execute_sql(SINGLE)
        if result is None:
            result = empty_set_result
        else:
            cols = outer_query.annotation_select.values()
            converters = compiler.get_converters(cols)
            rows = compiler.apply_converters((result,), converters)
            if compiler.has_composite_fields(cols):
                rows = compiler.composite_fields_to_tuples(rows, cols)
            result = next(rows)

        return dict(zip(outer_query.annotation_select, result))

    async def get_count(self, using):
        """
        Perform a COUNT() query using the current filter constraints.
        """
        obj = self.clone()
        return (await obj.get_aggregation(using, {"__count": Count("*")}))[
            "__count"
        ]

    async def has_results(self, using):
        q = self.exists()
        compiler = q.get_compiler(using=using)
        return await compiler.has_results()

    async def explain(self, using, format=None, **options):
        q = self.clone()
        for option_name in options:
            if (
                not EXPLAIN_OPTIONS_PATTERN.fullmatch(option_name)
                or "--" in option_name
            ):
                raise ValueError(f"Invalid option name: {option_name!r}.")
        q.explain_info = ExplainInfo(format, options)
        compiler = q.get_compiler(using=using)
        return "\n".join([i async for i in compiler.explain_query()])
