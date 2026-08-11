from django.db import models
from django.db.models import (
    Count,
    F,
)
from django.db.models.functions import Lower
from django.test.utils import isolate_apps
from test_app.models import (
    DefaultOrderingModel,
    TotalOrderingChildModel,
    TotalOrderingCompositePkModel,
    TotalOrderingModel,
    TotalOrderingRefModel,
)

from django_async_backend.db.models.manager import AsyncManager
from django_async_backend.test import AsyncioTestCase


class TestTotallyOrdered(AsyncioTestCase):
    """totally_ordered introspects the ordering only, so none of these
    querysets are evaluated.
    """

    async def test_unordered_queryset(self):
        self.assertIs(
            TotalOrderingModel.async_objects.all().totally_ordered,
            False,
            "A model without Meta.ordering is not ordered at all",
        )

    async def test_non_unique_field(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                "headline"
            ).totally_ordered,
            False,
            "A non-unique field cannot break ties",
        )

    async def test_pk_ordering(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("pk").totally_ordered,
            True,
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("id").totally_ordered,
            True,
            "The pk attname should be recognised as well as 'pk'",
        )

    async def test_reverse_ordering(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("-pk").totally_ordered,
            True,
            "Descending order is still a total ordering",
        )

    async def test_nullable_unique_field(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("rank").totally_ordered,
            False,
            "A unique but nullable field cannot ensure a total ordering",
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                "rank", "pk"
            ).totally_ordered,
            True,
            "Appending the pk makes the ordering total",
        )

    async def test_default_ordering_on_unique_field(self):
        self.assertIs(
            DefaultOrderingModel.async_objects.all().totally_ordered,
            True,
            "Meta.ordering on a unique non-null field is a total ordering",
        )

    async def test_cleared_default_ordering(self):
        self.assertIs(
            DefaultOrderingModel.async_objects.order_by().totally_ordered,
            False,
            "order_by() clears the default ordering",
        )

    async def test_group_by_ignores_default_ordering(self):
        queryset = (
            DefaultOrderingModel.async_objects.values("value")
            .annotate(total=Count("id"))
            .order_by()
        )

        self.assertIs(
            queryset.totally_ordered,
            False,
            "A GROUP BY query ignores the model's default ordering",
        )

    async def test_unique_together(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                "headline", "slug"
            ).totally_ordered,
            True,
            "A full unique_together pair is a total ordering",
        )

    async def test_composite_constraint_with_nullable_member(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                "pub_date", "rank"
            ).totally_ordered,
            False,
            "A unique constraint containing a nullable column is not total",
        )

    async def test_conditional_constraint(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("rank").totally_ordered,
            False,
            "A conditional unique constraint only applies to matching rows",
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                "barcode"
            ).totally_ordered,
            True,
            "An empty condition constrains every row, so it is total",
        )

    async def test_f_expressions(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(F("pk")).totally_ordered,
            True,
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                F("headline")
            ).totally_ordered,
            False,
        )

    async def test_order_by_expression(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                F("pk").desc()
            ).totally_ordered,
            True,
            "An OrderBy wrapping an F should be unwrapped",
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                F("headline").desc()
            ).totally_ordered,
            False,
        )

    async def test_one_to_one_relation(self):
        self.assertIs(
            TotalOrderingRefModel.async_objects.order_by(
                "proof"
            ).totally_ordered,
            False,
            "Ordering by a relation name defers to the related model",
        )
        self.assertIs(
            TotalOrderingRefModel.async_objects.order_by(
                "proof_id"
            ).totally_ordered,
            True,
            "The OneToOne column itself is unique and non-null",
        )

    async def test_relation_traversal(self):
        self.assertIs(
            TotalOrderingChildModel.async_objects.order_by(
                "parent__pk"
            ).totally_ordered,
            False,
            "A traversed lookup is not introspected",
        )

    async def test_composite_primary_key(self):
        self.assertIs(
            TotalOrderingCompositePkModel.async_objects.order_by(
                "tenant_id"
            ).totally_ordered,
            False,
            "One member of a composite pk is not enough",
        )
        self.assertIs(
            TotalOrderingCompositePkModel.async_objects.order_by(
                "tenant_id", "code"
            ).totally_ordered,
            True,
            "Every member of a composite pk together is a total ordering",
        )
        self.assertIs(
            TotalOrderingCompositePkModel.async_objects.order_by(
                "pk"
            ).totally_ordered,
            True,
        )

    async def test_non_field_expression_is_skipped(self):
        """An OrderBy wrapping something other than an F has no field name to
        introspect, so it neither proves nor breaks a total ordering.
        """
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                Lower("headline").asc()
            ).totally_ordered,
            False,
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by(
                Lower("headline").asc(), "pk"
            ).totally_ordered,
            True,
            "A later pk still provides the total ordering",
        )

    @isolate_apps("test_app")
    async def test_unresolvable_constraint_field_is_skipped(self):
        """A constraint naming something get_field() cannot resolve is skipped
        rather than raising, so a later valid constraint still applies.

        The model is defined under isolate_apps() because such a constraint is
        rejected by the system checks (models.E012), so it cannot live in
        test_app.models.
        """

        class GhostConstraintModel(models.Model):
            headline = models.CharField(max_length=100)
            slug = models.CharField(max_length=100)

            async_objects = AsyncManager()

            class Meta:
                app_label = "test_app"
                unique_together = (
                    ("headline", "ghost"),
                    ("headline", "slug"),
                )

        self.assertIs(
            GhostConstraintModel.async_objects.order_by(
                "headline", "slug"
            ).totally_ordered,
            True,
            "The unresolvable constraint should be skipped, not raise",
        )

    async def test_random_ordering(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("?").totally_ordered,
            False,
            "Random ordering is not deterministic",
        )

    async def test_none(self):
        self.assertIs(
            TotalOrderingModel.async_objects.order_by().none().totally_ordered,
            False,
        )
        self.assertIs(
            TotalOrderingModel.async_objects.order_by("pk")
            .none()
            .totally_ordered,
            True,
            "An empty queryset keeps the ordering it was given",
        )
