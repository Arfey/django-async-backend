from django.core.exceptions import FieldError
from django.db import DEFAULT_DB_ALIAS
from test_app.models import (
    ChildModel,
    GrandChildModel,
    RelatedSaveModel,
    SaveModel,
    TestModel,
    TotalOrderingModel,
    TotalOrderingRefModel,
)

from django_async_backend.db import async_connections
from django_async_backend.test import (
    AsyncCaptureQueriesContext,
    AsyncioTestCase,
)


class TestSelectRelatedForwardFk(AsyncioTestCase):
    """A forward FK is joined in and populated from the same row."""

    async def asyncSetUp(self):
        self.parent = TestModel(name="Parent", value=1)
        await self.parent.async_save()
        self.child = TestModel(name="Child", value=2, relative=self.parent)
        await self.child.async_save()

    async def test_relation_is_populated_without_extra_query(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in TestModel.async_objects.filter(
                    name="Child"
                ).select_related("relative")
            ][0]
            relative = obj.relative

        self.assertEqual(relative.pk, self.parent.pk)
        self.assertEqual(relative.name, "Parent")
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "select_related() should fetch the relation in a single query",
        )

    async def test_available_on_the_manager(self):
        obj = [
            o
            async for o in TestModel.async_objects.select_related(
                "relative"
            ).filter(name="Child")
        ][0]

        self.assertEqual(obj.relative.pk, self.parent.pk)

    async def test_null_relation_stays_none(self):
        """A nullable FK joins with LEFT OUTER and populates None."""
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in TestModel.async_objects.filter(
                    name="Parent"
                ).select_related("relative")
            ][0]
            relative = obj.relative

        self.assertIsNone(relative, "A null FK should populate as None")
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "Reading a null relation should not emit a query",
        )

    async def test_row_is_not_dropped_by_the_join(self):
        """The LEFT OUTER join must not filter out rows with a null FK."""
        names = {
            o.name
            async for o in TestModel.async_objects.select_related("relative")
        }

        self.assertEqual(
            names,
            {"Parent", "Child"},
            "Rows with a null FK should survive the join",
        )

    async def test_values_after_select_related_raises(self):
        with self.assertRaises(TypeError):
            TestModel.async_objects.values("name").select_related("relative")

    async def test_invalid_field_raises(self):
        queryset = TestModel.async_objects.select_related("does_not_exist")

        with self.assertRaises(FieldError):
            [o async for o in queryset]

    async def test_non_relational_field_raises(self):
        queryset = TestModel.async_objects.select_related("name")

        with self.assertRaises(FieldError):
            [o async for o in queryset]

    async def test_none_clears_select_related(self):
        queryset = TestModel.async_objects.filter(name="Child").select_related(
            "relative"
        )

        obj = [o async for o in queryset.select_related(None)][0]

        self.assertNotIn(
            "relative",
            obj._state.fields_cache,
            "select_related(None) should clear the selection",
        )


class TestSelectRelatedNested(AsyncioTestCase):
    """A chained lookup walks more than one relation in one query."""

    async def asyncSetUp(self):
        self.grand = TestModel(name="Grand", value=1)
        await self.grand.async_save()
        self.parent = TestModel(name="Parent", value=2, relative=self.grand)
        await self.parent.async_save()
        self.child = TestModel(name="Child", value=3, relative=self.parent)
        await self.child.async_save()

    async def test_nested_relation_is_populated(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in TestModel.async_objects.filter(
                    name="Child"
                ).select_related("relative__relative")
            ][0]
            grand = obj.relative.relative

        self.assertEqual(obj.relative.pk, self.parent.pk)
        self.assertEqual(grand.pk, self.grand.pk)
        self.assertEqual(grand.name, "Grand")
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "Both levels should come from one query",
        )

    async def test_nested_selection_implies_the_first_level(self):
        obj = [
            o
            async for o in TestModel.async_objects.filter(
                name="Child"
            ).select_related("relative__relative")
        ][0]

        self.assertIn("relative", obj._state.fields_cache)
        self.assertIn("relative", obj.relative._state.fields_cache)

    async def test_calls_accumulate(self):
        """Successive select_related() calls merge rather than replace."""
        obj = [
            o
            async for o in TestModel.async_objects.filter(name="Child")
            .select_related("relative")
            .select_related("relative__relative")
        ][0]

        self.assertIn("relative", obj.relative._state.fields_cache)


class TestSelectRelatedOneToOne(AsyncioTestCase):
    """Forward and reverse one-to-one relations are both traversable."""

    async def asyncSetUp(self):
        self.target = TotalOrderingModel(
            rank=1, headline="Headline", slug="slug", barcode="bar"
        )
        await self.target.async_save()
        self.ref = TotalOrderingRefModel(proof=self.target)
        await self.ref.async_save()

    async def test_forward_one_to_one(self):
        queryset = TotalOrderingRefModel.async_objects.select_related("proof")

        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [o async for o in queryset][0]
            proof = obj.proof

        self.assertEqual(proof.pk, self.target.pk)
        self.assertEqual(proof.headline, "Headline")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_reverse_one_to_one(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in TotalOrderingModel.async_objects.select_related(
                    "reference"
                )
            ][0]
            reference = obj.reference

        self.assertEqual(reference.pk, self.ref.pk)
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "A reverse o2o should be fetched in the same query",
        )


class TestSelectRelatedMultipleFields(AsyncioTestCase):
    """Several relations of one model can be selected at once."""

    async def asyncSetUp(self):
        self.fk = SaveModel(name="Fk", value=1)
        await self.fk.async_save()
        self.o2o = SaveModel(name="O2O", value=2)
        await self.o2o.async_save()
        self.obj = RelatedSaveModel(name="Owner", fk=self.fk, o2o=self.o2o)
        await self.obj.async_save()

    async def test_both_relations_are_populated(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in RelatedSaveModel.async_objects.select_related(
                    "fk", "o2o"
                )
            ][0]
            fk, o2o = obj.fk, obj.o2o

        self.assertEqual(fk.name, "Fk")
        self.assertEqual(o2o.name, "O2O")
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "Both relations should come from one query",
        )

    async def test_unselected_relation_is_not_populated(self):
        obj = [
            o
            async for o in RelatedSaveModel.async_objects.select_related("fk")
        ][0]

        self.assertIn("fk", obj._state.fields_cache)
        self.assertNotIn(
            "o2o",
            obj._state.fields_cache,
            "Only the requested relation should be populated",
        )


class TestSelectRelatedInheritance(AsyncioTestCase):
    """Multi-table inheritance parents are reachable through the join."""

    async def asyncSetUp(self):
        self.grand = GrandChildModel(
            parent_value=1, child_value=2, grand_child_value=3
        )
        await self.grand.async_save()

    async def test_parent_link_is_populated(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in GrandChildModel.async_objects.select_related(
                    "childmodel_ptr"
                )
            ][0]
            parent = obj.childmodel_ptr

        self.assertEqual(parent.pk, self.grand.pk)
        self.assertEqual(parent.child_value, 2)
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_inherited_fields_are_loaded(self):
        obj = [o async for o in GrandChildModel.async_objects.all()][0]

        self.assertEqual(obj.parent_value, 1)
        self.assertEqual(obj.child_value, 2)
        self.assertEqual(obj.grand_child_value, 3)

    async def test_reverse_child_from_parent(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = [
                o
                async for o in ChildModel.async_objects.select_related(
                    "grandchildmodel"
                )
            ][0]
            grand = obj.grandchildmodel

        self.assertEqual(grand.grand_child_value, 3)
        self.assertEqual(len(ctx.captured_queries), 1)


class TestSelectRelatedWithOnly(AsyncioTestCase):
    """_only() restricts the columns fetched for the related model too."""

    async def asyncSetUp(self):
        self.parent = TestModel(name="Parent", value=1)
        await self.parent.async_save()
        self.child = TestModel(name="Child", value=2, relative=self.parent)
        await self.child.async_save()

    async def test_related_field_is_loaded(self):
        obj = [
            o
            async for o in TestModel.async_objects.filter(name="Child")
            .select_related("relative")
            ._only("name", "relative__name")
        ][0]

        self.assertEqual(obj.relative.name, "Parent")

    async def test_unrequested_related_column_is_deferred(self):
        obj = [
            o
            async for o in TestModel.async_objects.filter(name="Child")
            .select_related("relative")
            ._only("name", "relative__name")
        ][0]

        self.assertIn(
            "value",
            obj.relative.get_deferred_fields(),
            "An unrequested related column should be deferred",
        )

    async def test_unrequested_related_column_is_not_selected(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            [
                o
                async for o in TestModel.async_objects.filter(name="Child")
                .select_related("relative")
                ._only("name", "relative__name")
            ]

        sql = ctx.captured_queries[-1]["sql"]

        self.assertNotIn(
            '"value"',
            sql,
            "A deferred column should not be selected",
        )


class TestSelectRelatedOrderingAndFiltering(AsyncioTestCase):
    """The join composes with filters and ordering across the relation."""

    async def asyncSetUp(self):
        self.a = TestModel(name="A", value=1)
        await self.a.async_save()
        self.b = TestModel(name="B", value=2)
        await self.b.async_save()
        self.child_a = TestModel(name="ChildA", value=10, relative=self.a)
        await self.child_a.async_save()
        self.child_b = TestModel(name="ChildB", value=20, relative=self.b)
        await self.child_b.async_save()

    async def test_filter_across_the_relation(self):
        objs = [
            o
            async for o in TestModel.async_objects.filter(
                relative__name="A"
            ).select_related("relative")
        ]

        self.assertEqual(len(objs), 1)
        self.assertEqual(objs[0].name, "ChildA")
        self.assertEqual(objs[0].relative.name, "A")

    async def test_order_by_a_related_column(self):
        names = [
            o.name
            async for o in TestModel.async_objects.filter(
                relative__isnull=False
            )
            .select_related("relative")
            .order_by("-relative__name")
        ]

        self.assertEqual(names, ["ChildB", "ChildA"])

    async def test_relations_are_populated_per_row(self):
        """Each row gets its own related instance, not a shared one."""
        objs = [
            o
            async for o in TestModel.async_objects.filter(
                relative__isnull=False
            )
            .select_related("relative")
            .order_by("name")
        ]

        self.assertEqual(
            [o.relative.name for o in objs],
            ["A", "B"],
            "Each row should carry its own related object",
        )

    async def test_aget_populates_the_relation(self):
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            obj = await TestModel.async_objects.select_related(
                "relative"
            ).aget(name="ChildA")
            relative = obj.relative

        self.assertEqual(relative.name, "A")
        self.assertEqual(len(ctx.captured_queries), 1)

    async def test_afirst_populates_the_relation(self):
        obj = (
            await TestModel.async_objects.filter(name="ChildB")
            .select_related("relative")
            .afirst()
        )

        self.assertEqual(obj.relative.name, "B")

    async def test_no_extra_queries_for_a_full_scan(self):
        """The N+1 the feature exists to avoid must not reappear."""
        async with AsyncCaptureQueriesContext(
            async_connections[DEFAULT_DB_ALIAS]
        ) as ctx:
            objs = [
                o
                async for o in TestModel.async_objects.filter(
                    relative__isnull=False
                ).select_related("relative")
            ]
            names = [o.relative.name for o in objs]

        self.assertEqual(sorted(names), ["A", "B"])
        self.assertEqual(
            len(ctx.captured_queries),
            1,
            "Walking every relation should stay at one query",
        )
