from django.core.exceptions import FieldFetchBlocked
from django.db.models.fetch_modes import (
    FETCH_ONE,
    FETCH_PEERS,
    RAISE,
)
from test_app.models import TestModel

from django_async_backend.test import AsyncioTestCase


class TestFetchMode(AsyncioTestCase):
    async def asyncSetUp(self):
        self.relative = TestModel(name="Relative", value=0)
        await self.relative.async_save()
        self.obj1 = TestModel(name="Test1", value=1, relative=self.relative)
        await self.obj1.async_save()
        self.obj2 = TestModel(name="Test2", value=2, relative=self.relative)
        await self.obj2.async_save()

    async def test_fetch_mode_default_is_fetch_one(self):
        queryset = TestModel.async_objects.all()

        self.assertIs(queryset._fetch_mode, FETCH_ONE)

    async def test_fetch_mode_sets_mode_on_clone(self):
        queryset = TestModel.async_objects.all()
        clone = queryset.fetch_mode(FETCH_PEERS)

        self.assertIsNot(clone, queryset, "Should return a new QuerySet")
        self.assertIs(clone._fetch_mode, FETCH_PEERS)
        self.assertIs(
            queryset._fetch_mode,
            FETCH_ONE,
            "The original QuerySet should be left untouched",
        )

    async def test_fetch_mode_survives_chaining(self):
        queryset = TestModel.async_objects.fetch_mode(FETCH_PEERS).filter(
            value__gt=0
        )

        self.assertIs(queryset._fetch_mode, FETCH_PEERS)

    async def test_fetch_mode_applied_to_loaded_objects(self):
        queryset = TestModel.async_objects.fetch_mode(RAISE).order_by("name")

        results = [obj async for obj in queryset]

        for obj in results:
            self.assertIs(obj._state.fetch_mode, RAISE)

    async def test_fetch_mode_raise_blocks_related_fetch(self):
        # RAISE is the observable consequence of the fetch mode reaching
        # from_db(): touching an unfetched related field is refused instead of
        # emitting a query.
        queryset = TestModel.async_objects.fetch_mode(RAISE).filter(
            name="Test1"
        )
        (obj,) = [obj async for obj in queryset]

        with self.assertRaises(FieldFetchBlocked):
            obj.relative

    # --- track_peers --------------------------------------------------

    async def test_fetch_one_does_not_track_peers(self):
        # fetch_mode.track_peers is False, so peers is left at the empty
        # default ModelState provides.
        queryset = TestModel.async_objects.order_by("name")

        results = [obj async for obj in queryset]

        for obj in results:
            self.assertEqual(obj._state.peers, ())

    async def test_fetch_peers_tracks_peers(self):
        # fetch_mode.track_peers is True, so every object gets a weak
        # reference to each of its peers from the same result set.
        queryset = TestModel.async_objects.fetch_mode(FETCH_PEERS).order_by(
            "name"
        )

        results = [obj async for obj in queryset]

        self.assertEqual(len(results), 3)
        for obj in results:
            self.assertEqual(
                [ref() for ref in obj._state.peers],
                results,
                "Each object should see the whole result set as its peers",
            )

    async def test_fetch_peers_shares_one_list_between_peers(self):
        # The peers list is built once per iteration and shared by reference,
        # so it holds every row even though it is appended to row by row.
        queryset = TestModel.async_objects.fetch_mode(FETCH_PEERS).order_by(
            "name"
        )

        first, second, third = [obj async for obj in queryset]

        self.assertIs(first._state.peers, second._state.peers)
        self.assertIs(second._state.peers, third._state.peers)

    async def test_fetch_peers_are_weak_references(self):
        # Peers are tracked weakly, so a collected object does not keep the
        # rest of the result set alive.
        queryset = TestModel.async_objects.fetch_mode(FETCH_PEERS).order_by(
            "name"
        )

        results = [obj async for obj in queryset]
        survivor = results[0]
        peers = survivor._state.peers
        del results

        self.assertEqual(len(peers), 3, "The weakrefs themselves remain")
        self.assertIs(peers[0](), survivor)

    async def test_fetch_peers_on_empty_result_set(self):
        queryset = TestModel.async_objects.fetch_mode(FETCH_PEERS).filter(
            name="Missing"
        )

        results = [obj async for obj in queryset]

        self.assertEqual(results, [])

    # Note: the fetching half of FETCH_PEERS (FetchPeers.fetch() calling
    # fetch_many() over the tracked peers) is not covered here. It is only
    # reached by touching a deferred related field, which goes through the
    # synchronous related descriptors and so cannot run in an async context.
    # These tests cover what the async iterables own: building the peers list.
