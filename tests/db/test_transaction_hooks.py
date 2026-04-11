import logging

import pytest
from django.db import DEFAULT_DB_ALIAS, transaction
from shared.models import Reporter

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic
from tests.fixtures.reporter_table import fetch_all_reporters


class ForcedError(Exception):
    pass


async def _create_int_instance(id):
    await Reporter.async_object.acreate(name=str(id))
    return int(id)


async def _fetch_int_ids():
    rows = await fetch_all_reporters()
    return [int(r) for r in rows]


@pytest.fixture
async def hook_state(reporter_table_transaction):
    """Track on_commit notifications. Uses reporter_table_transaction because
    on_commit semantics require a real BEGIN/COMMIT (not a nested savepoint)."""
    notified = []

    def notify(id_):
        if id_ == "error":
            raise ForcedError
        notified.append(id_)

    async def do(num):
        """Create a reporter and register an on_commit notification."""
        await _create_int_instance(num)
        await async_connections[DEFAULT_DB_ALIAS].on_commit(lambda: notify(num))

    async def assert_done(nums):
        assert notified == nums
        assert sorted(await _fetch_int_ids()) == sorted(nums)

    return type(
        "HookState",
        (),
        {
            "notified": notified,
            "notify": staticmethod(notify),
            "do": staticmethod(do),
            "assert_done": staticmethod(assert_done),
        },
    )()


async def test_executes_immediately_if_no_transaction(hook_state):
    await hook_state.do(1)
    await hook_state.assert_done([1])


async def test_robust_if_no_transaction(hook_state, caplog):
    connection = async_connections[DEFAULT_DB_ALIAS]

    def robust_callback():
        raise ForcedError("robust callback")

    with caplog.at_level(logging.ERROR, logger="django_async_backend.db.backends"):
        await connection.on_commit(robust_callback, robust=True)
        await hook_state.do(1)

    await hook_state.assert_done([1])
    log_record = caplog.records[0]
    assert "robust_callback in on_commit() (robust callback)" in log_record.getMessage()
    assert log_record.exc_info is not None
    raised = log_record.exc_info[1]
    assert isinstance(raised, ForcedError)
    assert str(raised) == "robust callback"


async def test_robust_transaction(hook_state, caplog):
    connection = async_connections[DEFAULT_DB_ALIAS]

    def robust_callback():
        raise ForcedError("robust callback")

    with caplog.at_level(logging.ERROR, logger="django_async_backend.db.backends"):
        async with async_atomic():
            await connection.on_commit(robust_callback, robust=True)
            await hook_state.do(1)

    await hook_state.assert_done([1])
    log_record = caplog.records[0]
    assert "robust_callback in on_commit() during transaction (robust callback)" in log_record.getMessage()
    assert log_record.exc_info is not None
    raised = log_record.exc_info[1]
    assert isinstance(raised, ForcedError)
    assert str(raised) == "robust callback"


async def test_delays_execution_until_after_transaction_commit(hook_state):
    async with async_atomic():
        await hook_state.do(1)
        assert hook_state.notified == []
    await hook_state.assert_done([1])


async def test_does_not_execute_if_transaction_rolled_back(hook_state):
    with pytest.raises(ForcedError):
        async with async_atomic():
            await hook_state.do(1)
            raise ForcedError

    await hook_state.assert_done([])


async def test_executes_only_after_final_transaction_committed(hook_state):
    async with async_atomic():
        async with async_atomic():
            await hook_state.do(1)
            assert hook_state.notified == []
        assert hook_state.notified == []
    await hook_state.assert_done([1])


async def test_discards_hooks_from_rolled_back_savepoint(hook_state):
    async with async_atomic():
        async with async_atomic():
            await hook_state.do(1)
        with pytest.raises(ForcedError):
            async with async_atomic():
                await hook_state.do(2)
                raise ForcedError
        async with async_atomic():
            await hook_state.do(3)

    await hook_state.assert_done([1, 3])


async def test_no_hooks_run_from_failed_transaction(hook_state):
    """If outer transaction fails, no hooks from within it run."""
    with pytest.raises(ForcedError):
        async with async_atomic():
            async with async_atomic():
                await hook_state.do(1)
            raise ForcedError

    await hook_state.assert_done([])


async def test_inner_savepoint_rolled_back_with_outer(hook_state):
    async with async_atomic():
        with pytest.raises(ForcedError):
            async with async_atomic():
                async with async_atomic():
                    await hook_state.do(1)
                raise ForcedError
        await hook_state.do(2)

    await hook_state.assert_done([2])


async def test_no_savepoints_atomic_merged_with_outer(hook_state):
    async with async_atomic(), async_atomic():
        await hook_state.do(1)
        with pytest.raises(ForcedError):
            async with async_atomic(savepoint=False):
                raise ForcedError

    await hook_state.assert_done([])


async def test_inner_savepoint_does_not_affect_outer(hook_state):
    async with async_atomic(), async_atomic():
        await hook_state.do(1)
        with pytest.raises(ForcedError):
            async with async_atomic():
                raise ForcedError

    await hook_state.assert_done([1])


async def test_runs_hooks_in_order_registered(hook_state):
    async with async_atomic():
        await hook_state.do(1)
        async with async_atomic():
            await hook_state.do(2)
        await hook_state.do(3)

    await hook_state.assert_done([1, 2, 3])


async def test_hooks_cleared_after_successful_commit(hook_state):
    async with async_atomic():
        await hook_state.do(1)
    async with async_atomic():
        await hook_state.do(2)

    await hook_state.assert_done([1, 2])


async def test_hooks_cleared_after_rollback(hook_state):
    with pytest.raises(ForcedError):
        async with async_atomic():
            await hook_state.do(1)
            raise ForcedError

    async with async_atomic():
        await hook_state.do(2)

    await hook_state.assert_done([2])


async def test_hooks_cleared_on_reconnect(hook_state):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async with async_atomic():
        await hook_state.do(1)
        await connection.close()

    await connection.connect()

    async with async_atomic():
        await hook_state.do(2)

    await hook_state.assert_done([2])


async def test_error_in_hook_does_not_prevent_clearing_hooks(hook_state):
    connection = async_connections[DEFAULT_DB_ALIAS]

    with pytest.raises(ForcedError):
        async with async_atomic():
            await connection.on_commit(lambda: hook_state.notify("error"))

    async with async_atomic():
        await hook_state.do(1)

    await hook_state.assert_done([1])


async def test_db_query_in_hook(hook_state):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async def commit():
        return [hook_state.notify(t) for t in await _fetch_int_ids()]

    async with async_atomic():
        await _create_int_instance(1)
        await connection.on_commit(commit)

    await hook_state.assert_done([1])


async def test_transaction_in_hook(hook_state):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async def on_commit():
        async with async_atomic():
            t = await _create_int_instance(1)
            hook_state.notify(t)

    async with async_atomic():
        await connection.on_commit(on_commit)

    await hook_state.assert_done([1])


async def test_hook_in_hook(hook_state):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async def on_commit(i, add_hook):
        async with async_atomic():
            if add_hook:
                await connection.on_commit(lambda: on_commit(i + 10, False))
            t = await _create_int_instance(i)
            hook_state.notify(t)

    async with async_atomic():
        await connection.on_commit(lambda: on_commit(1, True))
        await connection.on_commit(lambda: on_commit(2, True))

    await hook_state.assert_done([1, 11, 2, 12])


async def test_raises_exception_non_autocommit_mode(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]

    def should_never_be_called():
        raise AssertionError("this function should never be called")

    try:
        await connection.set_autocommit(False)
        with pytest.raises(
            transaction.TransactionManagementError,
            match="cannot be used in manual transaction management",
        ):
            await connection.on_commit(should_never_be_called)
    finally:
        await connection.set_autocommit(True)


async def test_raises_exception_non_callable(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    with pytest.raises(TypeError, match="callback must be a callable"):
        await connection.on_commit(None)
