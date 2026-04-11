import asyncio
import sys

import pytest
from django.db import DEFAULT_DB_ALIAS, DatabaseError, Error, IntegrityError, transaction

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic, async_mark_for_rollback_on_error
from tests.fixtures.reporter_table import fetch_all_reporters, insert_reporter

# ── Durable atomic blocks ──────────────────────────────────────────────


async def test_durable_atomic_commit(reporter_table_transaction):
    async with async_atomic(durable=True):
        await insert_reporter(1)
    assert await fetch_all_reporters() == ["1"]


async def test_durable_rollback(reporter_table_transaction):
    with pytest.raises(Exception, match="force"):
        async with async_atomic(durable=True):
            await insert_reporter(1)
            raise Exception("force")
    assert await fetch_all_reporters() == []


async def test_durable_nested_outer(reporter_table_transaction):
    async with async_atomic(durable=True):
        await insert_reporter(1)
        async with async_atomic():
            await insert_reporter(2)
    assert await fetch_all_reporters() == ["1", "2"]


async def test_durable_nested_both(reporter_table_transaction):
    msg = "A durable atomic block cannot be nested within another atomic block."
    async with async_atomic(durable=True):
        with pytest.raises(RuntimeError, match=msg):
            async with async_atomic(durable=True):
                pass


async def test_durable_nested_inner(reporter_table_transaction):
    msg = "A durable atomic block cannot be nested within another atomic block."
    async with async_atomic():
        with pytest.raises(RuntimeError, match=msg):
            async with async_atomic(durable=True):
                pass


async def test_durable_sequence(reporter_table_transaction):
    async with async_atomic(durable=True):
        await insert_reporter(1)
    assert await fetch_all_reporters() == ["1"]

    async with async_atomic(durable=True):
        await insert_reporter(2)
    assert await fetch_all_reporters() == ["1", "2"]


# ── AsyncAtomic basic tests ───────────────────────────────────────────
#
# The same test bodies run in three modes (mirroring Django's
# AsyncAtomicTests / AsyncAtomicInsideTransactionTests /
# AsyncAtomicWithoutAutocommitTests):
#
#   1. plain: no outer transaction, autocommit on
#   2. inside_transaction: wrapped in an outer async_atomic
#   3. without_autocommit: autocommit off, rollback before/after
#
# Each mode is a fixture that yields inside the right state.


@pytest.fixture(params=["plain", "inside_transaction", "without_autocommit"])
async def _in_atomic_mode(reporter_table_transaction, request):
    """Parametrized fixture: runs each test in three atomic modes.

    - plain: no outer transaction, autocommit on
    - inside_transaction: wrapped in an outer async_atomic
    - without_autocommit: autocommit off, rollback after
    """
    mode = request.param
    if mode == "plain":
        yield
    elif mode == "inside_transaction":
        atomic = async_atomic()
        atomic._from_testcase = True
        await atomic.__aenter__()
        try:
            yield
        finally:
            await atomic.__aexit__(*sys.exc_info())
    else:  # without_autocommit
        connection = async_connections[DEFAULT_DB_ALIAS]
        await connection.set_autocommit(False)
        try:
            yield
        finally:
            await connection.rollback()
            await connection.set_autocommit(True)


async def test_decorator_syntax_commit(_in_atomic_mode):
    @async_atomic
    async def make_reporter():
        return await insert_reporter(1)

    reporter = await make_reporter()
    assert await fetch_all_reporters() == [reporter]


async def test_decorator_syntax_rollback(_in_atomic_mode):
    @async_atomic
    async def make_reporter():
        await insert_reporter(1)
        raise Exception("Oops")

    with pytest.raises(Exception, match="Oops"):
        await make_reporter()
    assert await fetch_all_reporters() == []


async def test_alternate_decorator_syntax_commit(_in_atomic_mode):
    @async_atomic()
    async def make_reporter():
        return await insert_reporter(1)

    reporter = await make_reporter()
    assert await fetch_all_reporters() == [reporter]


async def test_alternate_decorator_syntax_rollback(_in_atomic_mode):
    @async_atomic()
    async def make_reporter():
        await insert_reporter(1)
        raise Exception("Oops")

    with pytest.raises(Exception, match="Oops"):
        await make_reporter()
    assert await fetch_all_reporters() == []


async def test_commit(_in_atomic_mode):
    async with async_atomic():
        reporter = await insert_reporter(1)
    assert await fetch_all_reporters() == [reporter]


async def test_rollback(_in_atomic_mode):
    with pytest.raises(Exception, match="Oops"):
        async with async_atomic():
            await insert_reporter(1)
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_nested_commit_commit(_in_atomic_mode):
    async with async_atomic():
        reporter1 = await insert_reporter(1)
        async with async_atomic():
            reporter2 = await insert_reporter(2)
    assert await fetch_all_reporters() == [reporter1, reporter2]


async def test_nested_commit_rollback(_in_atomic_mode):
    async with async_atomic():
        reporter = await insert_reporter(1)
        with pytest.raises(Exception, match="Oops"):
            async with async_atomic():
                await insert_reporter(2)
                raise Exception("Oops")
    assert await fetch_all_reporters() == [reporter]


async def test_nested_rollback_commit(_in_atomic_mode):
    with pytest.raises(Exception, match="Oops"):
        async with async_atomic():
            await insert_reporter(1)
            async with async_atomic():
                await insert_reporter(2)
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_nested_rollback_rollback(_in_atomic_mode):
    with pytest.raises(Exception, match="Oops"):
        async with async_atomic():
            await insert_reporter(1)
            with pytest.raises(Exception, match="Oops"):
                async with async_atomic():
                    await insert_reporter(2)
                raise Exception("Oops")
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_merged_commit_commit(_in_atomic_mode):
    async with async_atomic():
        reporter1 = await insert_reporter(1)
        async with async_atomic(savepoint=False):
            reporter2 = await insert_reporter(2)
    assert await fetch_all_reporters() == [reporter1, reporter2]


async def test_merged_commit_rollback(_in_atomic_mode):
    async with async_atomic():
        await insert_reporter(1)
        with pytest.raises(Exception, match="Oops"):
            async with async_atomic(savepoint=False):
                await insert_reporter(2)
                raise Exception("Oops")
    # Writes in the outer block are rolled back too.
    assert await fetch_all_reporters() == []


async def test_merged_rollback_commit(_in_atomic_mode):
    with pytest.raises(Exception, match="Oops"):
        async with async_atomic():
            await insert_reporter(1)
            async with async_atomic(savepoint=False):
                await insert_reporter(2)
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_merged_rollback_rollback(_in_atomic_mode):
    with pytest.raises(Exception, match="Oops"):
        async with async_atomic():
            await insert_reporter(1)
            with pytest.raises(Exception, match="Oops"):
                async with async_atomic(savepoint=False):
                    await insert_reporter(2)
                raise Exception("Oops")
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_reuse_commit_commit(_in_atomic_mode):
    atomic = async_atomic()
    async with atomic:
        reporter1 = await insert_reporter(1)
        async with atomic:
            reporter2 = await insert_reporter(2)
    assert await fetch_all_reporters() == [reporter1, reporter2]


async def test_reuse_commit_rollback(_in_atomic_mode):
    atomic = async_atomic()
    async with atomic:
        reporter = await insert_reporter(1)
        with pytest.raises(Exception, match="Oops"):
            async with atomic:
                await insert_reporter(2)
                raise Exception("Oops")
    assert await fetch_all_reporters() == [reporter]


async def test_reuse_rollback_commit(_in_atomic_mode):
    atomic = async_atomic()
    with pytest.raises(Exception, match="Oops"):
        async with atomic:
            await insert_reporter(1)
            async with atomic:
                await insert_reporter(2)
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_reuse_rollback_rollback(_in_atomic_mode):
    atomic = async_atomic()
    with pytest.raises(Exception, match="Oops"):
        async with atomic:
            await insert_reporter(1)
            with pytest.raises(Exception, match="Oops"):
                async with atomic:
                    await insert_reporter(2)
                raise Exception("Oops")
            raise Exception("Oops")
    assert await fetch_all_reporters() == []


async def test_force_rollback(_in_atomic_mode):
    async with async_atomic():
        await insert_reporter(1)
        assert not async_connections[DEFAULT_DB_ALIAS].get_rollback()
        async_connections[DEFAULT_DB_ALIAS].set_rollback(True)
    assert await fetch_all_reporters() == []


async def test_prevent_rollback(_in_atomic_mode):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with async_atomic():
        reporter = await insert_reporter(1)
        sid = await connection.savepoint()
        with pytest.raises(DatabaseError):
            async with async_atomic(savepoint=False):
                async with await connection.cursor() as cursor:
                    await cursor.execute("SELECT no_such_col FROM reporter_table_tmp")
        assert connection.get_rollback()
        connection.set_rollback(False)
        await connection.savepoint_rollback(sid)
    assert await fetch_all_reporters() == [reporter]


async def test_failure_on_exit_transaction(_in_atomic_mode):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with async_atomic():
        try:
            with pytest.raises(DatabaseError):
                async with async_atomic():
                    await insert_reporter(1)
                    assert len(await fetch_all_reporters()) == 1
                    connection.savepoint_ids.append("12")

            with pytest.raises(transaction.TransactionManagementError):
                await fetch_all_reporters()
            assert connection.needs_rollback is True
        finally:
            if connection.savepoint_ids:
                connection.savepoint_ids.pop()
    assert await fetch_all_reporters() == []


# ── AsyncAtomic merge tests ───────────────────────────────────────────


async def test_merged_outer_rollback(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async with async_atomic():
        await insert_reporter(1)
        async with async_atomic(savepoint=False):
            await insert_reporter(2)
            with pytest.raises(Exception, match="Oops"):
                async with async_atomic(savepoint=False):
                    await insert_reporter(3)
                    raise Exception("Oops")
            # The third insert couldn't be rolled back.
            assert connection.get_rollback()
            connection.set_rollback(False)
            assert len(await fetch_all_reporters()) == 3
            connection.set_rollback(True)
        # The second insert couldn't be rolled back.
        assert connection.get_rollback()
        connection.set_rollback(False)
        assert len(await fetch_all_reporters()) == 3
        connection.set_rollback(True)
    # The first block has a savepoint and must roll back.
    assert await fetch_all_reporters() == []


async def test_merged_inner_savepoint_rollback(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async with async_atomic():
        reporter = await insert_reporter(1)
        async with async_atomic():
            await insert_reporter(2)
            with pytest.raises(Exception, match="Oops"):
                async with async_atomic(savepoint=False):
                    await insert_reporter(3)
                    raise Exception("Oops")
            assert connection.get_rollback()
            connection.set_rollback(False)
            assert len(await fetch_all_reporters()) == 3
            connection.set_rollback(True)
        # The second block has a savepoint and must roll back.
        assert len(await fetch_all_reporters()) == 1
    assert await fetch_all_reporters() == [reporter]


# ── AsyncAtomic errors ────────────────────────────────────────────────


_FORBIDDEN_ATOMIC_MSG = "This is forbidden when an 'atomic' block is active."


async def test_atomic_prevents_setting_autocommit(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    autocommit = await connection.get_autocommit()
    async with async_atomic():
        with pytest.raises(transaction.TransactionManagementError, match=_FORBIDDEN_ATOMIC_MSG):
            await connection.set_autocommit(not autocommit)

    assert await connection.get_autocommit() == autocommit


async def test_atomic_prevents_calling_transaction_methods(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with async_atomic():
        with pytest.raises(transaction.TransactionManagementError, match=_FORBIDDEN_ATOMIC_MSG):
            await connection.commit()
        with pytest.raises(transaction.TransactionManagementError, match=_FORBIDDEN_ATOMIC_MSG):
            await connection.rollback()


async def test_atomic_prevents_queries_in_broken_transaction(reporter_table_transaction):
    await insert_reporter(1)

    async with async_atomic():
        with pytest.raises(IntegrityError):
            async with async_mark_for_rollback_on_error(DEFAULT_DB_ALIAS):
                await insert_reporter(1)

        msg = (
            "An error occurred in the current transaction. You can't "
            "execute queries until the end of the 'atomic' block."
        )
        with pytest.raises(transaction.TransactionManagementError, match=msg) as cm:
            await insert_reporter(2)

        assert isinstance(cm.value.__cause__, IntegrityError)
    assert len(await fetch_all_reporters()) == 1


async def test_atomic_prevents_queries_in_broken_transaction_after_client_close(
    reporter_table_transaction,
):
    connection = async_connections[DEFAULT_DB_ALIAS]

    async with async_atomic():
        await insert_reporter(1)
        await connection.close()
        with pytest.raises(Error):
            await insert_reporter(2)
    assert len(await fetch_all_reporters()) == 0


# ── Non-async autocommit ──────────────────────────────────────────────


@pytest.fixture
async def non_autocommit_mode(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    await connection.set_autocommit(False)
    try:
        yield
    finally:
        await connection.rollback()
        await connection.set_autocommit(True)


async def test_orm_query_after_error_and_rollback(non_autocommit_mode):
    """ORM queries are allowed after an error and a rollback in non-autocommit
    mode (#27504)."""
    connection = async_connections[DEFAULT_DB_ALIAS]
    await insert_reporter(1)
    with pytest.raises(IntegrityError):
        await insert_reporter(1)
    await connection.rollback()
    await fetch_all_reporters()


async def test_orm_query_without_autocommit(non_autocommit_mode):
    """ORM queries must be possible after set_autocommit(False) (#24921)."""
    await insert_reporter(1)


# ── Independent connection ────────────────────────────────────────────


async def test_nested_independent_connection(reporter_table_transaction):
    async with async_connections._independent_connection():
        await insert_reporter(1)
        assert len(await fetch_all_reporters()) == 1
        async with async_connections._independent_connection():
            await insert_reporter(2)
            assert len(await fetch_all_reporters()) == 2


async def test_nested_independent_connection_with_transaction(reporter_table_transaction):
    async with async_connections._independent_connection(), async_atomic():
        await insert_reporter(1)
        assert len(await fetch_all_reporters()) == 1
        async with async_connections._independent_connection():
            await insert_reporter(2)
            assert len(await fetch_all_reporters()) == 1


async def test_nested_independent_connection_with_nested_transaction(reporter_table_transaction):
    async with async_connections._independent_connection(), async_atomic():
        await insert_reporter(1)
        assert len(await fetch_all_reporters()) == 1
        async with async_connections._independent_connection(), async_atomic():
            await insert_reporter(2)
            assert len(await fetch_all_reporters()) == 1


# ── Cross-task transaction guard ──────────────────────────────────────
#
# Child tasks created via gather/create_task inherit the parent's
# connection via ContextVar. If a child opens its own async_atomic(),
# it would create overlapping transactions on the shared connection,
# corrupting state. A guard in __aenter__ raises RuntimeError with
# a clear message instead of allowing silent corruption.
#
# Correct patterns:
#   - Wrap all tasks in a single parent async_atomic()
#   - Use _independent_connection() per task


async def test_child_task_atomic_raises(reporter_table_transaction):
    async def writer():
        async with async_atomic():
            await insert_reporter("should_fail")

    async with async_atomic():
        with pytest.raises(RuntimeError, match="nested task"):
            await asyncio.create_task(writer())


async def test_concurrent_gather_atomic_raises(reporter_table_transaction):
    """gather() where each task opens async_atomic() raises.

    This is the original issue #11 pattern: no parent atomic,
    each child independently opens async_atomic(). The first
    child stamps the connection, subsequent children see the
    mismatch.
    """
    errors = []
    barrier = asyncio.Barrier(10)

    async def writer(task_id):
        try:
            await barrier.wait()
            async with async_atomic():
                await insert_reporter(f"t{task_id}")
                await asyncio.sleep(0)
        except RuntimeError as e:
            errors.append((task_id, e))

    await asyncio.gather(*(writer(i) for i in range(10)))
    assert len(errors) > 0, "Expected RuntimeError from concurrent async_atomic()"


async def test_nested_savepoint_same_task_works(reporter_table_transaction):
    async with async_atomic():
        await insert_reporter("outer")
        async with async_atomic():
            await insert_reporter("inner")
    assert len(await fetch_all_reporters()) == 2


async def test_sequential_atomic_same_task_works(reporter_table_transaction):
    async with async_atomic():
        await insert_reporter("first")
    async with async_atomic():
        await insert_reporter("second")
    assert len(await fetch_all_reporters()) == 2


async def test_parent_atomic_avoids_corruption(reporter_table_transaction):
    barrier = asyncio.Barrier(10)
    errors = []

    async def writer(task_id):
        try:
            await barrier.wait()
            await insert_reporter(f"p{task_id}")
            await asyncio.sleep(0)
        except Exception as e:
            errors.append((task_id, e))

    async with async_atomic():
        await asyncio.gather(*(writer(i) for i in range(10)))

    assert errors == []
    assert len(await fetch_all_reporters()) == 10


async def test_independent_connection_avoids_corruption(reporter_table_transaction):
    barrier = asyncio.Barrier(10)
    errors = []

    async def writer(task_id):
        try:
            await barrier.wait()
            async with async_connections._independent_connection():
                async with async_atomic():
                    await insert_reporter(f"i{task_id}")
                    await asyncio.sleep(0)
        except Exception as e:
            errors.append((task_id, e))

    await asyncio.gather(*(writer(i) for i in range(10)))

    assert errors == []
    assert len(await fetch_all_reporters()) == 10


# ── Misc atomic behaviors ─────────────────────────────────────────────


async def test_wrap_callable_instance():
    """#20028 -- async_atomic must support wrapping callable instances."""

    class Callable:
        async def __call__(self):
            pass

    # Must not raise an exception
    async_atomic()(Callable())


async def test_atomic_does_not_leak_savepoints_on_failure(reporter_table_transaction):
    """#23074 -- Savepoints must be released after rollback."""
    connection = async_connections[DEFAULT_DB_ALIAS]
    with pytest.raises(Error):
        async with async_atomic():
            with pytest.raises(Exception, match="Oops"):
                async with async_atomic():
                    sid = connection.savepoint_ids[-1]
                    raise Exception("Oops")
            # The savepoint no longer exists; rolling back to it must fail.
            await connection.savepoint_rollback(sid)


async def test_mark_for_rollback_on_error_in_transaction(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with async_atomic(savepoint=False):
        with pytest.raises(Exception, match="Oops"):
            async with async_mark_for_rollback_on_error():
                assert connection.needs_rollback is False
                raise Exception("Oops")
        # mark_for_rollback_on_error marked the transaction as broken …
        assert connection.needs_rollback is True

        # … and further queries fail.
        msg = "You can't execute queries until the end of the 'atomic' block."
        with pytest.raises(transaction.TransactionManagementError, match=msg):
            await insert_reporter("boom")

    # Transaction errors are reset at the end of a transaction, so this
    # should just work.
    await insert_reporter(1)
    assert await fetch_all_reporters() == ["1"]


async def test_mark_for_rollback_on_error_in_autocommit(reporter_table_transaction):
    connection = async_connections[DEFAULT_DB_ALIAS]
    assert await connection.get_autocommit() is True

    with pytest.raises(Exception, match="Oops"):
        async with async_mark_for_rollback_on_error():
            assert connection.needs_rollback is False
            raise Exception("Oops")

    # mark_for_rollback_on_error does not mark the transaction as broken
    # when we are in autocommit mode.
    assert connection.needs_rollback is False

    # … and further queries work nicely.
    await insert_reporter(1)
    assert await fetch_all_reporters() == ["1"]
