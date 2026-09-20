"""Ownership and lifecycle rules for Spark sessions and calculations (no AWS).

Covers the synchronous, thread-based and native asyncio Spark cursors:
borrowed versus client-owned sessions, calculation ownership, idempotent
stop/terminate, close races, and connection close using the same rules.
"""

import asyncio
import threading
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from pyathena.aio.connection import AioConnection
from pyathena.aio.spark.cursor import AioSparkCursor
from pyathena.connection import Connection
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import (
    AthenaCalculationExecutionStatus,
    AthenaSessionStatus,
)
from pyathena.spark.async_cursor import AsyncSparkCursor
from pyathena.spark.cursor import SparkCursor
from pyathena.util import RetryConfig

RUNNING = AthenaCalculationExecutionStatus.STATE_RUNNING
COMPLETED = AthenaCalculationExecutionStatus.STATE_COMPLETED
CANCELED = AthenaCalculationExecutionStatus.STATE_CANCELED
TERMINATED = AthenaSessionStatus.STATE_TERMINATED


def _status(state: str) -> MagicMock:
    mock = MagicMock()
    mock.state = state
    return mock


def _client_error(code: str = "InvalidRequestException") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "mock failure"}}, "OperationName")


@pytest.fixture
def conn() -> Connection:
    return Connection(
        region_name="us-east-1",
        work_group="wg",
        aws_access_key_id="x",
        aws_secret_access_key="x",
        retry_config=RetryConfig(attempt=1),
    )


def _make_cursor(
    conn: Connection,
    cursor_class: type = SparkCursor,
    *,
    session_id: str | None = None,
    owned_session_id: str = "owned-session",
    poll_interval: float = 0,
) -> Any:
    with (
        patch.object(cursor_class, "_start_session", return_value=owned_session_id),
        patch.object(cursor_class, "_exists_session", return_value=True),
        patch.object(cursor_class, "_assert_session_usable"),
    ):
        return conn.cursor(
            cursor_class,
            session_id=session_id,
            poll_interval=poll_interval,
            kill_on_interrupt=False,
        )


def _stub_calculation(
    cursor: Any,
    statuses: list,
    *,
    execution: MagicMock | None = None,
    stop_side_effect: Any = None,
) -> None:
    cursor._get_calculation_execution_status = MagicMock(side_effect=statuses)
    cursor._get_calculation_execution = MagicMock(return_value=execution or _status(CANCELED))
    cursor._connection.client.stop_calculation_execution = MagicMock(side_effect=stop_side_effect)
    cursor._connection.client.terminate_session = MagicMock()


# --- synchronous SparkCursor ---


class TestSparkCursorOwnership:
    def test_borrowed_session_is_not_terminated_but_active_calculation_stopped(self, conn):
        cursor = _make_cursor(conn, session_id="external-session")
        assert cursor._owns_session is False
        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])
        cursor._register_calculation("calc-1")

        cursor.close()

        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="calc-1"
        )
        cursor._connection.client.terminate_session.assert_not_called()
        assert "external-session" not in conn._spark_session_owners
        assert cursor.is_closed

    def test_close_is_idempotent(self, conn):
        cursor = _make_cursor(conn, session_id="external-session")
        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])
        cursor._register_calculation("calc-1")

        cursor.close()
        cursor.close()

        assert cursor._connection.client.stop_calculation_execution.call_count == 1
        assert cursor._connection.client.terminate_session.call_count == 0

    def test_completed_calculation_is_not_stopped_on_close(self, conn):
        cursor = _make_cursor(conn, session_id="external-session")
        _stub_calculation(cursor, [])
        cursor._register_calculation("calc-1")
        cursor._record_terminal_calculation("calc-1", _status(COMPLETED))

        cursor.close()

        cursor._connection.client.stop_calculation_execution.assert_not_called()
        cursor._connection.client.terminate_session.assert_not_called()

    def test_owned_session_is_terminated_on_close(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        assert cursor._owns_session is True
        _stub_calculation(cursor, [])

        cursor.close()

        cursor._connection.client.terminate_session.assert_called_once_with(SessionId="owned-1")
        assert conn._spark_open_cursors() == ()

    def test_last_owner_terminates_shared_session(self, conn):
        first = _make_cursor(conn, owned_session_id="shared")
        second = _make_cursor(conn, session_id="shared")
        _stub_calculation(first, [])
        _stub_calculation(second, [])
        assert second._owns_session is True

        first.close()
        first._connection.client.terminate_session.assert_not_called()
        # The surviving owner keeps working on the same session.
        assert second.session_id == "shared"

        second.close()
        second._connection.client.terminate_session.assert_called_once_with(SessionId="shared")

    def test_consecutive_calculations_only_active_one_is_stopped(self, conn):
        cursor = _make_cursor(conn, session_id="external-session")
        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])
        cursor._register_calculation("old")
        cursor._record_terminal_calculation("old", _status(COMPLETED))
        cursor._register_calculation("active")

        cursor.close()

        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="active"
        )

    def test_stop_rejected_but_terminal_state_reached_is_success(self, conn):
        cursor = _make_cursor(conn, session_id="external-session")
        _stub_calculation(
            cursor,
            [_status(RUNNING), _status(CANCELED)],
            stop_side_effect=_client_error(),
        )
        cursor._register_calculation("calc-1")

        cursor.close()

        cursor._connection.client.terminate_session.assert_not_called()
        assert cursor.is_closed

    def test_terminate_failure_keeps_retryable_state(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        _stub_calculation(cursor, [])
        cursor._connection.client.terminate_session = MagicMock(
            side_effect=_client_error("InternalServerException")
        )

        with pytest.raises(OperationalError):
            cursor.close()
        assert cursor.is_closed is False
        assert conn._spark_open_cursors() == (cursor,)
        assert conn._spark_session_owners["owned-1"] == 1
        with pytest.raises(OperationalError):
            conn.close()

        cursor._connection.client.terminate_session = MagicMock()
        cursor.close()
        assert cursor.is_closed
        assert conn._spark_open_cursors() == ()

    def test_terminate_already_terminated_session_is_idempotent_success(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        _stub_calculation(cursor, [])
        cursor._connection.client.terminate_session = MagicMock(side_effect=_client_error())
        cursor._get_session_status = MagicMock(return_value=_status(TERMINATED))

        cursor.close()

        assert cursor.is_closed
        cursor._connection.client.terminate_session.assert_called_once()

    def test_poll_failure_leaves_calculation_that_close_stops(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        cursor._calculate = MagicMock(return_value="calc-1")
        cursor._get_calculation_execution_status = MagicMock(
            side_effect=[_status(RUNNING), OperationalError("poll failed")]
        )

        with pytest.raises(OperationalError, match="poll failed"):
            cursor.execute("import time; time.sleep(60)")
        assert cursor.calculation_id == "calc-1"
        assert cursor._calculations["calc-1"] is None
        assert cursor.calculation_execution is None

        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])
        cursor.close()
        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="calc-1"
        )

    def test_normal_completion_publishes_results_and_reads_until_close(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        execution = MagicMock()
        execution.state = COMPLETED
        execution.std_out_s3_uri = "s3://bucket/stdout"
        execution.std_error_s3_uri = None
        cursor._calculate = MagicMock(return_value="calc-1")
        cursor._get_calculation_execution_status = MagicMock(return_value=_status(COMPLETED))
        cursor._get_calculation_execution = MagicMock(return_value=execution)
        cursor._read_s3_file_as_text = MagicMock(return_value="hello")

        cursor.execute("print('hello')")
        assert cursor.get_std_out() == "hello"

        cursor._connection.client.terminate_session = MagicMock()
        cursor.close()
        with pytest.raises(ProgrammingError):
            cursor.get_std_out()
        with pytest.raises(ProgrammingError):
            cursor.get_std_error()
        with pytest.raises(ProgrammingError):
            cursor.execute("print('again')")

    def test_late_terminal_poll_after_close_does_not_publish(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        _stub_calculation(cursor, [])
        cursor.close()
        with pytest.raises(ProgrammingError):
            cursor._poll("late-calc")

    def test_concurrent_close_releases_session_ownership_once(self, conn):
        cursor = _make_cursor(conn, owned_session_id="owned-1")
        _stub_calculation(cursor, [])
        barrier = threading.Barrier(2)
        errors: list[Exception] = []

        def close_cursor() -> None:
            barrier.wait()
            try:
                cursor.close()
            except Exception as e:  # pragma: no cover - surfaced via assertions
                errors.append(e)

        workers = [threading.Thread(target=close_cursor) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert errors == []
        assert cursor._connection.client.terminate_session.call_count == 1
        assert conn._spark_session_owners == {}

    def test_failed_calculation_stop_never_releases_other_owner(self, conn):
        first = _make_cursor(conn, owned_session_id="shared")
        second = _make_cursor(conn, session_id="shared")
        stop_mock = MagicMock()
        terminate_mock = MagicMock()
        conn.client.stop_calculation_execution = stop_mock
        conn.client.terminate_session = terminate_mock
        first._get_calculation_execution_status = MagicMock(
            side_effect=_client_error("InternalServerException")
        )
        first._register_calculation("calc-a")

        with pytest.raises(OperationalError):
            first.close()
        with pytest.raises(OperationalError):
            first.close()
        # Both failed attempts kept the ownership claims intact.
        assert conn._spark_session_owners["shared"] == 2

        first._get_calculation_execution_status = MagicMock(
            side_effect=[_status(RUNNING), _status(CANCELED)]
        )
        first._get_calculation_execution = MagicMock(return_value=_status(CANCELED))
        first.close()
        assert conn._spark_session_owners["shared"] == 1
        terminate_mock.assert_not_called()

        second.close()
        stop_mock.assert_called_once_with(CalculationExecutionId="calc-a")
        terminate_mock.assert_called_once_with(SessionId="shared")

    def test_start_returning_after_close_is_stopped_and_not_published(self, conn):
        cursor = _make_cursor(conn, session_id="external")
        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])

        def start_then_close(**kwargs) -> str:
            cursor.close()
            return "late-calc"

        cursor._calculate = MagicMock(side_effect=start_then_close)

        with pytest.raises(ProgrammingError):
            cursor.execute("print('late')")
        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="late-calc"
        )
        cursor._connection.client.terminate_session.assert_not_called()
        assert cursor.is_closed

    def test_session_being_terminated_cannot_be_borrowed(self, conn):
        _make_cursor(conn, owned_session_id="shared")
        # Simulate the last owner's release phase (terminate in flight).
        assert conn._spark_release_session("shared") is True
        try:
            with pytest.raises(OperationalError, match="being terminated"):
                _make_cursor(conn, session_id="shared")
            assert conn._spark_terminating_sessions == {"shared"}
        finally:
            conn._spark_mark_session_terminated("shared")

    def test_joining_dead_session_raises_without_touching_ownership(self, conn):
        _make_cursor(conn, owned_session_id="shared")
        with (
            patch.object(SparkCursor, "_start_session", return_value="shared"),
            patch.object(SparkCursor, "_exists_session", return_value=True),
            patch.object(
                SparkCursor,
                "_assert_session_usable",
                side_effect=OperationalError("TERMINATED"),
            ),
            pytest.raises(OperationalError, match="TERMINATED"),
        ):
            conn.cursor(
                SparkCursor,
                session_id="shared",
                poll_interval=0,
                kill_on_interrupt=False,
            )
        assert conn._spark_session_owners["shared"] == 1

    def test_join_confirm_rejects_session_released_during_liveness(self, conn):
        owner = _make_cursor(conn, owned_session_id="shared")

        def release_during_liveness(session_id) -> None:
            # The owner reaches its close-release phase while this cursor is
            # still performing its liveness check.
            assert conn._spark_release_session("shared") is True

        with (
            patch.object(SparkCursor, "_start_session", return_value="shared"),
            patch.object(SparkCursor, "_exists_session", return_value=True),
            patch.object(
                SparkCursor, "_assert_session_usable", side_effect=release_during_liveness
            ),
            pytest.raises(OperationalError, match="being terminated"),
        ):
            conn.cursor(
                SparkCursor,
                session_id="shared",
                poll_interval=0,
                kill_on_interrupt=False,
            )
        # The failed construction added neither an owner nor a cursor.
        assert conn._spark_open_cursors() == (owner,)
        assert conn._spark_session_owners == {}
        conn._spark_mark_session_terminated("shared")

    def test_start_returning_during_closing_phase_is_stopped(self, conn):
        cursor = _make_cursor(conn, session_id="external")
        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])

        def start_while_closing(**kwargs) -> str:
            # close set the closing flag but has not snapshotted/finished yet.
            with cursor._lock:
                cursor._closing = True
            return "late-calc"

        cursor._calculate = MagicMock(side_effect=start_while_closing)

        with pytest.raises(ProgrammingError):
            cursor.execute("print('late')")
        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="late-calc"
        )
        cursor._connection.client.terminate_session.assert_not_called()


# --- connection close ---


class TestConnectionClose:
    def test_connection_close_uses_cursor_rules(self, conn):
        stop_mock = MagicMock()
        terminate_mock = MagicMock()
        conn.client.stop_calculation_execution = stop_mock
        conn.client.terminate_session = terminate_mock
        borrowed = _make_cursor(conn, session_id="external")
        _make_cursor(conn, owned_session_id="owned")
        borrowed._register_calculation("calc-b")
        borrowed._get_calculation_execution_status = MagicMock(
            side_effect=[_status(RUNNING), _status(CANCELED)]
        )
        borrowed._get_calculation_execution = MagicMock(return_value=_status(CANCELED))

        conn.close()

        assert stop_mock.call_args_list == [call(CalculationExecutionId="calc-b")]
        assert terminate_mock.call_args_list == [call(SessionId="owned")]
        assert conn._spark_open_cursors() == ()

    def test_connection_close_failure_is_retryable(self, conn):
        owned = _make_cursor(conn, owned_session_id="owned")
        _stub_calculation(owned, [])
        owned._connection.client.terminate_session = MagicMock(
            side_effect=_client_error("InternalServerException")
        )

        with pytest.raises(OperationalError):
            conn.close()
        assert conn._spark_open_cursors() == (owned,)

        owned._connection.client.terminate_session = MagicMock()
        conn.close()
        assert conn._spark_open_cursors() == ()


# --- thread-based AsyncSparkCursor ---


class TestAsyncSparkCursorOwnership:
    def test_invalid_max_workers_leaves_no_registered_owner(self, conn):
        with (
            patch.object(AsyncSparkCursor, "_start_session", return_value="owned-1"),
            patch.object(AsyncSparkCursor, "_exists_session", return_value=True),
            patch.object(AsyncSparkCursor, "_assert_session_usable"),
            pytest.raises(ValueError, match="max_workers"),
        ):
            conn.cursor(
                AsyncSparkCursor,
                poll_interval=0,
                kill_on_interrupt=False,
                max_workers=0,
            )
        assert conn._spark_open_cursors() == ()
        assert conn._spark_session_owners == {}

    def test_execute_registers_calculation_and_close_terminates_owned(self, conn):
        cursor = _make_cursor(conn, AsyncSparkCursor, owned_session_id="owned-1")
        execution = _status(COMPLETED)
        cursor._calculate = MagicMock(return_value="calc-1")
        cursor._get_calculation_execution_status = MagicMock(return_value=_status(COMPLETED))
        cursor._get_calculation_execution = MagicMock(return_value=execution)
        cursor._connection.client.terminate_session = MagicMock()

        calculation_id, future = cursor.execute("print('hello')")
        assert calculation_id == "calc-1"
        assert future.result() is execution
        assert "calc-1" in cursor._calculations

        cursor.close(wait=True)
        cursor._connection.client.terminate_session.assert_called_once()

    def test_borrowed_close_stops_active_calculation_without_terminate(self, conn):
        cursor = _make_cursor(conn, AsyncSparkCursor, session_id="external")
        entered = threading.Event()

        def status_side(calculation_id):
            if not cursor._closing:
                entered.set()
                return _status(RUNNING)
            return _status(CANCELED)

        cursor._calculate = MagicMock(return_value="calc-1")
        cursor._get_calculation_execution_status = MagicMock(side_effect=status_side)
        cursor._get_calculation_execution = MagicMock(return_value=_status(CANCELED))
        cursor._connection.client.stop_calculation_execution = MagicMock()
        cursor._connection.client.terminate_session = MagicMock()

        _, future = cursor.execute("import time; time.sleep(60)")
        assert entered.wait(5)

        cursor.close(wait=True)

        with pytest.raises(ProgrammingError):
            future.result()
        cursor._connection.client.terminate_session.assert_not_called()

    def test_close_races_poll_stops_calculation_and_publishes_nothing(self, conn):
        cursor = _make_cursor(conn, AsyncSparkCursor, owned_session_id="owned-1")
        entered = threading.Event()
        stop_observed = {"seen": False}

        def status_side(calculation_id):
            if not cursor._closing:
                entered.set()
                return _status(RUNNING)
            # First status observed by close is still running, so the stop
            # API must be requested; afterwards the calculation is canceled.
            if not stop_observed["seen"]:
                stop_observed["seen"] = True
                return _status(RUNNING)
            return _status(CANCELED)

        cursor._calculate = MagicMock(return_value="calc-1")
        cursor._get_calculation_execution_status = MagicMock(side_effect=status_side)
        cursor._get_calculation_execution = MagicMock(return_value=_status(CANCELED))
        cursor._connection.client.stop_calculation_execution = MagicMock()
        cursor._connection.client.terminate_session = MagicMock()

        _, future = cursor.execute("import time; time.sleep(60)")
        assert entered.wait(5)

        cursor.close(wait=True)

        with pytest.raises(ProgrammingError):
            future.result()
        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="calc-1"
        )
        cursor._connection.client.terminate_session.assert_called_once()

    def test_reads_after_close_are_rejected(self, conn):
        cursor = _make_cursor(conn, AsyncSparkCursor, session_id="external")
        cursor._connection.client.terminate_session = MagicMock()
        cursor.close(wait=True)
        with pytest.raises(ProgrammingError):
            cursor.get_std_out(_status(COMPLETED))
        with pytest.raises(ProgrammingError):
            cursor.poll("calc-1")

    def test_start_returning_after_close_is_stopped_and_not_published(self, conn):
        cursor = _make_cursor(conn, AsyncSparkCursor, session_id="external")
        _stub_calculation(cursor, [_status(RUNNING), _status(CANCELED)])

        def start_then_close(**kwargs) -> str:
            cursor.close(wait=True)
            return "late-calc"

        cursor._calculate = MagicMock(side_effect=start_then_close)

        with pytest.raises(ProgrammingError):
            cursor.execute("print('late')")
        cursor._connection.client.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId="late-calc"
        )
        cursor._connection.client.terminate_session.assert_not_called()


# --- native asyncio AioSparkCursor ---


class TestAioSparkCursorOwnership:
    @pytest.fixture
    def aio_conn(self) -> AioConnection:
        return AioConnection(
            region_name="us-east-1",
            work_group="wg",
            aws_access_key_id="x",
            aws_secret_access_key="x",
            retry_config=RetryConfig(attempt=1),
        )

    @pytest.fixture
    async def aio_cursor(self, aio_conn):
        async def factory(**kwargs):
            return await asyncio.to_thread(lambda: _make_cursor(aio_conn, AioSparkCursor, **kwargs))

        return factory

    @staticmethod
    def _async_mock(return_value: Any) -> MagicMock:
        async def _coro(*args: Any, **kwargs: Any) -> Any:
            return return_value

        return MagicMock(side_effect=_coro)

    async def test_borrowed_close_stops_active_calculation_without_terminate(
        self, aio_conn, aio_cursor
    ):
        cursor = await aio_cursor(session_id="external")
        entered = asyncio.Event()

        async def status_side(calculation_id):
            if not cursor._closing:
                entered.set()
                return _status(RUNNING)
            # Running until the stop request lands, then canceled.
            return _status(RUNNING if not stop_mock.called else CANCELED)

        cursor._calculate = self._async_mock("calc-1")
        cursor._get_calculation_execution_status = MagicMock(side_effect=status_side)
        cursor._get_calculation_execution = self._async_mock(_status(CANCELED))
        stop_mock = MagicMock()
        terminate_mock = MagicMock()
        cursor._connection.client.stop_calculation_execution = stop_mock
        cursor._connection.client.terminate_session = terminate_mock

        task = asyncio.create_task(cursor.execute("import time; time.sleep(60)"))
        await entered.wait()

        await cursor.close()

        with pytest.raises(ProgrammingError):
            await task
        stop_mock.assert_called_once_with(CalculationExecutionId="calc-1")
        terminate_mock.assert_not_called()

    async def test_last_owner_terminates_shared_session(self, aio_conn, aio_cursor):
        terminate_mock = MagicMock()
        aio_conn.client.terminate_session = terminate_mock
        first = await aio_cursor(owned_session_id="shared")
        second = await aio_cursor(session_id="shared")

        await first.close()
        assert terminate_mock.call_args_list == []
        await second.close()
        assert terminate_mock.call_args_list == [call(SessionId="shared")]

    async def test_terminate_failure_is_retryable(self, aio_conn, aio_cursor):
        cursor = await aio_cursor(owned_session_id="owned-1")
        cursor._connection.client.terminate_session = MagicMock(
            side_effect=_client_error("InternalServerException")
        )

        with pytest.raises(OperationalError):
            await cursor.close()
        assert cursor.is_closed is False
        assert aio_conn._spark_session_owners["owned-1"] == 1

        cursor._connection.client.terminate_session = MagicMock()
        await cursor.close()
        assert cursor.is_closed

    async def test_reads_and_execute_after_close_are_rejected(self, aio_cursor):
        cursor = await aio_cursor(session_id="external")
        cursor._connection.client.terminate_session = MagicMock()
        await cursor.close()

        with pytest.raises(ProgrammingError):
            await cursor.get_std_out()
        with pytest.raises(ProgrammingError):
            await cursor.get_std_error()
        with pytest.raises(ProgrammingError):
            await cursor.execute("print('no')")
        await cursor.close()  # idempotent

    async def test_connection_sync_close_rejects_async_cursor(self, aio_conn, aio_cursor):
        cursor = await aio_cursor(owned_session_id="owned-1")
        cursor._connection.client.terminate_session = MagicMock()

        with pytest.raises(ProgrammingError):
            aio_conn.close()
        assert cursor.is_closed is False

        await aio_conn.aclose()
        assert cursor.is_closed

    async def test_aclose_closes_owned_and_leaves_borrowed(self, aio_conn, aio_cursor):
        terminate_mock = MagicMock()
        aio_conn.client.terminate_session = terminate_mock
        await aio_cursor(session_id="external")
        await aio_cursor(owned_session_id="owned-1")

        await aio_conn.aclose()
        assert terminate_mock.call_args_list == [call(SessionId="owned-1")]

    async def test_concurrent_close_releases_session_ownership_once(self, aio_conn, aio_cursor):
        cursor = await aio_cursor(owned_session_id="owned-1")
        terminate_mock = MagicMock()
        cursor._connection.client.terminate_session = terminate_mock

        await asyncio.gather(cursor.close(), cursor.close())

        assert terminate_mock.call_count == 1
        assert aio_conn._spark_session_owners == {}

    async def test_start_returning_after_close_is_stopped_and_not_published(
        self, aio_conn, aio_cursor
    ):
        cursor = await aio_cursor(session_id="external")

        async def status_side(calculation_id):
            return _status(RUNNING if not stop_mock.called else CANCELED)

        async def start_then_close(**kwargs) -> str:
            await cursor.close()
            return "late-calc"

        stop_mock = MagicMock()
        terminate_mock = MagicMock()
        cursor._connection.client.stop_calculation_execution = stop_mock
        cursor._connection.client.terminate_session = terminate_mock
        cursor._get_calculation_execution_status = MagicMock(side_effect=status_side)
        cursor._get_calculation_execution = self._async_mock(_status(CANCELED))
        cursor._calculate = MagicMock(side_effect=start_then_close)

        with pytest.raises(ProgrammingError):
            await cursor.execute("print('late')")
        stop_mock.assert_called_once_with(CalculationExecutionId="late-calc")
        terminate_mock.assert_not_called()

    async def test_cancellation_during_terminate_rearms_ownership(self, aio_conn, aio_cursor):
        cursor = await aio_cursor(owned_session_id="owned-1")
        terminate_started = asyncio.Event()

        async def hang_terminate():
            terminate_started.set()
            await asyncio.Event().wait()  # cancelled instead of returning

        cursor._terminate_session = hang_terminate

        task = asyncio.create_task(cursor.close())
        await terminate_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cursor.is_closed is False
        assert aio_conn._spark_session_owners["owned-1"] == 1
        assert cursor in aio_conn._spark_open_cursors()
