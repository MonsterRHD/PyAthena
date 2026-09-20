from __future__ import annotations

import logging
import threading
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, cast

import botocore

from pyathena import NotSupportedError, OperationalError, ProgrammingError
from pyathena.common import BaseCursor
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaQueryExecution,
    AthenaSessionStatus,
)
from pyathena.util import parse_output_location, retry_api_call

_logger = logging.getLogger(__name__)

_TERMINAL_CALCULATION_STATES = frozenset(
    {
        AthenaCalculationExecutionStatus.STATE_COMPLETED,
        AthenaCalculationExecutionStatus.STATE_FAILED,
        AthenaCalculationExecutionStatus.STATE_CANCELED,
    }
)
_UNUSABLE_SESSION_STATES = frozenset(
    {
        AthenaSessionStatus.STATE_TERMINATED,
        AthenaSessionStatus.STATE_DEGRADED,
        AthenaSessionStatus.STATE_FAILED,
    }
)


class SparkBaseCursor(BaseCursor, metaclass=ABCMeta):
    """Abstract base class for Spark-enabled cursor implementations.

    This class provides the foundational functionality for executing PySpark code
    on Amazon Athena for Apache Spark. It manages Spark sessions, handles
    calculation execution lifecycle, and provides utilities for reading
    results from S3.

    Session ownership:
        A session explicitly provided via ``session_id`` is *borrowed* unless
        another cursor of the same connection started it. Closing a cursor
        that borrows a session only stops the calculations started by that
        cursor and never terminates the session. A session started by a
        cursor is *client-owned*; ownership is reference counted on the
        connection and only the last owner terminates the session.

    Calculation ownership:
        Every calculation started by a cursor is tracked. On close (or when
        execute/cancel/poll races with close), calculations that have not
        reached a terminal state are stopped idempotently and their terminal
        state is awaited. Late polls and stdout/stderr/result reads after
        close has begun raise instead of publishing data.

    Attributes:
        session_id: The Athena Spark session identifier.
        calculation_id: ID of the current calculation being executed.
        engine_configuration: DPU and resource configuration for Spark.

    Note:
        This is an abstract base class used by concrete Spark cursor implementations
        like SparkCursor and AsyncSparkCursor. It should not be instantiated directly.
    """

    def __init__(
        self,
        session_id: str | None = None,
        description: str | None = None,
        engine_configuration: dict[str, Any] | None = None,
        notebook_version: str | None = None,
        session_idle_timeout_minutes: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._engine_configuration = (
            engine_configuration
            if engine_configuration
            else self.get_default_engine_configuration()
        )
        self._notebook_version = notebook_version
        self._session_description = description
        self._session_idle_timeout_minutes = session_idle_timeout_minutes

        # State guarded by ``_lock`` for the thread-based async cursor, whose
        # worker threads race with close() calls from other threads. The
        # native asyncio cursor relies on cooperative scheduling and uses the
        # same short critical sections without awaiting inside them.
        self._lock = threading.RLock()
        # Serializes concurrent close() calls so the session ownership
        # count is released exactly once per cursor.
        self._close_lock = threading.Lock()
        # calculation ID -> terminal execution, or None while it may run
        self._calculations: dict[str, AthenaCalculationExecution | None] = {}
        self._closing = False
        self._closed = False

        # Create the S3 client before acquiring any session ownership so a
        # construction failure cannot leak an ownership count.
        self._client = self.connection.session.client(
            "s3",
            region_name=self.connection.region_name,
            config=self.connection.config,
            **self.connection._client_kwargs,
        )

        if session_id:
            acquired = self.connection._spark_acquire_session(session_id)
            if acquired == "terminating":
                raise OperationalError(
                    f"Session: {session_id} is being terminated by another cursor "
                    f"of this connection."
                )
            if acquired == "joined":
                # Another cursor of this connection created the session; this
                # cursor shares its ownership. Verify liveness because the
                # session may have been terminated (e.g. idle timeout).
                try:
                    self._assert_session_usable(session_id)
                except Exception:
                    self.connection._spark_leave_session(session_id)
                    raise
                self._session_id = session_id
                self._owns_session = True
            elif self._exists_session(session_id):
                # The session is managed outside this connection: borrow it.
                self._session_id = session_id
                self._owns_session = False
            else:
                raise OperationalError(f"Session: {session_id} not found.")
        else:
            self._session_id = self._start_session()
            self.connection._spark_register_session(self._session_id)
            self._owns_session = True

        self._calculation_id: str | None = None
        self._calculation_execution: AthenaCalculationExecution | None = None
        self.connection._spark_register_cursor(self)

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def calculation_id(self) -> str | None:
        return self._calculation_id

    @property
    def is_closed(self) -> bool:
        """True once close has completed successfully."""
        return self._closed

    @staticmethod
    def get_default_engine_configuration() -> dict[str, Any]:
        return {
            "CoordinatorDpuSize": 1,
            "MaxConcurrentDpus": 2,
            "DefaultExecutorDpuSize": 1,
        }

    # --- close state and calculation ownership ---

    def _raise_if_closing(self) -> None:
        """Gate for operations that must not publish data during shutdown."""
        if self._closing:
            raise ProgrammingError("Operation on a closed or closing Spark cursor.")

    def _register_calculation(self, calculation_id: str) -> None:
        with self._lock:
            self._calculations.setdefault(calculation_id, None)

    def _register_started_calculation(self, calculation_id: str) -> bool:
        """Register a just-started calculation and report whether close ran.

        ``execute()`` calls this immediately after the start API returns. If
        close already converged while the start request was in flight, the
        caller must stop the newly started calculation itself because the
        close could not have observed it.

        Returns:
            True if the cursor has already fully closed.
        """
        with self._lock:
            self._calculations.setdefault(calculation_id, None)
            return self._closed

    def _record_terminal_calculation(
        self, calculation_id: str, execution: AthenaCalculationExecution
    ) -> None:
        with self._lock:
            self._calculations[calculation_id] = execution

    def _active_calculations(self) -> list[str]:
        with self._lock:
            return [
                calculation_id
                for calculation_id, execution in self._calculations.items()
                if execution is None
            ]

    @staticmethod
    def _is_invalid_request_error(exc: Exception) -> bool:
        return (
            isinstance(exc, botocore.exceptions.ClientError)
            and exc.response.get("Error", {}).get("Code") == "InvalidRequestException"
        )

    def _read_s3_file_as_text(self, uri) -> str:
        bucket, key = parse_output_location(uri)
        response = retry_api_call(
            self._client.get_object,
            config=self._retry_config,
            logger=_logger,
            Bucket=bucket,
            Key=key,
        )
        return cast(str, response["Body"].read().decode("utf-8").strip())

    def _get_session_status(self, session_id: str) -> AthenaSessionStatus:
        request: dict[str, Any] = {"SessionId": session_id}
        try:
            response = retry_api_call(
                self._connection.client.get_session_status,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get session status.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaSessionStatus(response)

    def _wait_for_idle_session(self, session_id: str):
        while True:
            session_status = self._get_session_status(session_id)
            if session_status.state in [AthenaSessionStatus.STATE_IDLE]:
                break
            if session_status.state in _UNUSABLE_SESSION_STATES:
                raise OperationalError(session_status.state_change_reason)
            time.sleep(self._poll_interval)

    def _assert_session_usable(self, session_id: str) -> None:
        """Raise unless the session is in a state that accepts calculations."""
        session_status = self._get_session_status(session_id)
        if session_status.state in _UNUSABLE_SESSION_STATES:
            raise OperationalError(f"Session: {session_id} is in state {session_status.state}.")

    def _exists_session(self, session_id: str) -> bool:
        request = {"SessionId": session_id}
        try:
            retry_api_call(
                self._connection.client.get_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            if self._is_invalid_request_error(e):
                _logger.exception(f"Session: {session_id} not found.")
                return False
            raise OperationalError(*e.args) from e
        else:
            self._wait_for_idle_session(session_id)
            return True

    def _start_session(self) -> str:
        request: dict[str, Any] = {
            "WorkGroup": self._work_group,
            "EngineConfiguration": self._engine_configuration,
        }
        if self._session_description:
            request.update({"Description": self._session_description})
        if self._notebook_version:
            request.update({"NotebookVersion": self._notebook_version})
        if self._session_idle_timeout_minutes:
            request.update({"SessionIdleTimeoutInMinutes": self._session_idle_timeout_minutes})
        try:
            session_id: str = retry_api_call(
                self._connection.client.start_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )["SessionId"]
        except Exception as e:
            _logger.exception("Failed to start session.")
            raise OperationalError(*e.args) from e
        else:
            self._wait_for_idle_session(session_id)
            return session_id

    def _terminate_session(self) -> None:
        """Terminate the session; idempotent for an already terminated one."""
        request = {"SessionId": self._session_id}
        try:
            retry_api_call(
                self._connection.client.terminate_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            # A racing termination (another owner path, idle timeout) leaves
            # the session in TERMINATED; treat that as success.
            if self._is_invalid_request_error(e) and self._is_session_terminated():
                _logger.warning("Session %s was already terminated.", self._session_id)
                return
            _logger.exception("Failed to terminate session.")
            raise OperationalError(*e.args) from e

    def _is_session_terminated(self) -> bool:
        try:
            session_status = self._get_session_status(self._session_id)
        except Exception:
            return False
        return session_status.state == AthenaSessionStatus.STATE_TERMINATED

    def __poll(self, query_id: str) -> AthenaQueryExecution | AthenaCalculationExecution:
        while True:
            self._raise_if_closing()
            calculation_status = self._get_calculation_execution_status(query_id)
            self._raise_if_closing()
            if self._on_poll:
                self._on_poll(calculation_status)
            if calculation_status.state in _TERMINAL_CALCULATION_STATES:
                calculation_execution = self._get_calculation_execution(query_id)
                self._record_terminal_calculation(query_id, calculation_execution)
                # Do not publish the result when close started while the
                # terminal state was being fetched.
                self._raise_if_closing()
                return calculation_execution
            time.sleep(self._poll_interval)

    def _poll(self, query_id: str) -> AthenaQueryExecution | AthenaCalculationExecution:
        try:
            query_execution = self.__poll(query_id)
        except KeyboardInterrupt as e:
            if self._kill_on_interrupt:
                _logger.warning("Query canceled by user.")
                self._cancel(query_id)
                query_execution = self.__poll(query_id)
            else:
                raise e
        return query_execution

    def _cancel(self, query_id: str) -> None:
        request = {"CalculationExecutionId": query_id}
        try:
            retry_api_call(
                self._connection.client.stop_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel calculation.")
            raise OperationalError(*e.args) from e

    def _stop_calculation(self, calculation_id: str) -> AthenaCalculationExecution:
        """Stop a calculation unless it already finished, then reach terminal.

        Idempotent and safe under execute/cancel/close races: if the
        calculation reaches a terminal state while the stop request is in
        flight, or the stop races with an identical request, the terminal
        execution is recorded and returned.
        """
        status = self._get_calculation_execution_status(calculation_id)
        if status.state not in _TERMINAL_CALCULATION_STATES:
            try:
                retry_api_call(
                    self._connection.client.stop_calculation_execution,
                    config=self._retry_config,
                    logger=_logger,
                    CalculationExecutionId=calculation_id,
                )
            except Exception as e:
                if not self._is_invalid_request_error(e):
                    _logger.exception("Failed to stop calculation.")
                    raise OperationalError(*e.args) from e
                # The stop may be rejected because the calculation already
                # reached a terminal state or the session is gone.
                status = self._get_calculation_execution_status(calculation_id)
                if status.state not in _TERMINAL_CALCULATION_STATES:
                    _logger.exception("Failed to stop calculation.")
                    raise OperationalError(*e.args) from e
        while True:
            if status.state in _TERMINAL_CALCULATION_STATES:
                break
            time.sleep(self._poll_interval)
            status = self._get_calculation_execution_status(calculation_id)
        execution = self._get_calculation_execution(calculation_id)
        self._record_terminal_calculation(calculation_id, execution)
        return execution

    def _stop_active_calculations(self) -> list[Exception]:
        errors: list[Exception] = []
        for calculation_id in self._active_calculations():
            try:
                self._stop_calculation(calculation_id)
            except Exception as e:  # noqa: PERF203
                _logger.exception("Failed to stop calculation %s.", calculation_id)
                errors.append(e)
        return errors

    def _stop_after_missed_close(self, calculation_id: str) -> None:
        """Best-effort stop for a start that returned after close converged.

        The cursor is already unregistered, so failures are only logged;
        for a client-owned session the terminating/terminated session
        force-stops the calculation server-side anyway.
        """
        try:
            self._stop_calculation(calculation_id)
        except Exception:
            _logger.exception(
                "Failed to stop calculation %s that started while the cursor was closing.",
                calculation_id,
            )

    def _release_owned_session(self) -> BaseException | None:
        if not self._owns_session:
            return None
        if not self.connection._spark_release_session(self._session_id):
            return None
        try:
            self._terminate_session()
        except BaseException as e:
            # Keep the ownership claim (also on KeyboardInterrupt / task
            # cancellation) so a later close() retries instead of losing the
            # session while termination may still be in flight.
            self.connection._spark_rearm_session(self._session_id)
            return e
        self.connection._spark_mark_session_terminated(self._session_id)
        return None

    def close(self) -> None:
        """Stop owned calculations and release the session per ownership.

        Borrowed sessions are never terminated; the last owner of a
        client-owned session terminates it. Concurrent calls are serialized:
        the method is idempotent after success and retryable after a
        failure, in which case it raises
        :class:`~pyathena.error.OperationalError` describing the unfinished
        remote cleanup. Session ownership is only released after this
        cursor's calculations are stopped, so a failed stop can never
        release (and terminate) a session that another owner still counts.
        """
        with self._close_lock:
            if self._closed:
                return
            self._closing = True
            errors = self._stop_active_calculations()
            if errors:
                raise OperationalError(
                    f"Failed to stop active calculations of the Spark cursor for "
                    f"session {self._session_id}. Retrying close() reattempts the "
                    f"unfinished cleanup."
                ) from errors[0]
            session_error = self._release_owned_session()
            if session_error is not None:
                if isinstance(session_error, BaseException) and not isinstance(
                    session_error, Exception
                ):
                    raise session_error
                raise OperationalError(
                    f"Failed to fully close Spark cursor for session "
                    f"{self._session_id}. Retrying close() reattempts the unfinished "
                    f"cleanup; the session may still be running."
                ) from session_error
            self._closed = True
            self.connection._spark_unregister_cursor(self)

    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        raise NotSupportedError


class WithCalculationExecution:
    """Mixin class providing access to Spark calculation execution properties.

    This mixin provides property accessors for calculation execution metadata
    and status information. It's designed to be mixed with cursor classes
    that execute Spark calculations on Athena.

    Properties:
        - description: Human-readable description of the calculation
        - working_directory: S3 path where calculation files are stored
        - state: Current execution state (COMPLETED, FAILED, etc.)
        - state_change_reason: Explanation for state changes
        - submission_date_time: When the calculation was submitted
        - completion_date_time: When the calculation completed
        - dpu_execution_in_millis: DPU execution time in milliseconds
        - progress: Current execution progress information
        - std_out_s3_uri: S3 URI for standard output
        - std_error_s3_uri: S3 URI for standard error
        - result_s3_uri: S3 URI for calculation results
        - result_type: Type of result produced by the calculation

    Note:
        This class requires that the implementing class provides
        calculation_execution, session_id, and calculation_id properties.
    """

    def __init__(self):
        super().__init__()

    @property
    @abstractmethod
    def calculation_execution(self) -> AthenaCalculationExecution | None:
        raise NotImplementedError  # pragma: no cover

    @property
    @abstractmethod
    def session_id(self) -> str:
        raise NotImplementedError  # pragma: no cover

    @property
    @abstractmethod
    def calculation_id(self) -> str | None:
        raise NotImplementedError  # pragma: no cover

    @property
    def description(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.description

    @property
    def working_directory(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.working_directory

    @property
    def state(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.state

    @property
    def state_change_reason(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.state_change_reason

    @property
    def submission_date_time(self) -> datetime | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.submission_date_time

    @property
    def completion_date_time(self) -> datetime | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.completion_date_time

    @property
    def dpu_execution_in_millis(self) -> int | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.dpu_execution_in_millis

    @property
    def progress(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.progress

    @property
    def std_out_s3_uri(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.std_out_s3_uri

    @property
    def std_error_s3_uri(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.std_error_s3_uri

    @property
    def result_s3_uri(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.result_s3_uri

    @property
    def result_type(self) -> str | None:
        if not self.calculation_execution:
            return None
        return self.calculation_execution.result_type
