from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

from pyathena.aio.util import async_retry_api_call
from pyathena.error import DatabaseError, NotSupportedError, OperationalError, ProgrammingError
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaCalculationExecutionStatus,
    AthenaQueryExecution,
    AthenaSessionStatus,
)
from pyathena.spark.common import (
    _TERMINAL_CALCULATION_STATES,
    SparkBaseCursor,
    WithCalculationExecution,
)
from pyathena.util import parse_output_location

_logger = logging.getLogger(__name__)


class AioSparkCursor(SparkBaseCursor, WithCalculationExecution):
    """Native asyncio cursor for executing PySpark code on Athena.

    Overrides post-init I/O methods of ``SparkBaseCursor`` with async
    equivalents.  Session management (``_exists_session``,
    ``_start_session``, etc.) stays synchronous because ``__init__``
    runs inside ``asyncio.to_thread``.

    Since ``SparkBaseCursor.__init__`` performs I/O (session management),
    cursor creation must be wrapped in ``asyncio.to_thread``::

        cursor = await asyncio.to_thread(conn.cursor)

    Example:
        >>> import asyncio
        >>> async with await pyathena.aio_connect(
        ...     work_group="spark-workgroup",
        ...     cursor_class=AioSparkCursor,
        ... ) as conn:
        ...     cursor = await asyncio.to_thread(conn.cursor)
        ...     await cursor.execute("spark.sql('SELECT 1').show()")
        ...     print(await cursor.get_std_out())
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
        super().__init__(
            session_id=session_id,
            description=description,
            engine_configuration=engine_configuration,
            notebook_version=notebook_version,
            session_idle_timeout_minutes=session_idle_timeout_minutes,
            **kwargs,
        )
        # Serializes concurrent close() coroutines; created after the
        # blocking base __init__ (which runs in asyncio.to_thread) and used
        # only from the event loop thread.
        self._aclose_lock = asyncio.Lock()

    @property
    def calculation_execution(self) -> AthenaCalculationExecution | None:
        return self._calculation_execution

    # --- async overrides of SparkBaseCursor I/O methods ---

    async def _get_calculation_execution_status(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaCalculationExecutionStatus:
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            response = await async_retry_api_call(
                self._connection.client.get_calculation_execution_status,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get calculation execution status.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaCalculationExecutionStatus(response)

    async def _get_calculation_execution(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaCalculationExecution:
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            response = await async_retry_api_call(
                self._connection.client.get_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get calculation execution.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaCalculationExecution(response)

    async def _calculate(  # type: ignore[override]
        self,
        session_id: str,
        code_block: str,
        description: str | None = None,
        client_request_token: str | None = None,
    ) -> str:
        request = self._build_start_calculation_execution_request(
            session_id=session_id,
            code_block=code_block,
            description=description,
            client_request_token=client_request_token,
        )
        try:
            response = await async_retry_api_call(
                self._connection.client.start_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
            calculation_id = response.get("CalculationExecutionId")
        except Exception as e:
            _logger.exception("Failed to execute calculation.")
            raise DatabaseError(*e.args) from e
        return cast(str, calculation_id)

    async def _aget_session_status(self, session_id: str) -> AthenaSessionStatus:
        request: dict[str, Any] = {"SessionId": session_id}
        try:
            response = await async_retry_api_call(
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

    async def __poll(self, query_id: str) -> AthenaQueryExecution | AthenaCalculationExecution:
        while True:
            self._raise_if_closing()
            calculation_status = await self._get_calculation_execution_status(query_id)
            self._raise_if_closing()
            if self._on_poll:
                self._on_poll(calculation_status)
            if calculation_status.state in _TERMINAL_CALCULATION_STATES:
                calculation_execution = await self._get_calculation_execution(query_id)
                self._record_terminal_calculation(query_id, calculation_execution)
                # Do not publish the result when close started while the
                # terminal state was being fetched.
                self._raise_if_closing()
                return calculation_execution
            await asyncio.sleep(self._poll_interval)

    async def _poll(  # type: ignore[override]
        self, query_id: str
    ) -> AthenaQueryExecution | AthenaCalculationExecution:
        try:
            query_execution = await self.__poll(query_id)
        except asyncio.CancelledError:
            if self._kill_on_interrupt and not self._closing:
                _logger.warning("Query canceled by user.")
                await self._cancel(query_id)
                query_execution = await self.__poll(query_id)
            else:
                raise
        return query_execution

    async def _cancel(self, query_id: str) -> None:  # type: ignore[override]
        request: dict[str, Any] = {"CalculationExecutionId": query_id}
        try:
            await async_retry_api_call(
                self._connection.client.stop_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel calculation.")
            raise OperationalError(*e.args) from e

    async def _stop_calculation(  # type: ignore[override]
        self, calculation_id: str
    ) -> AthenaCalculationExecution:
        """Async counterpart of the idempotent stop-and-reach-terminal flow."""
        status = await self._get_calculation_execution_status(calculation_id)
        if status.state not in _TERMINAL_CALCULATION_STATES:
            try:
                await async_retry_api_call(
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
                status = await self._get_calculation_execution_status(calculation_id)
                if status.state not in _TERMINAL_CALCULATION_STATES:
                    _logger.exception("Failed to stop calculation.")
                    raise OperationalError(*e.args) from e
        while True:
            if status.state in _TERMINAL_CALCULATION_STATES:
                break
            await asyncio.sleep(self._poll_interval)
            status = await self._get_calculation_execution_status(calculation_id)
        execution = await self._get_calculation_execution(calculation_id)
        self._record_terminal_calculation(calculation_id, execution)
        return execution

    async def _terminate_session(self) -> None:  # type: ignore[override]
        """Terminate the session; idempotent for an already terminated one."""
        request: dict[str, Any] = {"SessionId": self._session_id}
        try:
            await async_retry_api_call(
                self._connection.client.terminate_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            # A racing termination (another owner path, idle timeout) leaves
            # the session in TERMINATED; treat that as success.
            if self._is_invalid_request_error(e):
                session_status = await self._aget_session_status(self._session_id)
                if session_status.state == AthenaSessionStatus.STATE_TERMINATED:
                    _logger.warning("Session %s was already terminated.", self._session_id)
                    return
            _logger.exception("Failed to terminate session.")
            raise OperationalError(*e.args) from e

    async def _read_s3_file_as_text(self, uri) -> str:  # type: ignore[override]
        bucket, key = parse_output_location(uri)
        response = await async_retry_api_call(
            self._client.get_object,
            config=self._retry_config,
            logger=_logger,
            Bucket=bucket,
            Key=key,
        )
        return cast(str, response["Body"].read().decode("utf-8").strip())

    # --- public API ---

    async def get_std_out(self) -> str | None:
        """Get the standard output from the Spark calculation execution.

        Returns:
            The standard output as a string, or None if no output is available.

        Raises:
            ProgrammingError: If the cursor is closed or closing.
        """
        self._raise_if_closing()
        if not self._calculation_execution or not self._calculation_execution.std_out_s3_uri:
            return None
        text = await self._read_s3_file_as_text(self._calculation_execution.std_out_s3_uri)
        self._raise_if_closing()
        return text

    async def get_std_error(self) -> str | None:
        """Get the standard error from the Spark calculation execution.

        Returns:
            The standard error as a string, or None if no error output is available.

        Raises:
            ProgrammingError: If the cursor is closed or closing.
        """
        self._raise_if_closing()
        if not self._calculation_execution or not self._calculation_execution.std_error_s3_uri:
            return None
        text = await self._read_s3_file_as_text(self._calculation_execution.std_error_s3_uri)
        self._raise_if_closing()
        return text

    async def execute(  # type: ignore[override]
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        session_id: str | None = None,
        description: str | None = None,
        client_request_token: str | None = None,
        work_group: str | None = None,
        **kwargs,
    ) -> AioSparkCursor:
        """Execute PySpark code asynchronously.

        Args:
            operation: PySpark code to execute.
            parameters: Unused, kept for API compatibility.
            session_id: Spark session ID override.
            description: Calculation description.
            client_request_token: Idempotency token.
            work_group: Unused, kept for API compatibility.
            **kwargs: Additional parameters.

        Returns:
            Self reference for method chaining.
        """
        self._raise_if_closing()
        self._calculation_id = await self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        # Register before polling so close stops a calculation whose poll
        # fails or is cancelled.
        self._register_calculation(self._calculation_id)
        calculation_execution = cast(
            AthenaCalculationExecution, await self._poll(self._calculation_id)
        )
        # A close that raced with the terminal poll owns publication now.
        self._raise_if_closing()
        self._calculation_execution = calculation_execution
        if self._calculation_execution.state != AthenaCalculationExecutionStatus.STATE_COMPLETED:
            std_error = await self.get_std_error()
            raise OperationalError(std_error)
        return self

    async def cancel(self) -> None:
        """Cancel the currently running calculation.

        Raises:
            ProgrammingError: If no calculation is running.
        """
        if not self.calculation_id:
            raise ProgrammingError("CalculationExecutionId is none or empty.")
        await self._cancel(self.calculation_id)

    async def close(self) -> None:  # type: ignore[override]
        """Stop owned calculations and release the session per ownership.

        Borrowed sessions are never terminated; the last owner of a
        client-owned session terminates it. Concurrent calls are serialized:
        the method is idempotent after success and retryable after a
        failure, in which case it raises
        :class:`~pyathena.error.OperationalError` describing the unfinished
        remote cleanup.
        """
        async with self._aclose_lock:
            if self._closed:
                return
            self._closing = True
            errors: list[Exception] = []
            for calculation_id in self._active_calculations():
                try:
                    await self._stop_calculation(calculation_id)
                except Exception as e:  # noqa: PERF203
                    _logger.exception("Failed to stop calculation %s.", calculation_id)
                    errors.append(e)
            if self._owns_session and self.connection._spark_release_session(self._session_id):
                try:
                    await self._terminate_session()
                except Exception as e:
                    # Keep the ownership claim so a later close() can retry.
                    self.connection._spark_rearm_session(self._session_id)
                    errors.append(e)
            if errors:
                raise OperationalError(
                    f"Failed to fully close Spark cursor for session "
                    f"{self._session_id}. Retrying close() reattempts the unfinished "
                    f"cleanup; the session may still be running."
                ) from errors[0]
            self._closed = True
            self.connection._spark_unregister_cursor(self)

    async def executemany(  # type: ignore[override]
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any] | list[str] | None],
        **kwargs,
    ) -> None:
        raise NotSupportedError

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
