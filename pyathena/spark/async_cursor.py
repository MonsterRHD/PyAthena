import logging
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, cast

from pyathena.error import ProgrammingError
from pyathena.model import AthenaCalculationExecution
from pyathena.spark.common import SparkBaseCursor

if TYPE_CHECKING:
    from pyathena.model import AthenaQueryExecution

_logger = logging.getLogger(__name__)


class AsyncSparkCursor(SparkBaseCursor):
    """Asynchronous cursor for executing PySpark code on Amazon Athena for Apache Spark.

    This cursor provides asynchronous execution of PySpark code on Athena's managed
    Spark environment. It's designed for non-blocking big data processing, ETL
    operations, and machine learning workloads without blocking the main thread.

    Session and calculation ownership follows the same rules as the synchronous
    :class:`~pyathena.spark.cursor.SparkCursor`: borrowed sessions are never
    terminated, the last owner of a client-created session terminates it, and
    close stops only the calculations the cursor started. Worker tasks that
    are still running when close begins resolve with an exception instead of
    publishing late data.

    Attributes:
        max_workers: Maximum number of worker threads for async operations.
        session_id: The Athena Spark session ID.
        engine_configuration: Spark engine configuration settings.

    Example:
        >>> from pyathena.spark.async_cursor import AsyncSparkCursor
        >>>
        >>> cursor = connection.cursor(
        ...     AsyncSparkCursor,
        ...     engine_configuration={
        ...         'CoordinatorDpuSize': 1,
        ...         'MaxConcurrentDpus': 20
        ...     }
        ... )
        >>>
        >>> # Execute PySpark code asynchronously
        >>> spark_code = '''
        ... df = spark.read.table("my_database.my_table")
        ... result = df.groupBy("category").count()
        ... result.show()
        ... '''
        >>> calculation_id, future = cursor.execute(spark_code)
        >>>
        >>> # Get result when ready
        >>> calc_execution = future.result()
        >>> stdout_future = cursor.get_std_out(calc_execution)
        >>> if stdout_future:
        ...     output = stdout_future.result()
        ...     print(output)

    Note:
        Requires an Athena workgroup configured for Spark calculations.
        Spark sessions have associated costs and idle timeout settings.
        The cursor manages a thread pool for asynchronous operations.
    """

    def __init__(
        self,
        session_id: str | None = None,
        description: str | None = None,
        engine_configuration: dict[str, Any] | None = None,
        notebook_version: str | None = None,
        session_idle_timeout_minutes: int | None = None,
        max_workers: int = (cpu_count() or 1) * 5,
        **kwargs,
    ):
        # Create the executor before the base constructor registers the
        # cursor and its session ownership, so a failure here (e.g. invalid
        # max_workers) cannot leave a phantom owner in the registry.
        executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            super().__init__(
                session_id=session_id,
                description=description,
                engine_configuration=engine_configuration,
                notebook_version=notebook_version,
                session_idle_timeout_minutes=session_idle_timeout_minutes,
                **kwargs,
            )
        except BaseException:
            executor.shutdown(wait=False)
            raise
        self._max_workers = max_workers
        self._executor = executor

    def close(self, wait: bool = False) -> None:
        """Close the cursor and shut down the worker pool.

        Calculations started by this cursor are stopped if still running and
        the session is released per ownership. The shutdown of the thread
        pool follows ``wait``; with the default ``wait=False`` in-flight
        futures resolve with an exception rather than late data.

        Args:
            wait: Block until all submitted futures complete.
        """
        try:
            super().close()
        finally:
            self._executor.shutdown(wait=wait)

    def _submit(self, fn: Callable[..., Any], *args: Any) -> Future[Any]:
        self._raise_if_closing()

        def guarded() -> Any:
            self._raise_if_closing()
            result = fn(*args)
            self._raise_if_closing()
            return result

        return self._executor.submit(guarded)

    def calculation_execution(self, query_id: str) -> "Future[AthenaCalculationExecution]":
        return cast(
            "Future[AthenaCalculationExecution]",
            self._submit(self._get_calculation_execution, query_id),
        )

    def get_std_out(
        self, calculation_execution: AthenaCalculationExecution
    ) -> "Future[str] | None":
        self._raise_if_closing()
        if not calculation_execution.std_out_s3_uri:
            return None

        def read_std_out() -> str:
            text = self._read_s3_file_as_text(calculation_execution.std_out_s3_uri)
            self._raise_if_closing()
            return text

        return self._executor.submit(read_std_out)

    def get_std_error(
        self, calculation_execution: AthenaCalculationExecution
    ) -> "Future[str] | None":
        self._raise_if_closing()
        if not calculation_execution.std_error_s3_uri:
            return None

        def read_std_error() -> str:
            text = self._read_s3_file_as_text(calculation_execution.std_error_s3_uri)
            self._raise_if_closing()
            return text

        return self._executor.submit(read_std_error)

    def poll(self, query_id: str) -> "Future[AthenaCalculationExecution]":
        return cast("Future[AthenaCalculationExecution]", self._submit(self._poll, query_id))

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | list[str] | None = None,
        session_id: str | None = None,
        description: str | None = None,
        client_request_token: str | None = None,
        work_group: str | None = None,
        **kwargs,
    ) -> tuple[str, "Future[AthenaQueryExecution | AthenaCalculationExecution]"]:
        self._raise_if_closing()
        calculation_id = self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        # Register before the future starts so close stops the calculation
        # even if execute's caller never inspects the future. If close already
        # converged while the start request was in flight, stop the new
        # calculation instead of handing out a future nobody will clean up.
        if self._register_started_calculation(calculation_id):
            self._stop_after_missed_close(calculation_id)
            raise ProgrammingError("Cannot publish a calculation started while closing.")
        return calculation_id, self._submit(self._poll, calculation_id)

    def cancel(self, query_id: str) -> "Future[None]":
        self._raise_if_closing()
        return self._executor.submit(self._cancel, query_id)
