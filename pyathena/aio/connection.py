from __future__ import annotations

import asyncio
import inspect
from typing import Any

from pyathena.aio.cursor import AioCursor
from pyathena.connection import Connection
from pyathena.error import OperationalError, ProgrammingError


class AioConnection(Connection[AioCursor]):
    """Async-aware connection to Amazon Athena.

    Wraps the synchronous ``Connection`` with async context manager support
    and provides ``create()`` for non-blocking initialization.

    Example:
        >>> async def example():
        ...     async with await AioConnection.create(
        ...         s3_staging_dir="s3://bucket/path/",
        ...         region_name="us-east-1",
        ...     ) as conn:
        ...         async with conn.cursor() as cursor:
        ...             await cursor.execute("SELECT 1")
        ...             print(await cursor.fetchone())
    """

    def __init__(self, **kwargs: Any) -> None:
        if "cursor_class" not in kwargs:
            kwargs["cursor_class"] = AioCursor
        super().__init__(**kwargs)

    @classmethod
    async def create(
        cls,
        **kwargs: Any,
    ) -> AioConnection:
        """Async factory for creating an ``AioConnection``.

        Runs the (potentially blocking) ``__init__`` in a thread so that
        STS calls (``role_arn`` / ``serial_number``) do not block the loop.

        Args:
            **kwargs: Arguments forwarded to ``AioConnection.__init__``.

        Returns:
            A fully initialized ``AioConnection``.
        """
        return await asyncio.to_thread(cls, **kwargs)

    async def aclose(self) -> None:
        """Close the connection and its open Spark cursors asynchronously.

        Applies the same ownership rules as the synchronous
        :meth:`Connection.close`: each cursor stops its unfinished
        calculations, borrowed sessions survive, and the last owner of a
        client-created session terminates it. Failures are surfaced and
        ``aclose()`` stays retryable.
        """
        errors: list[Exception] = []
        for cursor in self._spark_open_cursors():
            close = cursor.close
            try:
                if inspect.iscoroutinefunction(close):
                    await close()
                else:
                    await asyncio.to_thread(close)
            except Exception as e:
                errors.append(e)
        if errors:
            raise OperationalError(
                "Failed to close one or more Spark cursors. Remote calculations or "
                "sessions may still be running; retrying connection.aclose() reattempts "
                "the unfinished cleanup."
            ) from errors[0]

    def close(self) -> None:
        """Synchronous close is only supported without asyncio Spark cursors.

        Native asyncio cursors cannot be driven from synchronous code inside
        a running event loop. Use :meth:`aclose` (or the async context
        manager) when :class:`~pyathena.aio.spark.cursor.AioSparkCursor`
        cursors are open.
        """
        if any(inspect.iscoroutinefunction(cursor.close) for cursor in self._spark_open_cursors()):
            raise ProgrammingError(
                "This connection has native asyncio cursors; "
                "use `await connection.aclose()` to close it."
            )
        super().close()

    async def __aenter__(self) -> AioConnection:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()
