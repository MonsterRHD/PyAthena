import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.util.concurrency import greenlet_spawn

from pyathena.aio.cursor import AioCursor
from pyathena.aio.sqlalchemy.base import AsyncAdapt_pyathena_cursor
from pyathena.converter import DefaultTypeConverter
from pyathena.cursor import Cursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.formatter import DefaultParameterFormatter
from pyathena.util import RetryConfig


@pytest.fixture(params=["sync", "async", "adapter"])
def rowcount_cursor(request):
    mode = request.param
    cursor_type = Cursor if mode == "sync" else AioCursor
    cursor = cursor_type(
        connection=Mock(_client_kwargs={}),
        converter=DefaultTypeConverter(),
        formatter=DefaultParameterFormatter(),
        retry_config=RetryConfig(),
    )
    mock_type = Mock if mode == "sync" else AsyncMock
    cursor._execute = mock_type(return_value="query-id")
    cursor._poll = mock_type(return_value=Mock(state="SUCCEEDED", substatement_type="UPDATE"))
    facade = AsyncAdapt_pyathena_cursor(cursor) if mode == "adapter" else cursor
    return mode, cursor, facade


def _responses(cursor, counts):
    cursor.connection.client.get_query_results.side_effect = [
        {
            "ResultSet": {"ResultSetMetadata": {"ColumnInfo": []}, "Rows": []},
            **({"UpdateCount": count} if count is not None else {}),
        }
        for count in counts
    ]


async def _invoke(context, method, *args, **kwargs):
    mode, _, facade = context
    fn = getattr(facade, method)
    if mode == "async":
        return await fn(*args, **kwargs)
    if mode == "adapter":
        return await greenlet_spawn(fn, *args, **kwargs)
    return fn(*args, **kwargs)


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ([1, 1, 0], 2),
        ([0, 0], 0),
        ([4], 4),
        ([], 0),
        ([1, None, 3], -1),
        ([None, 2], -1),
        ([1, None], -1),
    ],
)
async def test_executemany_rowcount(rowcount_cursor, counts, expected):
    _, cursor, facade = rowcount_cursor
    _responses(cursor, counts)
    parameters = [{"id": index} for index in range(len(counts))]
    assert facade.rowcount == -1
    result = await _invoke(
        rowcount_cursor,
        "executemany",
        "UPDATE t SET x=1 WHERE id=%(id)s",
        parameters,
        work_group="test-workgroup",
    )
    assert result is None
    assert facade.rowcount == expected
    assert facade.description is None
    assert cursor.result_set is None
    assert cursor.query_id is None
    assert cursor._execute.call_count == len(counts)
    assert cursor.connection.client.get_query_results.call_count == len(counts)
    for call, parameters_item in zip(cursor._execute.call_args_list, parameters, strict=True):
        assert call.kwargs["parameters"] == parameters_item
        assert call.kwargs["options"].work_group == "test-workgroup"
    facade.close()
    assert facade.rowcount == -1


async def test_executemany_replaces_previous_state(rowcount_cursor):
    _, cursor, facade = rowcount_cursor
    _responses(cursor, [5, 2, 1, 4, 0])
    await _invoke(rowcount_cursor, "execute", "UPDATE t SET x=1")
    previous = cursor.result_set
    await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [{}, {}])
    assert previous.is_closed
    assert facade.rowcount == 3
    await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [{}])
    assert facade.rowcount == 4
    await _invoke(rowcount_cursor, "execute", "UPDATE t SET x=1 WHERE id=99")
    assert facade.rowcount == 0
    await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [])
    assert facade.rowcount == 0
    assert facade.description is None


async def test_executemany_unknown_and_select_reset_previous_count(rowcount_cursor):
    _, cursor, facade = rowcount_cursor
    _responses(cursor, [7, None, 0, 0])
    await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [{}])
    assert facade.rowcount == 7
    await _invoke(rowcount_cursor, "execute", "UPDATE t SET x=1")
    assert facade.rowcount == -1
    cursor._poll.return_value.substatement_type = "SELECT"
    await _invoke(rowcount_cursor, "execute", "SELECT 1")
    assert facade.rowcount == -1
    await _invoke(rowcount_cursor, "executemany", "SELECT 1", [{}])
    assert facade.rowcount == -1
    if rowcount_cursor[0] != "adapter":
        for method in ("fetchone", "fetchmany", "fetchall"):
            with pytest.raises(ProgrammingError, match="No result set"):
                await _invoke(rowcount_cursor, method)
    else:
        assert facade.fetchall() == []


@pytest.mark.parametrize("failure_index", [0, 1])
async def test_executemany_failure_discards_partial_count(rowcount_cursor, failure_index):
    _, cursor, facade = rowcount_cursor
    _responses(cursor, [9, 2, 3])
    await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [{}])
    error = OperationalError("execution failed")
    execution = cursor._poll.return_value
    cursor._poll.side_effect = [execution] * failure_index + [error]
    with pytest.raises(OperationalError, match="execution failed") as caught:
        await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [{}, {}, {}])
    assert caught.value is error
    assert facade.rowcount == -1
    assert facade.description is None
    assert cursor.result_set is None
    cursor._poll.side_effect = None
    _responses(cursor, [3])
    await _invoke(rowcount_cursor, "execute", "UPDATE t SET x=1")
    assert facade.rowcount == 3


async def test_executemany_parameter_iteration_failure(rowcount_cursor):
    _, cursor, facade = rowcount_cursor
    _responses(cursor, [2])

    def parameters():
        yield {}
        raise ValueError("invalid parameters")

    with pytest.raises(ValueError, match="invalid parameters"):
        await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", parameters())
    assert facade.rowcount == -1
    assert cursor.result_set is None


@pytest.mark.parametrize("rowcount_cursor", ["async", "adapter"], indirect=True)
async def test_executemany_cancellation_discards_partial_count(rowcount_cursor):
    _, cursor, facade = rowcount_cursor
    _responses(cursor, [2])
    cursor._poll.side_effect = [cursor._poll.return_value, asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await _invoke(rowcount_cursor, "executemany", "UPDATE t SET x=1", [{}, {}])
    assert facade.rowcount == -1
    assert cursor.result_set is None


async def test_adapter_executemany_clears_buffered_rows():
    cursor = Mock()
    cursor.executemany = AsyncMock(side_effect=OperationalError("execution failed"))
    adapter = AsyncAdapt_pyathena_cursor(cursor)
    adapter._rows.append(("previous query",))
    with pytest.raises(OperationalError, match="execution failed"):
        await greenlet_spawn(adapter.executemany, "UPDATE t SET x=1", [{}], work_group="test")
    assert adapter.fetchall() == []
    cursor.executemany.assert_awaited_once_with("UPDATE t SET x=1", [{}], work_group="test")
