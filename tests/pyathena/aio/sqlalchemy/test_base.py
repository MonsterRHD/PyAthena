import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.sql.schema import MetaData, Table
from sqlalchemy.util.concurrency import greenlet_spawn

from pyathena.aio.sqlalchemy.base import AsyncAdapt_pyathena_cursor
from pyathena.error import OperationalError
from tests import ENV


class TestAsyncSQLAlchemyAthena:
    @pytest.mark.parametrize(
        "async_engine",
        [
            {"driver": "aiorest"},
            {"driver": "aiopandas"},
            {"driver": "aioarrow"},
            {"driver": "aiopolars"},
            {"driver": "aios3fs"},
        ],
        indirect=True,
    )
    async def test_basic_query(self, async_engine):
        engine, conn = async_engine
        rows = (await conn.execute(text("SELECT * FROM one_row"))).fetchall()
        assert len(rows) == 1
        assert rows[0].number_of_rows == 1
        assert len(rows[0]) == 1

    async def test_unicode(self, async_engine):
        _, conn = async_engine
        unicode_str = "密林"
        returned_str = (
            await conn.execute(
                sqlalchemy.select(
                    sqlalchemy.sql.expression.bindparam(
                        "あまぞん", unicode_str, type_=sqlalchemy.types.String()
                    )
                )
            )
        ).scalar()
        assert returned_str == unicode_str

    async def test_reflect_table(self, async_engine):
        _, conn = async_engine
        one_row = await conn.run_sync(
            lambda sync_conn: Table("one_row", MetaData(schema=ENV.schema), autoload_with=sync_conn)
        )
        assert len(one_row.c) == 1
        assert one_row.c.number_of_rows is not None
        assert one_row.comment == "table comment"

    async def test_reflect_schemas(self, async_engine):
        _, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_schema_names()

        schemas = await conn.run_sync(_inspect)
        assert ENV.schema in schemas
        assert "default" in schemas

    async def test_get_table_names(self, async_engine):
        _, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_table_names(schema=ENV.schema)

        table_names = await conn.run_sync(_inspect)
        assert "many_rows" in table_names

    async def test_has_table(self, async_engine):
        _, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return (
                insp.has_table("one_row", schema=ENV.schema),
                insp.has_table("this_table_does_not_exist", schema=ENV.schema),
            )

        exists, not_exists = await conn.run_sync(_inspect)
        assert exists
        assert not not_exists

    async def test_get_columns(self, async_engine):
        _, conn = async_engine

        def _inspect(sync_conn):
            insp = sqlalchemy.inspect(sync_conn)
            return insp.get_columns(table_name="one_row", schema=ENV.schema)

        columns = await conn.run_sync(_inspect)
        actual = columns[0]
        assert actual["name"] == "number_of_rows"
        assert isinstance(actual["type"], sqlalchemy.types.INTEGER)
        assert actual["nullable"]
        assert actual["default"] is None
        assert not actual["autoincrement"]
        assert actual["comment"] == "some comment"


class TestAsyncAdaptPyAthenaCursor:
    @pytest.mark.parametrize("rowcount", [2, 0, -1])
    async def test_executemany(self, rowcount):
        aio_cursor = MagicMock(rowcount=rowcount, description=None)
        aio_cursor.executemany = AsyncMock()
        cursor = AsyncAdapt_pyathena_cursor(aio_cursor)
        cursor._rows.append(("previous query",))
        parameters = [{"id": 1}, {"id": 2}]

        result = await greenlet_spawn(
            cursor.executemany,
            "UPDATE t SET x=1 WHERE id=%(id)s",
            parameters,
            work_group="test-workgroup",
        )

        assert result is None
        aio_cursor.executemany.assert_awaited_once_with(
            "UPDATE t SET x=1 WHERE id=%(id)s", parameters, work_group="test-workgroup"
        )
        aio_cursor.execute.assert_not_called()
        assert cursor.rowcount == rowcount
        assert cursor.description is None
        assert cursor.fetchall() == []

    @pytest.mark.parametrize(
        "error", [OperationalError("execution failed"), asyncio.CancelledError()]
    )
    async def test_executemany_failure_clears_buffered_rows(self, error):
        aio_cursor = MagicMock()
        aio_cursor.executemany = AsyncMock(side_effect=error)
        cursor = AsyncAdapt_pyathena_cursor(aio_cursor)
        cursor._rows.append(("previous query",))

        with pytest.raises(type(error)) as caught:
            await greenlet_spawn(cursor.executemany, "UPDATE t SET x=1", [{}])

        assert caught.value is error
        assert cursor.fetchall() == []
        aio_cursor.executemany.assert_awaited_once_with("UPDATE t SET x=1", [{}])
