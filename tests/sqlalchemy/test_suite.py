import logging

import pytest
from sqlalchemy import CHAR, VARCHAR, Integer, String, func, inspect, select
from sqlalchemy import exc as sa_exc
from sqlalchemy import testing as sa_testing
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.testing import eq_, fixtures
from sqlalchemy.testing.schema import Column, Table
from sqlalchemy.testing.suite import *  # noqa: F403
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _ComponentReflectionTestExtra
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest

del BinaryTest  # noqa: F821
del CompositeKeyReflectionTest  # noqa: F821
del CTETest  # noqa: F821
del DateTimeMicrosecondsTest  # noqa: F821
del DifficultParametersTest  # noqa: F821
del DistinctOnTest  # noqa: F821
del HasIndexTest  # noqa: F821
del IdentityAutoincrementTest  # noqa: F821
del JoinTest  # noqa: F821
del TimeMicrosecondsTest  # noqa: F821
del TimeTest  # noqa: F821
del TimestampMicrosecondsTest  # noqa: F821
del UuidTest  # noqa: F821


class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    @sa_testing.variation("criteria", ["rows", "norows", "aggregate"])
    @sa_testing.requires.update_where_target_in_subquery
    def test_update_where_target_in_subquery(self, connection, criteria):
        t = self.tables.plain_pk
        if criteria.rows:
            subquery = select(t.c.id).where(t.c.id < 3)
            expected = [(1, "updated"), (2, "updated"), (3, "d3")]
            rowcount = 2
        elif criteria.norows:
            subquery = select(t.c.id).where(t.c.id < 0)
            expected = [(1, "d1"), (2, "d2"), (3, "d3")]
            rowcount = 0
        elif criteria.aggregate:
            subquery = select(func.max(t.c.id))
            expected = [(1, "d1"), (2, "d2"), (3, "updated")]
            rowcount = 1
        else:
            criteria.fail()

        r = connection.execute(t.update().where(t.c.id.in_(subquery)), {"data": "updated"})
        assert not r.is_insert
        assert not r.returns_rows
        assert r.rowcount == rowcount
        eq_(connection.execute(t.select().order_by(t.c.id)).fetchall(), expected)


class ComponentReflectionTest(_ComponentReflectionTest):
    @classmethod
    def define_reflected_tables(cls, metadata, schema):
        super().define_reflected_tables(metadata, schema)
        # Iceberg has STRING but does not support CHAR.
        key = f"{schema}.users" if schema else "users"
        metadata.tables[key].c.test1.type = String()
        for table in metadata.tables.values():
            # The upstream fixture unconditionally indexes email_address.
            table.indexes.clear()
        for name in ("comment_test", "no_constraints"):
            key = f"{schema}.{name}" if schema else name
            # Athena does not persist Iceberg table comments. Keep the other
            # reflection fixtures on the suite's default Iceberg table type.
            options = metadata.tables[key].dialect_options["awsathena"]
            options["tblproperties"] = {"classification": "parquet"}
            options["file_format"] = "PARQUET"
        key = f"{schema}.comment_test" if schema else "comment_test"
        # Glue rejects newline and carriage return characters in Hive column comments.
        metadata.tables[key].c.d3.comment = "Comment with escapes"

    def test_get_comments(self, connection):
        self._test_get_comments(connection)

    @sa_testing.requires.schemas
    def test_get_comments_with_schema(self, connection):
        self._test_get_comments(connection, sa_testing.config.test_schema)

    @sa_testing.combinations((True, sa_testing.requires.schemas), False, argnames="use_schema")
    def test_get_hive_multi_table_comment(self, connection, use_schema):
        schema = sa_testing.config.test_schema if use_schema else None
        names = ["comment_test", "no_constraints", "users"]
        expected = self.exp_comments(schema=schema)
        assert inspect(connection).get_multi_table_comment(schema=schema, filter_names=names) == {
            (schema, name): expected[(schema, name)] for name in names
        }

    def exp_columns(self, *args, **kwargs):
        columns = super().exp_columns(*args, **kwargs)
        for table_columns in columns.values():
            for column in table_columns:
                # Athena DDL does not create NOT NULL or auto-increment constraints.
                column["nullable"] = True
                column["autoincrement"] = False
                if column["name"] == "d3":
                    column["comment"] = "Comment with escapes"
        return columns

    @sa_testing.requires.autoincrement_insert
    def test_autoincrement_col(self, connection):
        super().test_autoincrement_col(connection)


class ComponentReflectionTestExtra(_ComponentReflectionTestExtra):
    def test_reuses_listed_table_metadata(self, connection, metadata):
        table = Table("listed_metadata", metadata, Column("id", Integer, comment="identifier"))
        table.create(connection)
        inspector = inspect(connection)
        client = connection.connection.driver_connection.client
        calls = []

        def record_call(model, **kwargs):
            calls.append(model.name)

        client.meta.events.register("before-call.athena", record_call)
        try:
            assert table.name in inspector.get_table_names()
            assert inspector.get_columns(table.name)[0]["comment"] == "identifier"
            assert inspector.get_table_options(table.name)["awsathena_location"]
            assert inspector.has_table(table.name)
            assert "ListTableMetadata" in calls
            assert "GetTableMetadata" not in calls
            inspector.clear_cache()
            assert inspector.get_columns(table.name)[0]["name"] == "id"
            assert calls.count("GetTableMetadata") == 1
        finally:
            client.meta.events.unregister("before-call.athena", record_call)

    @sa_testing.combinations((String, None), (VARCHAR, 52), (CHAR, 52), argnames="type_,length")
    def test_hive_string_length_reflection(self, connection, metadata, type_, length):
        table = Table(
            "string_length",
            metadata,
            Column("data", type_(52)),
            awsathena_tblproperties={"classification": "parquet"},
            awsathena_file_format="PARQUET",
        )
        table.create(connection)
        reflected_type = inspect(connection).get_columns(table.name)[0]["type"]
        assert isinstance(reflected_type, type_)
        # Generic String compiles to Hive STRING without a length constraint.
        assert reflected_type.length == length

    def test_hive_comments_unicode(self, connection, metadata):
        table = Table(
            "unicode_comments",
            metadata,
            Column("data", Integer, comment="é試蛇ẟΩ✨"),
            comment="試蛇ẟΩ✨",
            awsathena_tblproperties={"classification": "parquet"},
            awsathena_file_format="PARQUET",
        )
        table.create(connection)
        inspector = inspect(connection)
        assert inspector.get_table_comment(table.name) == {"text": table.comment}
        assert inspector.get_columns(table.name)[0]["comment"] == table.c.data.comment

    @pytest.mark.skip("Iceberg strings do not preserve CHAR or VARCHAR length constraints.")
    def test_string_length_reflection(self, connection, metadata, type_):
        pass

    @pytest.mark.skip("Athena CREATE TABLE does not support NOT NULL constraints.")
    def test_nullable_reflection(self, connection, metadata):
        pass


@pytest.mark.skip("Athena table names do not support spaces or embedded quote characters.")
class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    pass


class LongNameBlowoutTest(_LongNameBlowoutTest):
    @sa_testing.combinations(
        ("fk", sa_testing.requires.foreign_key_ddl),
        ("pk", sa_testing.requires.primary_key_constraint_reflection),
        ("ix", sa_testing.requires.index_reflection),
        ("ck", sa_testing.requires.check_constraint_reflection),
        ("uq", sa_testing.requires.unique_constraint_reflection),
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        # Reuse the upstream fixtures without calling its parametrized wrapper.
        actual_name, reflected_name = getattr(self, type_)(metadata, connection)
        assert len(actual_name) > 255
        if reflected_name is not None:
            overlap = actual_name[: len(reflected_name)]
            if len(overlap) < len(actual_name):
                assert overlap[:-5] == reflected_name[:-5]
            else:
                assert overlap == reflected_name


class HasTableTest(_HasTableTest):
    @sa_testing.combinations((True, sa_testing.requires.schemas), False, argnames="use_schema")
    def test_has_table_cache_drop(self, connection, metadata, use_schema):
        schema = sa_testing.config.test_schema if use_schema else None
        table = Table("cache_drop", metadata, Column("id", Integer), schema=schema)
        table.create(connection)
        inspector = inspect(connection)
        assert inspector.has_table(table.name, schema=schema)

        table.drop(connection)
        assert inspector.has_table(table.name, schema=schema)
        assert not connection.dialect.has_table(connection, table.name, schema=schema)
        assert not inspect(connection).has_table(table.name, schema=schema)
        inspector.clear_cache()
        assert not inspector.has_table(table.name, schema=schema)

        table.create(connection)
        assert not inspector.has_table(table.name, schema=schema)
        assert connection.dialect.has_table(connection, table.name, schema=schema)
        assert inspect(connection).has_table(table.name, schema=schema)
        inspector.clear_cache()
        assert inspector.has_table(table.name, schema=schema)

    @sa_testing.requires.schemas
    @sa_testing.combinations(12, 129, argnames="length")
    def test_has_table_cache_schema(self, connection, metadata, length):
        table = Table("cache_schema".ljust(length, "t"), metadata, Column("id", Integer))
        other = Table(
            table.name,
            metadata,
            Column("id", Integer),
            schema=sa_testing.config.test_schema,
        )
        table.create(connection)
        inspector = inspect(connection)
        assert inspector.has_table(table.name)
        assert not inspector.has_table(other.name, schema=other.schema)
        other.create(connection)
        assert not inspector.has_table(other.name, schema=other.schema)
        inspector.clear_cache()
        assert inspector.has_table(other.name, schema=other.schema)


class IdentifierReflectionTest(fixtures.TestBase):
    @sa_testing.combinations("select", "_reflection", "quoted_lowercase", argnames="name")
    def test_quoted_identifier(self, connection, metadata, name):
        table = Table(
            name,
            metadata,
            Column("from", Integer, quote=True, comment="quoted column"),
            quote=True,
            comment="quoted table",
        )
        table.create(connection)
        connection.execute(table.insert().values({"from": 1}))
        assert connection.execute(select(table.c["from"])).scalar_one() == 1
        inspector = inspect(connection)
        assert inspector.has_table(table.name)
        columns = inspector.get_columns(table.name)
        assert [column["name"] for column in columns] == ["from"]
        assert columns[0]["comment"] == "quoted column"
        # Athena persists Iceberg column comments, but not table comments.
        assert inspector.get_table_comment(table.name) == {"text": None}
        assert inspector.get_table_options(table.name)["awsathena_location"]

    @sa_testing.combinations(128, 129, 255, argnames="length")
    def test_identifier_length(self, connection, metadata, caplog, length):
        table = Table("t" * length, metadata, Column("c" * 255, Integer))
        inspector = inspect(connection)
        assert not inspector.has_table(table.name)
        missing_schema = f"{sa_testing.config.test_schema}_missing"
        caplog.clear()
        assert not inspector.has_table(table.name, schema=missing_schema)
        with pytest.raises(sa_exc.NoSuchTableError):
            inspector.get_columns(table.name, schema=missing_schema)
        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
        table.create(connection)
        connection.execute(table.insert().values({"c" * 255: 1}))
        assert connection.execute(select(table)).scalar_one() == 1
        assert not inspector.has_table(table.name)
        inspector.clear_cache()
        assert inspector.has_table(table.name)
        assert inspector.get_columns(table.name)[0]["name"] == "c" * 255
        uppercase_name = quoted_name(str(table.name).upper(), quote=True)
        assert inspector.has_table(uppercase_name)
        assert inspector.get_columns(uppercase_name)[0]["name"] == "c" * 255
        table.drop(connection)
        assert inspector.has_table(table.name)
        inspector.clear_cache()
        assert not inspector.has_table(table.name)
        with pytest.raises(sa_exc.NoSuchTableError):
            inspector.get_columns(table.name)

        too_long = Table("t" * 256, metadata, Column("id", Integer))
        try:
            with pytest.raises(sa_exc.IdentifierError):
                too_long.create(connection)
        finally:
            # The rejected table must not be visited by teardown.
            metadata.remove(too_long)


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip("Athena does not support auto-incrementing.")
    def test_insert_from_select_autoinc(self, connection):
        pass

    @pytest.mark.skip("Athena does not support auto-incrementing.")
    def test_insert_from_select_autoinc_no_rows(self, connection):
        pass


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    @pytest.mark.skip("Athena does not support expressions in the offset clause.")
    def test_simple_limit_expr_offset(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the limit clause.")
    def test_expr_limit(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the limit clause.")
    def test_expr_limit_offset(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the limit clause.")
    def test_expr_limit_simple_offset(self, connection):
        pass

    @pytest.mark.skip("Athena does not support expressions in the offset clause.")
    def test_expr_offset(self, connection):
        pass

    @pytest.mark.skip("TODO")
    def test_limit_render_multiple_times(self, connection):
        # TODO
        pass


class IntegerTest(_IntegerTest):
    @pytest.mark.skip("TODO")
    def test_huge_int(self, integer_round_trip, intvalue):
        # TODO
        pass


class StringTest(_StringTest):
    @pytest.mark.skip("TODO")
    def test_dont_truncate_rightside(self, metadata, connection, expr, expected):
        # TODO
        pass
