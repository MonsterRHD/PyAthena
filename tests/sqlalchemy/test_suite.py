import json as _json
import logging
from datetime import date
from datetime import datetime as _datetime
from decimal import Decimal

import pytest
from sqlalchemy import (
    CHAR,
    VARCHAR,
    Integer,
    MetaData,
    String,
    all_,
    any_,
    bindparam,
    func,
    inspect,
    literal,
    select,
    text,
    types,
)
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

from pyathena.sqlalchemy.types import AthenaArray, AthenaMap, AthenaStruct

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


class _ArrayTimestamp(types.TypeDecorator):
    impl = types.TIMESTAMP
    cache_ok = True

    def process_result_value(self, value, dialect):
        assert value is None or isinstance(value, _datetime)
        return value


class _ArrayJSONText(types.TypeDecorator):
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return _json.dumps(value)

    def process_result_value(self, value, dialect):
        return _json.loads(value)


class ArrayExpressionTest(fixtures.TestBase):
    __backend__ = True
    __requires__ = ("array_type",)

    def test_cached_steps_and_boolean_quantifiers(self, connection):
        array = literal([1, 2, 3], types.ARRAY(Integer))
        eq_(connection.execute(select(array[1:2:1])).scalar_one(), [1, 2])
        with pytest.raises(sa_exc.StatementError):
            connection.execute(select(array[1:2:2])).all()
        flags = literal([True, False], AthenaArray(types.Boolean))
        eq_(
            tuple(connection.execute(select(any_(flags) == True, all_(flags) == True)).one()),  # noqa: E712
            (True, False),
        )

    def test_index_slice_and_concat(self, connection):
        array = literal([1, None, 3], AthenaArray(Integer))
        empty = literal([], AthenaArray(Integer))
        missing = literal(None, AthenaArray(Integer))
        expressions = [
            array[1],
            array[2],
            array[0],
            array[-1],
            array[10],
            array[:],
            array[:2],
            array[2:],
            array[-2:100],
            array[3:1],
            empty[1:3],
            missing[1:3],
            array.concat([4]),
            array[1:2].concat(array[3:]),
        ]
        eq_(
            tuple(connection.execute(select(*expressions)).one()),
            (
                1,
                None,
                None,
                None,
                None,
                [1, None, 3],
                [1, None],
                [None, 3],
                [1, None, 3],
                [],
                [],
                None,
                [1, None, 3, 4],
                [1, None, 3],
            ),
        )

    def test_dimensions_zero_indexes_and_bound_index(self, connection):
        array = literal([[1, 2], [3]], AthenaArray(Integer, dimensions=2, zero_indexes=True))
        statement = select(array[bindparam("index")], array[:0], array[0][1], array[1:])
        eq_(tuple(connection.execute(statement, {"index": 0}).one()), ([1, 2], [[1, 2]], 2, [[3]]))
        eq_(tuple(connection.execute(statement, {"index": 1}).one()), ([3], [[1, 2]], 2, [[3]]))
        eq_(connection.execute(select(array.any([1, 2]))).scalar_one(), True)
        plain = literal([1, 2], types.ARRAY(Integer))
        eq_(connection.execute(select(plain[bindparam("index")]), {"index": 2}).scalar_one(), 2)

    def test_quantified_comparisons(self, connection):
        cases = [
            ([1, 2, None], True, False, False, False),
            ([1, 3], False, False, False, True),
            ([], False, True, True, True),
            (None, None, None, None, None),
        ]
        for values, eq_any, eq_all, lt_all, neg_any in cases:
            array = literal(values, AthenaArray(Integer))
            row = connection.execute(
                select(
                    any_(array) == 2,
                    all_(array) == 2,
                    all_(array) > 2,
                    ~array.any(2),
                    any_(array) == None,  # noqa: E711
                )
            ).one()
            eq_(
                tuple(row),
                (
                    eq_any,
                    eq_all,
                    lt_all,
                    neg_any if eq_any is not None else None,
                    False if values == [] else None,
                ),
            )

    def test_where_and_lambda_names(self, connection, metadata):
        table = Table(
            "array_expressions",
            metadata,
            Column("id", Integer),
            Column("items", AthenaArray(Integer)),
            Column("_pyathena_element_0", Integer),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [
                {"id": 1, "items": [1, 3], "_pyathena_element_0": 3},
                {"id": 2, "items": [2, 4], "_pyathena_element_0": 1},
            ],
        )
        predicate = (table.c._pyathena_element_0 == any_(table.c["items"])) & (
            table.c["items"][1] == 1
        )
        eq_(connection.execute(select(table.c.id).where(predicate)).scalars().all(), [1])


class NativeArrayTest(fixtures.TestBase):
    __backend__ = True
    __requires__ = ("array_type",)

    def test_native_ordering(self, connection, metadata):
        table = Table(
            "native_array_order",
            metadata,
            Column("id", Integer),
            Column("value", AthenaArray(Integer)),
        )
        table.create(connection)
        connection.execute(
            table.insert(),
            [{"id": 1, "value": [10]}, {"id": 2, "value": [2]}, {"id": 3, "value": [2]}],
        )
        value = table.c.value.label("items")
        for ordering in (value, "items", text("items")):
            stmt = select(value).distinct().order_by(ordering)
            eq_(connection.execute(stmt).scalars().all(), [[2], [10]])
        eq_(
            connection.execute(select(value).order_by(table.c.id.desc()).limit(2)).scalars().all(),
            [[2], [2]],
        )
        union = (
            select(table.c.value)
            .where(table.c.id == 1)
            .union_all(select(table.c.value).where(table.c.id == 2))
            .order_by("value")
        )
        eq_(connection.execute(union).scalars().all(), [[2], [10]])
        eq_(
            connection.execute(select(value, table.c.id).order_by(text("items DESC, id"))).all(),
            [([10], 1), ([2], 2), ([2], 3)],
        )

    def test_review_regressions(self, connection):
        expressions = [
            literal(["a-very-long-string"], AthenaArray(String(3))),
            literal([0.1], AthenaArray(types.Double)),
            literal([0.1], AthenaArray(types.DOUBLE_PRECISION)),
            func.array_agg(func.length(literal("abc"))),
        ]
        # Aggregate and scalar expressions are checked separately for Athena grouping rules.
        eq_(
            tuple(connection.execute(select(*expressions[:3])).one()),
            (["a-very-long-string"], [0.1], [0.1]),
        )
        eq_(connection.execute(select(expressions[3])).scalar_one(), [3])
        custom_values = [
            (AthenaArray(_ArrayTimestamp()), [_datetime(2025, 1, 2, 3, 4, 5)]),
            (AthenaArray(_ArrayJSONText()), [{"nested": [1, 2], "fraction": 0.1}]),
        ]
        for type_, value in custom_values:
            for literal_execute in (False, True):
                eq_(
                    connection.execute(
                        select(literal(value, type_, literal_execute=literal_execute))
                    ).scalar_one(),
                    value,
                )

    def test_reflection_and_executemany(self, connection, metadata):
        table = Table(
            "native_array_values",
            metadata,
            Column("id", Integer),
            Column("numbers", types.ARRAY(Integer)),
            Column("labels", types.ARRAY(String, dimensions=2)),
            Column("amounts", AthenaArray(types.Numeric(30, 20))),
        )
        table.create(connection)
        values = [
            {
                "id": 1,
                "numbers": [1, None, 3],
                "labels": [["001", "null", "a,b", ""], ["thr'ee", "réve🐍 illé"]],
                "amounts": [Decimal("0.12345678901234567890")],
            },
            {"id": 2, "numbers": [], "labels": [[], None], "amounts": []},
            {"id": 3, "numbers": None, "labels": None, "amounts": None},
        ]
        connection.execute(table.insert(), values)
        reflected = Table(table.name, MetaData(), autoload_with=connection)
        assert isinstance(reflected.c.numbers.type, types.ARRAY)
        assert isinstance(reflected.c.numbers.type.item_type, types.Integer)
        assert isinstance(reflected.c.labels.type.item_type, types.ARRAY)
        assert reflected.c.amounts.type.item_type.precision == 30
        assert reflected.c.amounts.type.item_type.scale == 20
        for source in (table, reflected):
            rows = connection.execute(select(source).order_by(source.c.id)).mappings().all()
            eq_([dict(row) for row in rows], values)
        result = connection.execute(select(table.c.labels).where(table.c.id == 1))
        cursor = getattr(result.context.cursor, "_cursor", result.context.cursor)
        assert cursor.effective_engine_version == "Athena engine version 3"

    @sa_testing.combinations(False, True, argnames="literal_execute")
    def test_typed_scalar_and_complex_elements(self, connection, literal_execute):
        cases = [
            (AthenaArray(String), ["100%", "%(param_1)s", "back\\slash", "line\nbreak", "null"]),
            (AthenaArray(types.Boolean), [True, False, None]),
            (AthenaArray(types.Date), [date(2025, 1, 2), None]),
            (AthenaArray(types.DateTime), [_datetime(2025, 1, 2, 3, 4, 5, 123000)]),
            (AthenaArray(types.BINARY), [b"\x00\xff", b"", None]),
            (AthenaArray(AthenaMap(Integer, String)), [{1: "001", 2: "a,b"}, {}, None]),
            (
                AthenaArray(AthenaStruct(("name", String), ("n", Integer))),
                [{"name": "a,b", "n": 2}, None],
            ),
            (AthenaArray(Integer, as_tuple=True), (1, None, 3)),
            (AthenaArray(types.JSON), [{"n": 1, "s": "a,b", "fraction": 0.1}, [1, None], None]),
        ]
        expressions = [
            literal(value, type_=type_, literal_execute=literal_execute).label(f"v{index}")
            for index, (type_, value) in enumerate(cases)
        ]
        row = connection.execute(select(*expressions)).one()
        eq_(tuple(row), tuple(value for _, value in cases))

    def test_nested_complex_reflection(self, connection, metadata):
        table = Table(
            "native_array_complex",
            metadata,
            Column(
                "value",
                AthenaArray(
                    AthenaStruct(
                        ("label", String),
                        ("numbers", AthenaArray(Integer)),
                        ("amount", types.Numeric(12, 3)),
                    )
                ),
            ),
        )
        table.create(connection)
        value = [{"label": "001,a", "numbers": [1, None], "amount": Decimal("12.340")}]
        connection.execute(table.insert().values(value=value))
        reflected = Table(table.name, MetaData(), autoload_with=connection)
        item = reflected.c.value.type.item_type
        assert isinstance(item, AthenaStruct)
        assert isinstance(item.fields["numbers"], types.ARRAY)
        assert item.fields["amount"].scale == 3
        eq_(connection.execute(select(reflected.c.value)).scalar_one(), value)


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
