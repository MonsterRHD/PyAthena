import json
import pickle
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    LABEL_STYLE_TABLENAME_PLUS_COL,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    cast,
    literal,
    select,
    text,
    types,
)
from sqlalchemy import exc as sa_exc
from sqlalchemy.sql import sqltypes

import pyathena
from pyathena.formatter import DefaultParameterFormatter
from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.types import (
    ARRAY,
    MAP,
    STRUCT,
    AthenaArray,
    AthenaDate,
    AthenaMap,
    AthenaStruct,
    AthenaTimestamp,
    get_double_type,
)


class TestAthenaStruct:
    def test_creation_with_strings(self):
        struct_type = AthenaStruct("name", "age")
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.String)

    def test_creation_with_tuples(self):
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)

    def test_creation_with_type_instances(self):
        struct_type = AthenaStruct(("name", String()), ("age", Integer()))
        assert "name" in struct_type.fields
        assert "age" in struct_type.fields
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)

    def test_field_access_by_key(self):
        struct_type = AthenaStruct(("name", String), ("age", Integer))
        name_field = struct_type["name"]
        assert isinstance(name_field, sqltypes.String)

    def test_python_type(self):
        struct_type = AthenaStruct(("name", String))
        assert struct_type.python_type is dict

    def test_invalid_field_specification(self):
        with pytest.raises(ValueError, match="Invalid field specification"):
            AthenaStruct(123)  # Invalid field type

    def test_visit_name(self):
        struct_type = AthenaStruct()
        assert struct_type.__visit_name__ == "struct"

    def test_struct_uppercase_visit_name(self):
        struct_type = STRUCT()
        assert struct_type.__visit_name__ == "STRUCT"

    def test_empty_struct(self):
        struct_type = AthenaStruct()
        assert len(struct_type.fields) == 0

    def test_mixed_field_definitions(self):
        struct_type = AthenaStruct("name", ("age", Integer), ("active", String()))
        assert len(struct_type.fields) == 3
        assert isinstance(struct_type.fields["name"], sqltypes.String)
        assert isinstance(struct_type.fields["age"], sqltypes.Integer)
        assert isinstance(struct_type.fields["active"], sqltypes.String)

    def test_field_access_nonexistent_key(self):
        struct_type = AthenaStruct(("name", String))
        with pytest.raises(KeyError):
            struct_type["nonexistent"]


class TestAthenaMap:
    def test_creation_with_defaults(self):
        map_type = AthenaMap()
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.String)

    def test_creation_with_type_classes(self):
        map_type = AthenaMap(String, Integer)
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.Integer)

    def test_creation_with_type_instances(self):
        map_type = AthenaMap(String(), Integer())
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.Integer)

    def test_python_type(self):
        map_type = AthenaMap()
        assert map_type.python_type is dict

    def test_visit_name(self):
        map_type = AthenaMap()
        assert map_type.__visit_name__ == "map"

    def test_map_uppercase_visit_name(self):
        map_type = MAP()
        assert map_type.__visit_name__ == "MAP"

    def test_mixed_type_definitions(self):
        map_type = AthenaMap(String, Integer())
        assert isinstance(map_type.key_type, sqltypes.String)
        assert isinstance(map_type.value_type, sqltypes.Integer)


class TestAthenaArray:
    def test_creation_with_default(self):
        array_type = AthenaArray()
        assert isinstance(array_type.item_type, sqltypes.String)

    def test_creation_with_type_class(self):
        array_type = AthenaArray(Integer)
        assert isinstance(array_type.item_type, sqltypes.Integer)

    def test_creation_with_type_instance(self):
        array_type = AthenaArray(Integer())
        assert isinstance(array_type.item_type, sqltypes.Integer)

    def test_creation_with_string_type(self):
        array_type = AthenaArray(String)
        assert isinstance(array_type.item_type, sqltypes.String)

    def test_python_type(self):
        array_type = AthenaArray()
        assert array_type.python_type is list

    def test_visit_name(self):
        array_type = AthenaArray()
        assert array_type.__visit_name__ == "array"

    def test_array_uppercase_visit_name(self):
        array_type = ARRAY()
        assert array_type.__visit_name__ == "ARRAY"

    def test_array_with_complex_type(self):
        array_type = AthenaArray(AthenaStruct(("name", String), ("age", Integer)))
        assert isinstance(array_type.item_type, AthenaStruct)
        assert "name" in array_type.item_type.fields
        assert "age" in array_type.item_type.fields

    def test_array_with_nested_array(self):
        array_type = AthenaArray(AthenaArray(Integer))
        assert isinstance(array_type.item_type, AthenaArray)
        assert isinstance(array_type.item_type.item_type, sqltypes.Integer)

    def test_array_with_map_type(self):
        array_type = AthenaArray(AthenaMap(String, Integer))
        assert isinstance(array_type.item_type, AthenaMap)
        assert isinstance(array_type.item_type.key_type, sqltypes.String)
        assert isinstance(array_type.item_type.value_type, sqltypes.Integer)


def test_get_double_type():
    from pyathena.sqlalchemy.base import ischema_names

    result = get_double_type()
    if hasattr(types, "DOUBLE"):
        assert result is types.DOUBLE
    else:
        assert result is types.FLOAT
    assert ischema_names["double"] is result


@pytest.mark.parametrize(
    ("type_", "ddl", "dml"),
    [
        (types.ARRAY(Integer), "ARRAY<INT>", "ARRAY(INTEGER)"),
        (AthenaArray(), "ARRAY<STRING>", "ARRAY(VARCHAR)"),
        (ARRAY(String(12)), "ARRAY<STRING>", "ARRAY(VARCHAR)"),
        (types.ARRAY(String, dimensions=2), "ARRAY<ARRAY<STRING>>", "ARRAY(ARRAY(VARCHAR))"),
        (AthenaArray(AthenaArray(Integer)), "ARRAY<ARRAY<INT>>", "ARRAY(ARRAY(INTEGER))"),
        (AthenaArray(types.Numeric(12, 3)), "ARRAY<DECIMAL(12, 3)>", "ARRAY(DECIMAL(12, 3))"),
        (AthenaArray(types.Float), "ARRAY<FLOAT>", "ARRAY(REAL)"),
        (AthenaArray(types.BINARY), "ARRAY<BINARY>", "ARRAY(VARBINARY)"),
    ],
)
def test_array_type_rendering(type_, ddl, dml):
    dialect = AthenaDialect()
    assert type_.compile(dialect=dialect) == ddl
    assert str(cast(literal(None), type_).compile(dialect=dialect)).endswith(f"AS {dml})")
    assert isinstance(type_.dialect_impl(dialect), types.ARRAY)


@pytest.mark.parametrize(
    ("signature", "expected"),
    [
        ("array<integer>", AthenaArray(types.INTEGER)),
        ("ARRAY(ARRAY(VARCHAR(32)))", AthenaArray(AthenaArray(types.VARCHAR(32)))),
        ("array<decimal(18,7)>", AthenaArray(types.DECIMAL(18, 7))),
        (
            "array<map<string,array<int>>>",
            AthenaArray(AthenaMap(String, AthenaArray(types.INTEGER))),
        ),
        (
            'array<struct<"a,b":decimal(10,2),`c:d`:varchar(17)>>',
            AthenaArray(AthenaStruct(("a,b", types.DECIMAL(10, 2)), ("c:d", types.VARCHAR(17)))),
        ),
    ],
)
def test_array_reflection_preserves_element_types(signature, expected):
    actual = AthenaDialect()._get_column_type(signature)
    assert isinstance(actual, types.ARRAY)
    assert actual._static_cache_key == expected._static_cache_key


@pytest.mark.parametrize("dimensions", [0, -1, True, 1.5])
def test_array_rejects_invalid_dimensions(dimensions):
    with pytest.raises(ValueError, match="positive integer"):
        AthenaArray(Integer, dimensions=dimensions)


def test_array_rejects_ambiguous_dimensions():
    with pytest.raises(ValueError, match="either nested ARRAY types or dimensions"):
        AthenaArray(AthenaArray(Integer), dimensions=2)


@pytest.mark.parametrize(
    ("type_", "value", "expected"),
    [
        (AthenaArray(Integer), [1, None, 3], "ARRAY[1, NULL, 3]"),
        (
            AthenaArray(String),
            ["thr'ee", "réve🐍 illé", "a,b", "null"],
            "ARRAY['thr''ee', 'réve🐍 illé', 'a,b', 'null']",
        ),
        (
            AthenaArray(String, dimensions=2),
            [["one"], [], None],
            "ARRAY[ARRAY['one'], ARRAY[], NULL]",
        ),
        (AthenaArray(types.Date), [date(2025, 1, 2)], "ARRAY[DATE '2025-01-02']"),
        (AthenaArray(types.BINARY), [b"\x00\xff"], "ARRAY[X'00ff']"),
        (AthenaArray(Integer), [], "ARRAY[]"),
        (AthenaArray(Integer), None, "NULL"),
    ],
)
def test_array_bound_and_literal_values(type_, value, expected):
    dialect = AthenaDialect()
    assert type_.literal_processor(dialect)(value) == expected
    bound = type_.bind_processor(dialect)(value)
    actual = DefaultParameterFormatter().format("SELECT %(value)s", {"value": bound})
    assert actual == "SELECT " + expected.replace("NULL", "null")


def test_array_binding_preserves_in_parameters():
    formatter = DefaultParameterFormatter()
    assert (
        formatter.format("SELECT 1 WHERE 1 IN %(items)s", {"items": [1, 2]})
        == "SELECT 1 WHERE 1 IN (1, 2)"
    )


def test_array_binary_binding_uses_native_literals():
    dialect = AthenaDialect(dbapi=pyathena)
    processor = AthenaArray(types.BINARY).bind_processor(dialect)
    assert (
        DefaultParameterFormatter().format("SELECT %(value)s", {"value": processor([b"\xff"])})
        == "SELECT ARRAY[X'ff']"
    )


@pytest.mark.parametrize("value", [[[1]], "[1]", {"x": 1}])
def test_array_binding_rejects_incorrect_shape(value):
    with pytest.raises(TypeError, match="ARRAY"):
        AthenaArray(Integer).bind_processor(AthenaDialect())(value)


def test_array_textual_sql_preserves_native_fallback():
    value = "[[one, two], [a,b]]"
    assert AthenaArray(String, dimensions=2).result_processor(AthenaDialect(), None)(value) == value


def test_array_insert_uses_typed_parameter():
    formatter = DefaultParameterFormatter()
    table = Table("array_values", MetaData(), Column("items", types.ARRAY(Integer)))
    compiled = table.insert().values(items=[1, 2]).compile(dialect=AthenaDialect())
    params = {
        name: compiled._bind_processors[name](value) for name, value in compiled.params.items()
    }
    assert (
        formatter.format(str(compiled), params)
        == "INSERT INTO array_values (items) VALUES (CAST(ARRAY[1, 2] AS ARRAY(INTEGER)))"
    )


@pytest.mark.parametrize(
    ("type_", "encoded", "expected"),
    [
        (AthenaArray(Integer), '["1",null,"3"]', [1, None, 3]),
        (AthenaArray(String), '["001","null","a,b",""]', ["001", "null", "a,b", ""]),
        (AthenaArray(Integer, dimensions=2, as_tuple=True), '[["1"],[],null]', ((1,), (), None)),
        (
            AthenaArray(types.Numeric(30, 20)),
            '["0.12345678901234567890"]',
            [Decimal("0.12345678901234567890")],
        ),
        (AthenaArray(types.Date), '["2025-01-02"]', [date(2025, 1, 2)]),
        (AthenaArray(types.BINARY), '["00FF",""]', [b"\x00\xff", b""]),
        (AthenaArray(types.JSON), '[{"fraction":0.1}]', [{"fraction": 0.1}]),
        (
            AthenaArray(AthenaMap(Integer, String)),
            '[[["1","001"],["2",null]]]',
            [{1: "001", 2: None}],
        ),
        (
            AthenaArray(AthenaStruct(("name", String), ("n", Integer))),
            '[{"name":"001","n":"2"},null]',
            [{"name": "001", "n": 2}, None],
        ),
    ],
)
def test_array_result_conversion(type_, encoded, expected):
    processor = type_.result_processor(AthenaDialect(), None)
    assert processor(encoded) == expected
    assert processor(json.loads(encoded)) == expected
    assert processor(json.dumps({"_pyathena_array": json.loads(encoded)})) == expected
    assert processor('{"_pyathena_array":null}') is None
    assert processor(None) is None


def test_array_result_projection_does_not_change_subquery_type():
    table = Table("array_values", MetaData(), Column("items", AthenaArray(Integer)))
    subquery = select(table.c["items"]).subquery()
    compiled = str(select(subquery.c["items"]).compile(dialect=AthenaDialect()))
    assert compiled.count("json_format(") == 1
    assert "SELECT array_values.items AS items" in compiled
    assert isinstance(subquery.c["items"].type, types.ARRAY)


def test_array_cache_key_includes_nested_fields():
    first = AthenaArray(AthenaStruct(("x", Integer)))
    second = AthenaArray(AthenaStruct(("x", String)))
    assert first._static_cache_key != second._static_cache_key
    assert hash(first._static_cache_key)


def test_array_distinct_and_union_keep_native_ordering():
    table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
    for statement in (
        select(table.c["items"]).distinct().order_by("items"),
        select(table.c["items"]).union_all(select(table.c["items"])).order_by("items"),
    ):
        sql = str(statement.compile(dialect=AthenaDialect()))
        assert sql.count("json_format(") == 1
        assert "ORDER BY anon_1.items" in sql


class TestAthenaDate:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (date(2017, 1, 1), "DATE '2017-01-01'"),
            (datetime(2017, 1, 1, 12, 34, 56), "DATE '2017-01-01'"),
        ],
    )
    def test_process_renders_date_only_literal(self, value, expected):
        assert AthenaDate.process(value) == expected

    def test_process_falls_back_to_str(self):
        assert AthenaDate.process("2017-01-01") == "DATE '2017-01-01'"


class TestAthenaTimestamp:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # Athena TIMESTAMP has millisecond precision, so the six digits
            # strftime("%f") emits are truncated to three.
            (
                datetime(2017, 1, 1, 12, 34, 56, 789012),
                "TIMESTAMP '2017-01-01 12:34:56.789'",
            ),
            (
                datetime(2017, 1, 1, 12, 34, 56),
                "TIMESTAMP '2017-01-01 12:34:56.000'",
            ),
        ],
    )
    def test_process_renders_millisecond_precision_literal(self, value, expected):
        assert AthenaTimestamp.process(value) == expected

    def test_process_falls_back_to_str(self):
        assert (
            AthenaTimestamp.process("2017-01-01 12:34:56.789")
            == "TIMESTAMP '2017-01-01 12:34:56.789'"
        )


class Color(Enum):
    RED = "red"


class OffsetInteger(types.TypeDecorator):
    impl = Integer
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return value - 1 if value is not None else None

    def process_result_value(self, value, dialect):
        return value + 1 if value is not None else None


@pytest.mark.parametrize("item_type", [types.Double, types.DOUBLE, types.DOUBLE_PRECISION])
def test_array_double_precision_cast(item_type):
    sql = str(cast(literal(None), AthenaArray(item_type)).compile(dialect=AthenaDialect()))
    assert "AS ARRAY(DOUBLE)" in sql


def test_array_custom_element_processors():
    dialect = AthenaDialect()
    enum = AthenaArray(types.Enum(Color))
    assert enum.result_processor(dialect, None)('["RED"]') == [Color.RED]
    decorated = AthenaArray(OffsetInteger())
    assert decorated.result_processor(dialect, None)('["4"]') == [5]
    sql = str(select(literal([5], decorated)).compile(dialect=dialect))
    assert "ARRAY(INTEGER)" in sql


def test_untyped_array_preserves_json_scalar_types():
    dialect = AthenaDialect()
    untyped = AthenaArray(types.NullType())
    sql = str(select(Column("items", untyped)).compile(dialect=dialect))
    assert "json_format" not in sql
    assert untyped.result_processor(dialect, None)('[1,{"x":2},[3]]') == [1, {"x": 2}, [3]]
    with pytest.raises(sa_exc.CompileError, match="explicit element type"):
        select(literal([1], untyped)).compile(dialect=dialect)


def test_array_textual_ordering_and_hive_field_spaces():
    table = Table(
        "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
    )
    sql = str(select(table).order_by(text("id DESC")).compile(dialect=AthenaDialect()))
    assert "ORDER BY anon_1.id DESC" in sql
    reflected = AthenaDialect()._get_column_type("array<struct<first name:string>>")
    assert list(reflected.item_type.fields) == ["first name"]
    assert isinstance(reflected.item_type.fields["first name"], String)


class DecoratedTimestamp(types.TypeDecorator):
    impl = types.TIMESTAMP
    cache_ok = True

    def process_result_value(self, value, dialect):
        assert value is None or isinstance(value, datetime)
        return value


class JSONEncodedDict(types.TypeDecorator):
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        return json.loads(value)


@pytest.mark.parametrize(
    ("item_type", "value"),
    [
        (DecoratedTimestamp(), datetime(2025, 1, 2, 3, 4, 5)),
        (JSONEncodedDict(), {"x": 1}),
        (OffsetInteger().with_variant(String(), "awsathena"), "unchanged"),
    ],
)
def test_decorator_bind_literal_and_result_paths(item_type, value):
    dialect = AthenaDialect()
    array = AthenaArray(item_type)
    bind = array.bind_processor(dialect)([value])
    rendered = DefaultParameterFormatter().format("SELECT %(value)s", {"value": bind})
    assert "ARRAY[" in rendered
    literal_sql = array.literal_processor(dialect)([value])
    assert "ARRAY[" in literal_sql
    select(literal([value], array)).compile(dialect=dialect)
    encoded = (
        value.isoformat(" ")
        if isinstance(value, datetime)
        else (json.dumps(value) if isinstance(value, dict) else value)
    )
    assert array.result_processor(dialect, None)(json.dumps([encoded])) == [value]


def test_textual_ordering_list_and_unresolved_expression():
    table = Table(
        "arrays", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
    )
    sql = str(select(table).order_by(text("items DESC, id")).compile(dialect=AthenaDialect()))
    assert "ORDER BY anon_1.items DESC, anon_1.id" in sql
    with pytest.raises(sa_exc.CompileError, match="column expressions"):
        select(table).order_by(text("cardinality(items)")).compile(dialect=AthenaDialect())


def test_array_ordering_ordinals_use_selected_positions():
    first = Table(
        "first_table", MetaData(), Column("id", Integer), Column("items", AthenaArray(Integer))
    )
    second = Table("second_table", MetaData(), Column("id", Integer))
    sql = str(
        select(first.c.id, second.c.id, first.c["items"])
        .order_by(text("2"))
        .compile(dialect=AthenaDialect())
    )
    assert "ORDER BY anon_1.id_1" in sql
    sql = str(
        select(first)
        .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        .order_by(text("1"))
        .compile(dialect=AthenaDialect())
    )
    assert "ORDER BY anon_1.first_table_id" in sql
    numeric = Table("numeric", MetaData(), Column("id", Integer), Column("1", AthenaArray(Integer)))
    sql = str(select(numeric).order_by(text('"1"')).compile(dialect=AthenaDialect()))
    assert 'ORDER BY anon_1."1"' in sql


@pytest.mark.parametrize(
    "ordering", ["id > 5", "coalesce(name, ')')", "CASE WHEN id > 5 THEN 0 ELSE 1 END"]
)
def test_unsupported_array_text_ordering_raises_compile_error(ordering):
    table = Table("arrays", MetaData(), Column("items", AthenaArray(Integer)))
    with pytest.raises(sa_exc.CompileError, match="column expressions"):
        select(table).order_by(text(ordering)).compile(dialect=AthenaDialect())


def test_unknown_array_does_not_rewrite_ordering():
    table = Table("arrays", MetaData(), Column("items", AthenaArray(types.NullType())))
    sql = str(select(table).order_by(text("lower(name)")).compile(dialect=AthenaDialect()))
    assert "ORDER BY lower(name)" in sql
    assert "anon_1" not in sql


def test_array_pickle_type_uses_overridden_processors():
    dialect = AthenaDialect(dbapi=SimpleNamespace(Binary=bytes, paramstyle="pyformat"))
    array = AthenaArray(types.PickleType())
    bound = array.bind_processor(dialect)([5])
    assert pickle.loads(bound.values[0]) == 5
    assert array.result_processor(dialect, None)(json.dumps([bound.values[0].hex()])) == [5]
