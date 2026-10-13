"""Table join behavior shared with the pinned Hail implementation."""

import json
import struct

import pytest
import vail as hl


def keyed(rows, *, key="k", schema=None, n_partitions=2, globals=None):
    table = hl.Table.parallelize(rows, schema=schema, key=key, n_partitions=n_partitions)
    if globals is not None:
        table = table.annotate_globals(**globals)
    return table


def test_join_modes_cartesian_duplicate_keys_missing_and_lazy_plan():
    schema = hl.tstruct(k=hl.tint32, left=hl.tint32)
    right_schema = hl.tstruct(rk=hl.tint32, right=hl.tint32)
    left = keyed(
        [{"k": 1, "left": 10}, {"k": 1, "left": 11}, {"k": None, "left": 12}],
        schema=schema,
    )
    right = keyed(
        [{"rk": 1, "right": 20}, {"rk": 1, "right": 21}, {"rk": None, "right": 22}],
        key="rk",
        schema=right_schema,
    )

    expected_matches = [
        (10, 20), (10, 21), (11, 20), (11, 21),
    ]
    inner = left.join(right, "inner")
    assert json.loads(inner._native.ir_json())["node"] == "TableJoin"
    assert "JoinExec" in inner._native.physical_plan()
    assert [(r.left, r.right) for r in inner.collect()] == expected_matches
    assert [(r.k, r.left, r.right) for r in left.join(right, "left").collect()] == [
        (1, 10, 20), (1, 10, 21), (1, 11, 20), (1, 11, 21), (None, 12, None)
    ]
    assert [(r.k, r.left, r.right) for r in left.join(right, "right").collect()] == [
        (1, 10, 20), (1, 10, 21), (1, 11, 20), (1, 11, 21), (None, None, 22)
    ]
    assert [(r.k, r.left, r.right) for r in left.join(right, "outer").collect()] == [
        (1, 10, 20), (1, 10, 21), (1, 11, 20), (1, 11, 21),
        (None, 12, None), (None, None, 22),
    ]


def test_join_empty_inputs_zero_key_cross_product_and_prefix_suffix_keys():
    empty = hl.Table.parallelize([], schema=hl.tstruct(k=hl.tint32, value=hl.tstr), key="k")
    populated = keyed([{"k": 1, "value": "x"}], schema=hl.tstruct(k=hl.tint32, value=hl.tstr))
    assert empty.join(populated, "inner").collect() == []
    assert len(empty.join(populated, "right").collect()) == 1
    assert empty.join(populated, "left").collect() == []

    unkeyed_left = hl.Table.parallelize([{"a": 1}, {"a": 2}], schema=hl.tstruct(a=hl.tint32))
    unkeyed_right = hl.Table.parallelize([{"b": 3}, {"b": 4}, {"b": 5}], schema=hl.tstruct(b=hl.tint32))
    assert [(r.a, r.b) for r in unkeyed_left.join(unkeyed_right, _join_key=0).collect()] == [
        (1, 3), (1, 4), (1, 5), (2, 3), (2, 4), (2, 5)
    ]

    left = hl.Table.parallelize(
        [{"a": 1, "b": "left", "v": 10}],
        schema=hl.tstruct(a=hl.tint32, b=hl.tstr, v=hl.tint32), key=["a", "b"],
    )
    right = hl.Table.parallelize(
        [{"x": 1, "y": "right", "w": 20}],
        schema=hl.tstruct(x=hl.tint32, y=hl.tstr, w=hl.tint32), key=["x", "y"],
    )
    prefix = left.join(right, _join_key=1)
    assert list(prefix.row) == ["a", "b", "v", "y", "w"]
    assert list(prefix.key) == ["a", "b", "y"]
    row = prefix.collect()[0]
    assert (row.a, row.b, row.v, row.y, row.w) == (1, "left", 10, "right", 20)


def test_join_mangles_right_row_and_global_names_and_keeps_left_key():
    left = keyed(
        [{"k": 1, "same": "L"}], schema=hl.tstruct(k=hl.tint32, same=hl.tstr),
        globals={"g": "left", "shared": 1},
    )
    right = keyed(
        [{"rk": 1, "same": "R"}], key="rk",
        schema=hl.tstruct(rk=hl.tint32, same=hl.tstr),
        globals={"g": "right", "shared": 2},
    )
    joined = left.join(right)
    assert list(joined.row) == ["k", "same", "same_1"]
    assert list(joined.key) == ["k"]
    assert list(joined.globals) == ["g", "shared", "g_1", "shared_1"]
    row = joined.collect()[0]
    assert (row.k, row.same, row.same_1) == (1, "L", "R")
    assert hl.eval(joined.globals) == hl.Struct(g="left", shared=1, g_1="right", shared_1=2)


def test_join_user_fields_can_use_internal_sort_field_names():
    left = keyed(
        [{"k": 1, "__vail_join_left_ordinal": "left payload"}],
        schema=hl.tstruct(k=hl.tint32, __vail_join_left_ordinal=hl.tstr),
    )
    right = keyed(
        [{"k": 1, "__vail_join_right_ordinal": "right payload"}],
        schema=hl.tstruct(k=hl.tint32, __vail_join_right_ordinal=hl.tstr),
    )
    row = left.join(right).collect()[0]
    assert row.__vail_join_left_ordinal == "left payload"
    assert row.__vail_join_right_ordinal == "right payload"


def test_join_name_mangling_reserves_collisions_with_unprocessed_right_fields():
    schema = hl.tstruct(k=hl.tint32, v=hl.tstr, v_1=hl.tstr)
    left = keyed([{"k": 1, "v": "L", "v_1": "L1"}], schema=schema)
    right = keyed([{"k": 1, "v": "R", "v_1": "R1"}], schema=schema)
    joined = left.join(right)
    assert list(joined.row) == ["k", "v", "v_1", "v_2", "v_1_1"]
    row = joined.collect()[0]
    assert (row.v, row.v_1, row.v_2, row.v_1_1) == ("L", "L1", "R", "R1")


def test_join_prefix_and_semi_anti_join_prefix():
    left = hl.Table.parallelize(
        [
            {"a": 1, "b": "x", "v": 10},
            {"a": 1, "b": "y", "v": 11},
            {"a": 2, "b": "z", "v": 12},
        ],
        schema=hl.tstruct(a=hl.tint32, b=hl.tstr, v=hl.tint32), key=["a", "b"],
        n_partitions=3,
    )
    right = hl.Table.parallelize(
        [{"x": 1, "w": 20}, {"x": 1, "w": 21}, {"x": 3, "w": 22}],
        schema=hl.tstruct(x=hl.tint32, w=hl.tint32), key="x",
    )
    joined = left.join(right, _join_key=1)
    assert [(r.a, r.b, r.v, r.w) for r in joined.collect()] == [
        (1, "x", 10, 20), (1, "x", 10, 21),
        (1, "y", 11, 20), (1, "y", 11, 21),
    ]
    semi = left.semi_join(right)
    anti = left.anti_join(right)
    assert [(r.a, r.b, r.v) for r in semi.collect()] == [(1, "x", 10), (1, "y", 11)]
    assert [(r.a, r.b, r.v) for r in anti.collect()] == [(2, "z", 12)]


def test_semi_anti_join_validate_key_prefix_types_and_retain_duplicate_rows():
    left = hl.Table.parallelize(
        [{"a": 1, "b": "x", "v": 1}, {"a": 1, "b": "x", "v": 2}, {"a": 2, "b": "y", "v": 3}],
        schema=hl.tstruct(a=hl.tint32, b=hl.tstr, v=hl.tint32), key=["a", "b"],
    )
    right = keyed([{"k": 1}], schema=hl.tstruct(k=hl.tint32))
    assert [r.v for r in left.semi_join(right).collect()] == [1, 2]
    assert [r.v for r in left.anti_join(right).collect()] == [3]
    nullable = hl.Table.parallelize(
        [{"a": None, "v": 4}, {"a": 1, "v": 5}],
        schema=hl.tstruct(a=hl.tint32, v=hl.tint32), key="a",
    )
    missing_only = keyed([{"k": None}], schema=hl.tstruct(k=hl.tint32))
    assert [r.v for r in nullable.semi_join(missing_only).collect()] == []
    assert [r.v for r in nullable.anti_join(missing_only).collect()] == [5, 4]
    with pytest.raises(ValueError, match="cannot join"):
        left.semi_join(keyed([{"k": "1"}], schema=hl.tstruct(k=hl.tstr)))
    unkeyed = hl.Table.parallelize([{"k": 1}], schema=hl.tstruct(k=hl.tint32))
    with pytest.raises(ValueError, match="no key"):
        left.anti_join(unkeyed)
    too_long = keyed([{"x": 1, "y": "x"}], key=["x", "y"])
    with pytest.raises(ValueError, match="cannot join"):
        right.semi_join(too_long)


def test_semi_anti_joins_keep_left_partitions_globals_and_followup_filters():
    left = hl.range_table(12, n_partitions=4).annotate_globals(label="left")
    right = hl.range_table(12, n_partitions=2)
    right = right.filter(right.idx % 2 == 0)
    semi = left.semi_join(right)
    anti = left.anti_join(right)
    assert semi.n_partitions() == left.n_partitions()
    assert anti.n_partitions() == left.n_partitions()
    assert hl.eval(semi.label) == "left"
    assert hl.eval(anti.label) == "left"
    assert [row.idx for row in semi.filter(semi.idx >= 4).collect()] == [4, 6, 8, 10]
    assert [row.idx for row in anti.filter(anti.idx < 4).collect()] == [1, 3]


def test_join_nested_and_float_keys_use_native_datafusion_equality():
    nested_schema = hl.tstruct(k=hl.tstruct(x=hl.tint32), v=hl.tint32)
    left_nested = keyed([{"k": {"x": 1}, "v": 10}], schema=nested_schema)
    right_nested = keyed([{"k": {"x": 1}, "v": 20}], schema=nested_schema)
    result = left_nested.join(right_nested)
    assert result.count() == 1
    assert result.collect()[0].v == 10
    assert result.collect()[0].v_1 == 20

    def float64(bits):
        return struct.unpack(">d", struct.pack(">Q", bits))[0]

    float_schema = hl.tstruct(k=hl.tfloat64, v=hl.tint32)
    left = keyed(
        [{"k": float64(0x7FF8000000000001), "v": 1},
         {"k": float64(0x7FF8000000000002), "v": 2}, {"k": -0.0, "v": 3}],
        schema=float_schema,
    )
    right = keyed(
        [{"k": float64(0x7FF8000000000001), "v": 4},
         {"k": float64(0x7FF8000000000003), "v": 5}, {"k": 0.0, "v": 6}],
        schema=float_schema,
    )
    rows = left.join(right).collect()
    assert [(r.v, r.v_1) for r in rows] == [(3, 6), (1, 4)]

    float32_schema = hl.tstruct(k=hl.tfloat32, v=hl.tint32)
    left32 = keyed([{"k": 1.5, "v": 7}], schema=float32_schema)
    right32 = keyed([{"k": 1.5, "v": 8}], schema=float32_schema)
    assert [(r.v, r.v_1) for r in left32.join(right32).collect()] == [(7, 8)]


def test_multi_way_zip_join_values_globals_and_schema_validation():
    row_schema = hl.tstruct(k=hl.tint32, nested=hl.tstruct(value=hl.tstr))
    left = keyed(
        [{"k": 0, "nested": {"value": "left"}}, {"k": 2, "nested": {"value": "both"}}],
        schema=row_schema, globals={"g": "L"},
    )
    right = keyed(
        [{"k": 1, "nested": {"value": "right"}}, {"k": 2, "nested": {"value": "other"}}],
        schema=row_schema, globals={"g": "R"},
    )
    zipped = hl.Table.multi_way_zip_join([left, right], "data", "globals")
    assert json.loads(zipped._native.ir_json())["node"] == "TableMultiWayZipJoin"
    assert zipped._native.physical_plan()
    assert [(r.k, r.data) for r in zipped.collect()] == [
        (0, [hl.Struct(nested=hl.Struct(value="left")), None]),
        (1, [None, hl.Struct(nested=hl.Struct(value="right"))]),
        (2, [hl.Struct(nested=hl.Struct(value="both")), hl.Struct(nested=hl.Struct(value="other"))]),
    ]
    assert hl.eval(zipped.globals) == hl.Struct(globals=[hl.Struct(g="L"), hl.Struct(g="R")])
    with pytest.raises(ValueError, match="at least one"):
        hl.Table.multi_way_zip_join([], "data", "globals")
    wrong_rows = keyed([{"k": 0, "nested": "different"}], schema=hl.tstruct(k=hl.tint32, nested=hl.tstr))
    with pytest.raises(TypeError, match="row type"):
        hl.Table.multi_way_zip_join([left, wrong_rows], "data", "globals")


def test_multi_way_zip_join_duplicate_keys_have_one_union_key_row():
    first = keyed(
        [{"k": 1, "v": "first-a"}, {"k": 1, "v": "first-b"}],
        schema=hl.tstruct(k=hl.tint32, v=hl.tstr),
    )
    second = keyed(
        [{"k": 1, "v": "second"}, {"k": 2, "v": "other"}],
        schema=hl.tstruct(k=hl.tint32, v=hl.tstr),
    )
    zipped = hl.Table.multi_way_zip_join([first, second], "data", "globals")
    assert zipped.count() == 2
    assert [row.k for row in zipped.collect()] == [1, 2]


def test_multi_way_zip_join_missing_keys_from_distinct_inputs_do_not_match():
    schema = hl.tstruct(k=hl.tint32, v=hl.tstr)
    left = keyed([{"k": None, "v": "left"}], schema=schema)
    right = keyed([{"k": None, "v": "right"}], schema=schema)
    zipped = hl.Table.multi_way_zip_join([left, right], "data", "globals")
    assert [(row.k, row.data) for row in zipped.collect()] == [
        (None, [hl.Struct(v="left"), None]),
        (None, [None, hl.Struct(v="right")]),
    ]


def test_multi_way_zip_join_keyless_tables_emit_one_row_for_the_empty_key():
    schema = hl.tstruct(v=hl.tstr)
    first = hl.Table.parallelize([{"v": "a"}, {"v": "b"}], schema=schema)
    second = hl.Table.parallelize([{"v": "c"}, {"v": "d"}, {"v": "e"}], schema=schema)
    zipped = hl.Table.multi_way_zip_join([first, second], "data", "globals")
    rows = zipped.collect()
    assert len(rows) == 1
    assert len(rows[0].data) == 2
    assert rows[0].data[0] is not None and rows[0].data[0].v in {"a", "b"}
    assert rows[0].data[1] is not None and rows[0].data[1].v in {"c", "d", "e"}


def test_multi_way_zip_join_output_names_can_match_non_key_and_global_fields():
    table = keyed(
        [{"k": 1, "v": "row"}], schema=hl.tstruct(k=hl.tint32, v=hl.tstr), globals={"g": "global"}
    )
    row_name_collision = hl.Table.multi_way_zip_join([table], "v", "globals")
    assert row_name_collision.collect()[0].v == [hl.Struct(v="row")]
    global_name_collision = hl.Table.multi_way_zip_join([table], "data", "g")
    assert hl.eval(global_name_collision.globals) == hl.Struct(g=[hl.Struct(g="global")])
    global_row_name_collision = hl.Table.multi_way_zip_join([table], "g", "globals")
    assert global_row_name_collision.collect()[0].g == [hl.Struct(v="row")]


def test_multi_way_zip_join_rejects_output_names_that_collide_with_keys_or_each_other():
    table = keyed([{"k": 1, "v": "row"}], schema=hl.tstruct(k=hl.tint32, v=hl.tstr))
    with pytest.raises(ValueError):
        hl.Table.multi_way_zip_join([table], "k", "globals")
    with pytest.raises(ValueError):
        hl.Table.multi_way_zip_join([table], "data", "k")
    with pytest.raises(ValueError):
        hl.Table.multi_way_zip_join([table], "same", "same")


def test_multi_way_zip_join_requires_matching_key_names_and_order():
    schema = hl.tstruct(a=hl.tint32, b=hl.tint32, v=hl.tstr)
    base = hl.Table.parallelize([{"a": 1, "b": 2, "v": "x"}], schema=schema)
    different_key = base.key_by("b")
    ordered_keys = base.key_by("a", "b")
    reversed_keys = base.key_by("b", "a")
    with pytest.raises(TypeError, match="key type"):
        hl.Table.multi_way_zip_join([base.key_by("a"), different_key], "data", "globals")
    with pytest.raises(TypeError, match="key type"):
        hl.Table.multi_way_zip_join([ordered_keys, reversed_keys], "data", "globals")


def test_join_written_inputs_and_missing_read_fail_only_on_action(tmp_path):
    left_path = tmp_path / "left.ht"
    right_path = tmp_path / "right.ht"
    left = keyed([{"k": 1, "v": "left"}], schema=hl.tstruct(k=hl.tint32, v=hl.tstr))
    right = keyed([{"k": 1, "v": "right"}], schema=hl.tstruct(k=hl.tint32, v=hl.tstr))
    left.write(left_path)
    right.write(right_path)
    joined = hl.read_table(left_path).join(hl.read_table(right_path))
    assert json.loads(joined._native.ir_json())["node"] == "TableJoin"
    assert joined.collect() == [hl.Struct(k=1, v="left", v_1="right")]

    missing_path = tmp_path / "removed-data.ht"
    left.write(missing_path)
    missing = hl.read_table(missing_path)
    data_files = list(missing_path.rglob("*.vortex"))
    assert data_files
    data_files[0].unlink()
    lazy_join = left.join(missing, "left")
    assert json.loads(lazy_join._native.ir_json())["node"] == "TableJoin"
    with pytest.raises(FileNotFoundError):
        lazy_join.collect()


@pytest.mark.parametrize("method, expected", [
    ("semi_join", [4]), ("anti_join", [5, 6, 7]),
])
def test_membership_join_executes_only_selected_left_partition(tmp_path, method, expected):
    path = tmp_path / "independent.viceberg"
    hl.range_table(12, n_partitions=3).write(path)
    left = hl.read_table(path)
    files = sorted(path.rglob("*.vortex"))
    assert len(files) == 3
    files[0].unlink()
    files[2].unlink()
    right = hl.Table.parallelize([{"idx": 4}], key="idx")
    joined = getattr(left, method)(right)
    selected = joined._filter_partitions([1])
    assert [row.idx for row in selected.collect()] == expected


def test_join_preserves_literal_case_sensitive_field_names():
    left = hl.Table.parallelize([{'K': 1, 'L_DUP': 'left'}], key='K')
    right = hl.Table.parallelize([{'K': 1, 'R_DUP': 'right'}], key='K')
    for how in ('inner', 'left', 'right', 'outer'):
        joined = left.join(right, how=how)
        assert joined.K.collect() == [1]
        assert joined.L_DUP.collect() == ['left']
        assert joined.R_DUP.collect() == ['right']
        assert joined.aggregate(hl.agg.collect(joined.row)) == joined.collect()


def test_union_retains_input_execution_partitions_after_computed_keys():
    first = hl.range_table(5, n_partitions=2)
    second = hl.range_table(5, n_partitions=2)
    second = second.key_by(idx=second.idx + 5)
    third = hl.range_table(5, n_partitions=2)
    third = third.key_by(idx=third.idx + 10)
    union = first.key_by().union(second.key_by(), third.key_by())
    assert union.n_partitions() == 6
    assert union.idx.collect() == list(range(15))
    assert union._same(hl.range_table(15).key_by())
