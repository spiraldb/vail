import pytest
import vail as hl


def test_set_type_wire_and_dtype_are_distinct_from_array():
    assert hl.dtype("set<int32>") == hl.tset(hl.tint32)
    assert hl.type_to_wire(hl.tset(hl.tint32)) == {"set": "int32"}
    assert hl.type_from_wire({"set": "int32"}) == hl.tset(hl.tint32)
    assert hl.tset(hl.tint32) != hl.tarray(hl.tint32)


def test_set_literal_inference_and_explicit_empty_or_missing_types():
    assert hl.literal({1, 2, 3}).dtype == hl.tset(hl.tint32)
    assert hl.literal(set(), hl.tset(hl.tint32)).dtype == hl.tset(hl.tint32)
    assert hl.literal(None, hl.tset(hl.tint32)).dtype == hl.tset(hl.tint32)
    with pytest.raises(ValueError, match="empty set"):
        hl.literal(set())


def test_set_literals_collect_in_globals_and_survive_storage(tmp_path):
    table = hl.range_table(3).annotate(x15={1, 2, 3}).annotate_globals(empty=hl.literal(set(), hl.tset(hl.tint32)))
    assert table.x15.dtype == hl.tset(hl.tint32)
    assert table.x15.collect() == [{1, 2, 3}] * 3
    assert hl.eval(table.globals).empty == set()
    assert table._same(table)

    path = tmp_path / "sets"
    table.write(path)
    restored = hl.read_table(path)
    assert restored.x15.dtype == hl.tset(hl.tint32)
    assert restored.collect() == table.collect()
    assert restored._same(table)


def test_set_literals_deduplicate_and_preserve_missing_values():
    table = hl.range_table(1).annotate(
        values=hl.literal([3, None, 3, 1, None], hl.tset(hl.tint32))
    )
    assert table.values.collect() == [{1, 3, None}]


def test_float32_sets_canonicalize_rounding_signed_zero_and_nan():
    value = hl.literal(
        [1.0, 1.0 + 2**-25, 0.0, -0.0, float("nan"), float("nan")],
        hl.tset(hl.tfloat32),
    )
    result = hl.eval(value)
    assert len(result) == 3
    assert 1.0 in result
    assert 0.0 in result
    assert sum(isinstance(item, float) and item != item for item in result) == 1


def test_nested_set_eval_and_storage_roundtrip(tmp_path):
    nested_type = hl.tset(hl.tset(hl.tint32))
    nested = hl.literal({frozenset({1, 2}), frozenset({3})}, nested_type)
    expected = {frozenset({1, 2}), frozenset({3})}
    assert hl.eval(nested) == expected

    table = hl.range_table(2).annotate(nested=nested)
    path = tmp_path / "nested-sets"
    table.write(path)
    restored = hl.read_table(path)
    assert restored.collect() == table.collect()
    assert restored.nested.dtype == nested_type


def test_set_globals_eval_index_binding_and_multirow_annotation():
    source = hl.range_table(0).annotate_globals(members={4, 2, 3})
    assert hl.eval(source.globals)["members"] == {2, 3, 4}
    indexed = source.index_globals()

    table = hl.range_table(3).annotate(copy=indexed.members)
    assert table.copy.dtype == hl.tset(hl.tint32)
    assert table.copy.collect() == [{2, 3, 4}] * 3
    assert hl.eval(indexed)["members"] == {2, 3, 4}


def test_set_same_ignores_order_but_checks_missingness_and_collection_type():
    first = hl.range_table(1).annotate(values=hl.literal([3, None, 1], hl.tset(hl.tint32)))
    reordered = hl.range_table(1).annotate(values=hl.literal([1, 3, None], hl.tset(hl.tint32)))
    without_missing = hl.range_table(1).annotate(values=hl.literal([1, 3], hl.tset(hl.tint32)))
    as_array = hl.range_table(1).annotate(values=[1, 3])

    assert first._same(reordered)
    assert not first._same(without_missing)
    assert not first._same(as_array)


def test_parallelize_typed_set_fields_keep_set_dtype_and_missingness():
    table = hl.Table.parallelize(
        [{"values": {2, None, 2}}, {"values": set()}, {"values": None}],
        schema=hl.tstruct(values=hl.tset(hl.tint32)),
    )
    assert table.values.dtype == hl.tset(hl.tint32)
    assert table.values.collect() == [{2, None}, set(), None]


def test_nested_set_localization_uses_hashable_frozensets():
    nested_type = hl.tset(hl.tset(hl.tint32))
    value = hl.decode_value([[1, 2], [3]], nested_type)
    assert value == {frozenset({1, 2}), frozenset({3})}
    encoded = hl.encode_value(value)
    assert hl.decode_value(encoded, nested_type) == value


def test_set_type_keeps_array_element_logical_type():
    assert hl.tset(hl.tarray(hl.tint32)) == hl.tset(hl.tarray(hl.tint32))
    with pytest.raises(NotImplementedError, match="cannot localize set elements"):
        hl.decode_value([[1, 2]], hl.tset(hl.tarray(hl.tint32)))
