import pytest
import vail as hl


def test_dictionary_views_size_map_values_and_conversion():
    d = hl.literal({'a': 1, 'b': None, None: 3}, hl.tdict(hl.tstr, hl.tint32))
    assert hl.eval(d.keys()) == ['a', 'b', None]
    assert hl.eval(d.values()) == [1, None, 3]
    assert hl.eval(d.key_set()) == {'a', 'b', None}
    assert hl.eval(d.items()) == [('a', 1), ('b', None), (None, 3)]
    assert hl.eval(hl.array(d)) == hl.eval(d.items())
    assert hl.eval(d.size()) == hl.eval(hl.len(d)) == 3
    result = d.map_values(lambda x: x + 2)
    assert result.dtype == hl.tdict(hl.tstr, hl.tint32)
    assert hl.eval(result) == {'a': 3, 'b': None, None: 5}
    assert hl.eval(d.map_values(lambda x: hl.struct(v=x))) == {
        'a': hl.Struct(v=1), 'b': hl.Struct(v=None), None: hl.Struct(v=3)}


def test_dictionary_get_missing_value_missing_key_and_default():
    assert hl.eval(hl.missing(hl.tint32) == hl.missing(hl.tint32)) is None
    d = hl.literal({'a': 1, 'b': None, None: 3}, hl.tdict(hl.tstr, hl.tint32))
    assert hl.eval(d.get('a')) == 1
    assert hl.eval(d.get('b', 42)) is None
    assert hl.eval(d.get('absent', 42)) == 42
    assert hl.eval(d.get('absent')) is None
    assert hl.eval(d.get(hl.missing(hl.tstr))) == 3
    assert hl.eval(d.contains('a')) is True
    assert hl.eval(d.contains('b')) is True
    assert hl.eval(d.contains('absent')) is False
    assert hl.eval(d.contains(None)) is True
    assert hl.eval(d['a']) == 1
    assert hl.eval(d[hl.missing(hl.tstr)]) == 3


def test_dictionary_missing_and_empty():
    for d in (hl.missing(hl.tdict(hl.tstr, hl.tint32)), hl.empty_dict(hl.tstr, hl.tint32)):
        absent = hl.eval(d) is None
        assert hl.eval(d.size()) == (None if absent else 0)
        assert hl.eval(d.get('a', 42)) == (None if absent else 42)
        assert hl.eval(d.contains('a')) == (None if absent else False)
        assert hl.eval(d.keys()) == (None if absent else [])
        assert hl.eval(d.items()) == (None if absent else [])
        assert hl.eval(d.map_values(lambda x: x + 1)) == (None if absent else {})


@pytest.mark.parametrize('operation, expected', [
    ('difference', {1}), ('intersection', {2, None}), ('union', {1, 2, 3, None}),
])
def test_set_algebra_and_missing_members(operation, expected):
    left = hl.literal({1, 2, None}, hl.tset(hl.tint32))
    right = hl.literal({2, 3, None}, hl.tset(hl.tint32))
    assert hl.eval(getattr(left, operation)(right)) == expected
    assert hl.eval(getattr(left, operation)(hl.missing(left.dtype))) is None
    assert hl.eval(getattr(hl.missing(left.dtype), operation)(right)) is None


def test_set_membership_updates_and_operators():
    s = hl.literal({1, None}, hl.tset(hl.tint32))
    assert hl.eval(s.contains(1)) is True
    assert hl.eval(s.contains(2)) is False
    assert hl.eval(s.contains(None)) is True
    assert hl.eval(s.add(2)) == {1, 2, None}
    assert hl.eval(s.add(None)) == {1, None}
    assert hl.eval(s.remove(None)) == {1}
    assert hl.eval(s.remove(1)) == {None}
    assert hl.eval(s.remove(5)) == {1, None}
    t = hl.literal({1, 2}, hl.tset(hl.tint32))
    assert hl.eval(s & t) == {1}
    assert hl.eval(s | t) == {1, 2, None}
    assert hl.eval(s - t) == {None}
    assert hl.eval(s ^ t) == {2, None}
    assert hl.eval({1, 2} & s) == {1}
    assert hl.eval({1, 2} - s) == {2}
    assert hl.eval({1, 2} | s) == {1, 2, None}
    assert hl.eval({1, 2} ^ s) == {2, None}
    assert hl.eval(s.size()) == hl.eval(s.length()) == hl.eval(hl.len(s)) == 2


def test_nested_set_membership_and_algebra_use_native_equality():
    inner = hl.tset(hl.tint32)
    outer = hl.tset(inner)
    left = hl.literal({frozenset({1, 2}), frozenset({5})}, outer)
    right = hl.literal({frozenset({2, 1}), frozenset({3})}, outer)
    assert hl.eval(left.contains(hl.literal({2, 1}, inner))) is True
    assert hl.eval(left.contains(hl.literal({1, 3}, inner))) is False
    assert hl.eval(left.union(right)) == {frozenset({1, 2}), frozenset({3}), frozenset({5})}
    assert hl.eval(left.intersection(right)) == {frozenset({1, 2})}
    assert hl.eval(left.difference(right)) == {frozenset({5})}


def test_set_subset_and_superset():
    s, t = hl.literal({1}), hl.literal({1, 2})
    assert hl.eval(s.is_subset(t)) is True
    assert hl.eval(s < t) is True
    assert hl.eval(s <= s) is True
    assert hl.eval(t > s) is True
    assert hl.eval(t >= t) is True
    assert hl.eval(t < s) is False
    assert hl.eval(s < s) is False
    assert hl.eval(hl.empty_set(hl.tint32).is_subset(s)) is True
    assert hl.eval(hl.missing(s.dtype).is_subset(s)) is None


def test_set_construction_mapping_filter_and_array_conversion():
    s = hl.set(hl.array([1, 2, 1, None]))
    assert s.dtype == hl.tset(hl.tint32)
    assert hl.eval(s) == {1, 2, None}
    assert hl.eval(s.map(lambda x: x % 2)) == {0, 1, None}
    assert s.map(lambda x: x + 1).dtype == hl.tset(hl.tint32)
    assert hl.eval(s.filter(lambda x: x > 1)) == {2}
    assert set(hl.eval(hl.array(s))) == {1, 2, None}
    assert hl.eval(hl.empty_set(hl.tint32)) == set()
    assert hl.eval(hl.set(hl.missing(hl.tarray(hl.tint32)))) is None


def test_set_predicates_and_starmap():
    s = hl.literal({1, 2})
    assert hl.eval(s.any(lambda x: x > 1)) is True
    assert hl.eval(s.all(lambda x: x > 0)) is True
    assert hl.eval(s.all(lambda x: x > 1)) is False
    assert hl.eval(s.find(lambda x: x > 1)) == 2
    assert hl.eval(s.find(lambda x: x > 9)) is None
    empty = hl.empty_set(hl.tint32)
    assert hl.eval(empty.any(lambda x: x > 0)) is False
    assert hl.eval(empty.all(lambda x: x > 0)) is True
    missing = hl.missing(s.dtype)
    assert hl.eval(missing.any(lambda x: x > 0)) is None
    assert hl.eval(hl.literal({(1, 2), (2, 1)}).starmap(lambda x, y: x + y)) == {3}
    nullable = hl.literal({1, None}, hl.tset(hl.tint32))
    assert hl.eval(nullable.any(lambda x: x > 5)) is None
    assert hl.eval(nullable.any(lambda x: x > 0)) is True
    assert hl.eval(nullable.all(lambda x: x > 0)) is None


def test_set_struct_projection():
    source = hl.set(hl.array([hl.struct(a=1, b=2), hl.struct(a=1, b=3)]))
    assert hl.eval(source.a) == {1}
    assert hl.eval(source['a']) == {1}


def test_collection_operations_in_lazy_table_with_globals(tmp_path):
    t = hl.Table.parallelize([{'d': {'x': 1}, 's': {1, 2}}, {'d': {}, 's': set()}],
                            schema=hl.tstruct(d=hl.tdict(hl.tstr, hl.tint32), s=hl.tset(hl.tint32)),
                            n_partitions=2).annotate_globals(offset=10)
    result = t.annotate(d=t.d.map_values(lambda x: x + t.offset),
                        v=t.d.get('x', t.offset), s=t.s.map(lambda x: x + t.offset))
    assert result.n_partitions() == 2
    assert [(r.d, r.v, r.s) for r in result.collect()] == [({'x': 11}, 1, {11, 12}), ({}, 10, set())]
    result.write(str(tmp_path / 'collections'))
    assert hl.read_table(str(tmp_path / 'collections'))._same(result)


def test_explicit_datafusion_gaps():
    # Collection distinctness and key equality follow DataFusion's native
    # float semantics, including its treatment of signed zero.
    assert hl.eval(hl.set(hl.array([-0.0, 0.0]))) == {0.0}
    assert hl.eval(hl.literal({1}).map(lambda x: hl.float64(x))) == {1.0}
    assert hl.eval(hl.dict(hl.array([('a', 1), ('a', 2)]))) == {'a': 2}
    with pytest.raises(NotImplementedError, match='fold'):
        hl.literal({1}).fold(lambda a, b: a + b, 0)
    with pytest.raises(NotImplementedError, match='group_by'):
        hl.literal({1}).group_by(lambda x: x)
    with pytest.raises(NotImplementedError, match='exact integer'):
        hl.sum(hl.literal({1, 2}))


def test_set_flatmap_skips_missing_inner_sets():
    s = hl.literal({1, 2})
    result = s.flatmap(lambda x: hl.if_else(x == 1, hl.missing(hl.tset(hl.tint32)), hl.set(hl.array([x, x + 1]))))
    assert result.dtype == hl.tset(hl.tint32)
    assert hl.eval(result) == {2, 3}
    assert hl.eval(hl.empty_set(hl.tint32).flatmap(lambda x: hl.set(hl.array([x])))) == set()
    assert hl.eval(hl.missing(hl.tset(hl.tint32)).flatmap(lambda x: hl.set(hl.array([x])))) is None


def test_dictionary_constructor_with_expression_values():
    assert hl.eval(hl.dict({'a': hl.int32(2), 'b': hl.missing(hl.tint32)})) == {'a': 2, 'b': None}


def test_dictionary_construction_canonical_keys_and_duplicate_wins():
    assert hl.dict({'a': 1}).dtype == hl.tdict(hl.tstr, hl.tint32)
    pairs = hl.array([('a', 1), ('a', 2), (hl.missing(hl.tstr), 5),
                      ('missing', hl.missing(hl.tint32))])
    value = hl.dict(pairs)
    assert hl.eval(value) == {'a': 2, None: 5, 'missing': None}
    assert hl.eval(value.get(hl.missing(hl.tstr))) == 5
    assert hl.eval(value[hl.missing(hl.tstr)]) == 5
    with pytest.raises(RuntimeError, match='dictionary key is missing'):
        hl.eval(value['absent'])

    array_keys = hl.dict(hl.array([(hl.array([1, None]), 10),
                                   (hl.array([1, None]), 20)]))
    assert hl.eval(array_keys) == {(1, None): 20}
    struct_keys = hl.dict(hl.array([(hl.struct(a=1, b=hl.missing(hl.tint32)), 10),
                                    (hl.struct(a=1, b=hl.missing(hl.tint32)), 20)]))
    assert hl.eval(struct_keys) == {hl.Struct(a=1, b=None): 20}


def test_unordered_collection_equality_uses_native_normalized_arrays():
    left = hl.dict(hl.array([('a', 1), ('b', 2)]))
    reordered = hl.dict(hl.array([('b', 2), ('a', 1)]))
    changed = hl.dict(hl.array([('b', 3), ('a', 1)]))
    assert hl.eval(left == reordered) is True
    assert hl.eval(left != changed) is True
    assert hl.eval(hl.missing(left.dtype) == hl.missing(left.dtype)) is None

    first = hl.literal([1, 2], hl.tset(hl.tint32))
    second = hl.literal([2, 1], hl.tset(hl.tint32))
    assert hl.eval(first == second) is True
    assert hl.eval(hl.missing(first.dtype) == hl.missing(first.dtype)) is None
    left_values = hl.dict(hl.array([('x', first)]))
    right_values = hl.dict(hl.array([('x', second)]))
    assert hl.eval(left_values == right_values) is True

    left_nested = hl.array([hl.array([first])])
    right_nested = hl.array([hl.array([second])])
    assert hl.eval(left_nested == right_nested) is True


def test_dictionary_rejects_set_and_dict_keys_without_native_keying():
    with pytest.raises(NotImplementedError, match='Set/Dictionary/ndarray ordering and keying'):
        hl.dict(hl.array([(hl.set(hl.array([1, 2])), 10),
                          (hl.set(hl.array([2, 1])), 20)]))
    first = hl.literal({1: 10, 2: 20}, hl.tdict(hl.tint32, hl.tint32))
    reordered = hl.literal({2: 20, 1: 10}, hl.tdict(hl.tint32, hl.tint32))
    with pytest.raises(NotImplementedError, match='Set/Dictionary/ndarray ordering and keying'):
        hl.dict(hl.array([(first, 10), (reordered, 20)]))
    cast_keys = hl.dict(hl.array([(hl.int64(1), 10), (hl.int64(1), 20)]))
    assert hl.eval(cast_keys) == {1: 20}
    arithmetic_keys = hl.dict(hl.array([(hl.int32(1) + 0, 10), (hl.int32(1), 20)]))
    assert hl.eval(arithmetic_keys) == {1: 20}


def test_mapping_expression_dictionary_keeps_first_duplicate():
    expression = hl.dict({hl.int32(1): 10, hl.int32(1): 20})
    assert hl.eval(expression) == {1: 10}


def test_dictionary_constructor_keeps_row_and_global_expressions_lazy():
    table = hl.range_table(3).annotate_globals(offset=10)
    expression = hl.dict(hl.array([(table.idx, table.offset),
                                   (table.idx, table.offset + 1)]))
    result = table.annotate(mapping=expression)
    assert [row.mapping for row in result.collect()] == [{0: 10}, {1: 10}, {2: 10}]


@pytest.mark.parametrize('name, expected', [('sum', 3.0), ('product', 2.0), ('min', 1.0), ('max', 2.0)])
def test_set_builtin_reductions(name, expected):
    value = hl.literal({1.0, 2.0, None}, hl.tset(hl.tfloat64))
    assert hl.eval(getattr(hl, name)(value)) == expected
    assert hl.eval(getattr(hl, name)(value, filter_missing=False)) is None
    empty = hl.empty_set(hl.tfloat64)
    assert hl.eval(getattr(hl, name)(empty)) == {'sum': 0.0, 'product': 1.0, 'min': None, 'max': None}[name]
    assert hl.eval(getattr(hl, name)(hl.missing(value.dtype))) is None


def test_set_integer_min_max():
    value = hl.literal({2**60, 2**60 + 1}, hl.tset(hl.tint64))
    assert hl.eval(hl.min(value)) == 2**60
    assert hl.eval(hl.max(value)) == 2**60 + 1


def test_set_float_algebra_uses_datafusion_native_equality():
    import math
    import struct
    nan1 = struct.unpack('>d', bytes.fromhex('7ff8000000000001'))[0]
    nan2 = struct.unpack('>d', bytes.fromhex('7ff8000000000002'))[0]
    left = hl.literal([-0.0, nan1, None], hl.tset(hl.tfloat64))
    right = hl.literal([0.0, nan2, None], hl.tset(hl.tfloat64))
    assert hl.eval(left.contains(0.0)) is True
    assert hl.eval(left.contains(-0.0)) is True
    assert hl.eval(left.contains(nan1)) is True
    assert hl.eval(left.contains(nan2)) is False
    union = hl.eval(hl.array(left.union(right)))
    assert len(union) == 4 and sum(math.isnan(x) for x in union if x is not None) == 2
    assert hl.eval(hl.array(left.intersection(right))) == [0.0, None]
    difference = hl.eval(hl.array(left.difference(right)))
    assert len(difference) == 1 and math.isnan(difference[0])
    assert hl.eval(hl.len(left.add(0.0))) == 3
    assert hl.eval(hl.len(left.remove(nan1))) == 2
    assert hl.eval(left.is_subset(left.union(right))) is True
    assert hl.eval(hl.len(left.union(hl.missing(left.dtype)))) is None


def test_collection_predicates_and_dictionary_in_aggregation():
    t = hl.utils.range_table(3)
    d = hl.literal({1: 10, 2: None}, hl.tdict(hl.tint32, hl.tint32))
    assert t.aggregate(hl.agg.collect(d.get(t.idx, 99))) == [99, 10, None]
    assert t.aggregate(hl.agg.sum(hl.literal({1, 2}).contains(t.idx))) == 2
    assert t.aggregate(hl.agg.explode(lambda x: hl.agg.count_where(hl.literal({1, 2}).any(lambda y: x == y)), hl.array([1, 3]))) == 3


def test_find_skips_missing_elements():
    s = hl.literal([None, 1], hl.tset(hl.tint32))
    assert hl.eval(s.find(lambda x: True)) == 1


def test_dictionary_complex_key_lookup():
    d = hl.literal({(1, None): 7}, hl.tdict(hl.ttuple(hl.tint32, hl.tint32), hl.tint32))
    assert hl.eval(d.get(hl.as_expr((1, hl.missing(hl.tint32))))) == 7
    assert hl.eval(d.contains(hl.as_expr((2, hl.missing(hl.tint32))))) is False


def test_datafusion_function_ir_and_validation():
    s = hl.set(hl.array([1, 1]))
    assert s.to_ir()['function'] == 'array_distinct'
    assert s.union(hl.literal({2})).to_ir()['function'] == 'array_union'
    with pytest.raises(TypeError, match='same type'):
        s.union(hl.literal({'a'}))
    with pytest.raises(TypeError, match='bool'):
        s.any(lambda x: x)
    with pytest.raises(TypeError, match='callable'):
        hl.literal({'a': 1}, hl.tdict(hl.tstr, hl.tint32)).map_values(0)


def test_dictionary_key_and_default_coercion_matches_hail():
    d = hl.literal({1: 2}, hl.tdict(hl.tint64, hl.tint64))
    assert hl.eval(d.get(hl.int32(1), hl.int32(0))) == 2
    narrow = hl.literal({1: 2}, hl.tdict(hl.tint32, hl.tint32))
    with pytest.raises(TypeError, match='expected'):
        narrow.get(hl.int64(1))
    with pytest.raises(TypeError, match='expected'):
        narrow.get('1')
    with pytest.raises(TypeError, match='expected'):
        narrow.get(1, hl.float64(0))


def test_numeric_set_min_max_obey_ieee_total_order():
    import math
    import struct
    nan = struct.unpack('>d', bytes.fromhex('7ff8000000000001'))[0]
    value = hl.literal([-0.0, 0.0, nan], hl.tset(hl.tfloat64))
    assert math.copysign(1, hl.eval(hl.min(value))) == -1
    assert struct.pack('>d', hl.eval(hl.max(value))).hex() == '7ff8000000000001'


def test_sorted_native_primitive_collections_keep_missing_last():
    assert hl.eval(hl.sorted(hl.array([3, None, 1, 2]))) == [1, 2, 3, None]
    assert hl.eval(hl.sorted(hl.array([3, None, 1]), reverse=True)) == [3, 1, None]
    assert hl.eval(hl.sorted(hl.set([3, 1, 2]))) == [1, 2, 3]
