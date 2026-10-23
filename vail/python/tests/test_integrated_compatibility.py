"""Public regressions at compatibility-branch boundaries."""
import pickle

import vail as hl


def test_inferred_frozendict_retains_dictionary_values_and_pickle_contract():
    frozen = hl.utils.frozendict({'a': 3, 'b': 7})
    expression = hl.literal(pickle.loads(pickle.dumps(frozen)))
    assert expression.dtype == hl.tdict(hl.tstr, hl.tint32)
    assert hl.eval(expression.values()) == [3, 7]
    assert hl.eval(expression) == frozen


def test_struct_rename_keeps_nested_dictionary_and_numeric_promotion():
    value = hl.struct(mapping={'a': [hl.int32(2)], 'b': [hl.float64(3)]}, missing=hl.missing(hl.tint32))
    renamed = value.rename({'mapping': 'renamed'})
    assert renamed.renamed.dtype == hl.tdict(hl.tstr, hl.tarray(hl.tfloat64))
    assert hl.eval(renamed) == hl.Struct(missing=None, renamed={'a': [2.0], 'b': [3.0]})
    assert hl.eval(hl.missing(value.dtype).rename({'mapping': 'renamed'})) is None


def test_count_does_not_change_ordered_selection_scan_or_collection():
    table = hl.Table.parallelize([hl.Struct(k=i, value=v) for i, v in enumerate([3, 1, 3, 2, 1])], n_partitions=3)
    ordered = table.order_by(hl.desc(table.value), table.k)
    assert ordered._force_count() == 5
    assert [(r.k, r.value) for r in ordered.collect()] == [(0, 3), (2, 3), (3, 2), (1, 1), (4, 1)]
    selected = ordered.head(3)
    assert selected._force_count() == 3
    assert [r.k for r in selected.collect()] == [0, 2, 3]
    scanned = ordered.annotate(prefix=hl.scan.sum(ordered.value))
    assert scanned._force_count() == 5
    assert [r.prefix for r in scanned.collect()] == [0, 3, 6, 8, 9]
    collected = ordered.aggregate(hl.agg.collect(ordered.value))
    assert collected == [3, 3, 2, 1, 1]
    assert ordered.aggregate(hl.agg.count()) == 5
    assert [r.k for r in ordered.collect()] == [0, 2, 3, 1, 4]
