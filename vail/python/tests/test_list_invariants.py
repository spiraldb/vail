"""Invariant placement preserves lexical scope and list-operation identities."""
import pytest
import vail as hl


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('operation', ['map', 'filter', 'any', 'first'])
def test_outer_lambda_invariants(monkeypatch, enabled, operation):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_INVARIANT_MAP', raising=False)
    table = hl.Table.parallelize(
        [{'xs': [1, 2, 3]}, {'xs': []}, {'xs': None}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)))

    def inner(outer):
        if operation == 'map':
            return table.xs.map(lambda x: x + outer * 2)
        predicate = lambda x: x > outer * 2
        if operation == 'filter':
            return table.xs.filter(predicate)
        if operation == 'any':
            return hl.any(predicate, table.xs)
        return hl.find(predicate, table.xs)

    expected = {
        'map': [[3, 4, 5], [5, 6, 7], [7, 8, 9]],
        'filter': [[3], [], []],
        'any': [True, False, False],
        'first': [3, None, None],
    }
    assert table.select(result=table.xs.map(inner)).result.collect() == [
        expected[operation], [], None]


@pytest.mark.parametrize('enabled', [False, True])
def test_first_list_empty_is_missing(monkeypatch, enabled):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_INVARIANT_MAP', raising=False)
    table = hl.Table.parallelize(
        [{'limit': 1, 'groups': []}, {'limit': 2, 'groups': None}],
        schema=hl.tstruct(limit=hl.tint32, groups=hl.tarray(hl.tarray(hl.tint32))))
    result = hl.find(lambda xs: hl.len(xs) > table.limit, table.groups)
    assert table.select(result=result).result.collect() == [None, None]


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('scan', [False, True])
def test_fold_and_scan_invariants_preserve_initial_state(monkeypatch, enabled, scan):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_INVARIANT_MAP', raising=False)
    table = hl.Table.parallelize(
        [{'offset': 2, 'xs': [1, 2]}, {'offset': 9, 'xs': []},
         {'offset': 4, 'xs': None}],
        schema=hl.tstruct(offset=hl.tint32, xs=hl.tarray(hl.tint32)))
    operation = hl.array_scan if scan else hl.fold
    result = operation(lambda state, x: state + x + table.offset * 2,
                       table.offset, table.xs)
    expected = [[2, 7, 13], [9], None] if scan else [13, 9, None]
    assert table.select(result=result).result.collect() == expected


@pytest.mark.parametrize('enabled', [False, True])
def test_nested_bindings_and_unused_parameters(monkeypatch, enabled):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_INVARIANT_MAP', raising=False)
    table = hl.Table.parallelize([{'xs': [1, 2], 'offset': 4}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), offset=hl.tint32))
    result = table.xs.map(lambda outer: hl.bind(
        lambda bound: table.xs.map(lambda inner: hl.struct(
            value=inner+bound, unused=table.xs.map(lambda ignored: table.offset))),
        outer*2))
    rows = table.select(result=result).result.collect()[0]
    assert [[x.value for x in group] for group in rows] == [[3, 4], [5, 6]]
    assert [[x.unused for x in group] for group in rows] == [[[4, 4], [4, 4]], [[4, 4], [4, 4]]]


@pytest.mark.parametrize('enabled', [False, True])
def test_invariant_placement_preserves_lazy_error_branches(monkeypatch, enabled):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_INVARIANT_MAP', raising=False)
    table = hl.Table.parallelize([{'xs': [0, 2], 'offset': 12}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), offset=hl.tint32))
    result = table.xs.map(lambda x: hl.if_else(x == 0, 0, table.offset // x))
    assert table.select(result=result).result.collect() == [[0, 6]]


@pytest.mark.parametrize('scan', [False, True])
def test_fold_invariants_keep_scalar_list_and_empty_input_contract(monkeypatch, scan):
    monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')
    table = hl.Table.parallelize([{'offset': 2, 'xs': [1]}],
        schema=hl.tstruct(offset=hl.tint32, xs=hl.tarray(hl.tint32)))
    operation = hl.array_scan if scan else hl.fold
    result = operation(lambda state, x: state+x+table.offset, 0, hl.range(4))
    assert table.select(result=result).result.collect() == [
        [0, 2, 5, 9, 14] if scan else 14]
    empty = table.filter(False)
    result = operation(lambda state, x: state+x+empty.offset,
                       hl.array([1])[5], empty.xs)
    assert empty.select(result=result).collect() == []


@pytest.mark.parametrize('enabled', [False, True])
def test_composed_maps_keep_missing_values_and_outer_bindings(monkeypatch, enabled):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_COMPOSE_MAPS', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_COMPOSE_MAPS', raising=False)
    table = hl.Table.parallelize([
        {'xs': [None, -1, 2], '__vail_map_item': 7},
        {'xs': [], '__vail_map_item': 4},
        {'xs': None, '__vail_map_item': 9}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), __vail_map_item=hl.tint32))
    result = table.xs.map(lambda x: hl.or_missing(x > 0, x*2)).map(
        lambda x: hl.coalesce(x, table['__vail_map_item']))
    assert table.select(result=result).result.collect() == [[7, 7, 4], [], None]
    nested = hl.array([1, 2]).map(lambda outer:
        hl.array([1, 3]).map(lambda x: x+outer).map(lambda x: x+outer))
    assert hl.eval(nested) == [[3, 5], [5, 7]]


@pytest.mark.parametrize('enabled', [False, True])
def test_composed_maps_bind_each_original_value_once(monkeypatch, enabled):
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_COMPOSE_MAPS', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_COMPOSE_MAPS', raising=False)
    table = hl.Table.parallelize([{'xs': [1, 2, None], 'offset': 3}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), offset=hl.tint32))
    result = table.xs.map(lambda x: hl.struct(a=x+table.offset, b=x*2)).map(
        lambda value: hl.struct(total=value.a+value.b, unchanged=value.a))
    assert table.select(result=result).result.collect() == [[
        hl.Struct(total=6, unchanged=4), hl.Struct(total=9, unchanged=5),
        hl.Struct(total=None, unchanged=None)]]


def test_composed_maps_preserve_sliced_list_offsets_and_parent_masks(monkeypatch):
    import pyarrow as pa
    monkeypatch.setenv('VAIL_EXPERIMENT_COMPOSE_MAPS', '1')
    values = pa.ListArray.from_arrays(
        pa.array([0, 1, 3, 4, 6], type=pa.int32()),
        pa.array([99, 100, 200, 3, None, 4], type=pa.int32()),
        mask=pa.array([False, True, False, False])).slice(1, 3)
    table = hl.Table.parallelize(pa.table({'xs': values}))
    result = table.xs.map(lambda x: x+1).map(lambda x: x*2)
    assert table.select(result=result).result.collect() == [None, [8], [None, 10]]


def test_composed_map_chain_preserves_stage_order(monkeypatch):
    monkeypatch.setenv('VAIL_EXPERIMENT_COMPOSE_MAPS', '1')
    table = hl.Table.parallelize([{'xs': [1, 2, None]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    result = table.xs
    for increment in range(12):
        result = result.map(lambda x: x*2+increment)
    expected = [1, 2, None]
    for increment in range(12):
        expected = [None if x is None else x*2+increment for x in expected]
    assert table.select(result=result).result.collect() == [expected]


@pytest.mark.parametrize('enabled', [False, True])
def test_aligned_row_invariants_preserve_entry_masks_and_nullable_values(monkeypatch, enabled):
    monkeypatch.setenv('VAIL_EXPERIMENT_INDEXED_BATCH', '1')
    if enabled:
        monkeypatch.setenv('VAIL_EXPERIMENT_ALIGNED_INVARIANTS', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_ALIGNED_INVARIANTS', raising=False)
    mt = hl.utils.range_matrix_table(3, 3)
    mt = mt.annotate_rows(info=hl.or_missing(mt.row_idx != 1,
        hl.struct(xs=hl.range(mt.row_idx+1))), __vail_row_invariant_0=9)
    mt = mt.filter_entries(mt.col_idx != 1)
    mt = mt.annotate_entries(length=hl.len(mt.info.xs),
        missing=hl.is_missing(mt.info.xs),
        fallback=hl.coalesce(hl.len(mt.info.xs), 0),
        witness=mt.row_idx+mt.col_idx)
    rows = mt.localize_entries('entries', 'columns').collect()
    assert [[None if e is None else (e.length,e.missing,e.fallback,e.witness)
             for e in row.entries] for row in rows] == [
        [(1,False,1,0),None,(1,False,1,2)],
        [(None,True,0,1),None,(None,True,0,3)],
        [(3,False,3,2),None,(3,False,3,4)]]


@pytest.mark.parametrize('filtered', [False, True])
def test_borrowed_entry_inputs_keep_case_domains_replay_and_concurrent_results(monkeypatch, filtered):
    from concurrent.futures import ThreadPoolExecutor
    monkeypatch.setenv('VAIL_EXPERIMENT_INDEXED_BATCH', '1')
    monkeypatch.setenv('VAIL_EXPERIMENT_BORROW_INDEXED_INPUT', '1')
    mt = hl.utils.range_matrix_table(4, 3)
    mt = mt.annotate_entries(values=hl.range(mt.row_idx+mt.col_idx+1))
    original = mt
    if filtered:
        mt = mt.filter_entries(mt.col_idx != 1)
    mt = mt.annotate_entries(score=hl.len(mt.values)+mt.row_idx,
        chosen=hl.if_else(mt.col_idx % 2 == 0, mt.values, mt.values[::-1]))
    query = mt.localize_entries('entries', 'columns')
    expected = query.to_arrow()
    rows = query.collect()
    for row in rows:
        for col, entry in enumerate(row.entries):
            if filtered and col == 1:
                assert entry is None
            else:
                values = list(range(row.row_idx+col+1))
                assert entry['values'] == values
                assert entry.chosen == (values if col % 2 == 0 else values[::-1])
                assert entry.score == len(values)+row.row_idx
    with ThreadPoolExecutor(max_workers=2) as pool:
        for result in pool.map(lambda _: query.to_arrow(), range(2)):
            assert result.equals(expected, check_metadata=True)
    assert query.to_arrow().equals(expected, check_metadata=True)
    # A consumer of the original producer must remain independent of its views.
    assert [[entry['values'] for entry in row.entries] for row in
            original.localize_entries('entries', 'columns').collect()] == [
        [list(range(row+col+1)) for col in range(3)] for row in range(4)]


def test_coalesce_preserves_nested_values_and_lazy_fallback():
    table = hl.Table.parallelize(
        [{'xs': [1, None, 3]}, {'xs': []}, {'xs': None}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    result = table.xs.map(lambda x: hl.coalesce(
        x, hl.coalesce(hl.missing(hl.tint32), 7)))
    assert table.select(result=result).result.collect() == [[1, 7, 3], [], None]
    safe = hl.coalesce(5, hl.int32(1) // hl.int32(0))
    assert table.select(result=safe).result.collect() == [5, 5, 5]


def test_constant_list_callback_and_lazy_branch():
    assert hl.eval(hl.array([1, 2, 3]).map(lambda x: x + 1)) == [2, 3, 4]
    table = hl.Table.parallelize([{'xs': [0, 2]}, {'xs': []}, {'xs': None}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    result = table.xs.map(lambda x: hl.if_else(x == 0, 7, 12 // x))
    for _ in range(2):
        assert table.select(result=result).result.collect() == [[7, 6], [], None]
