"""Optimizer contracts for native semantic kernels, beyond value-only examples."""
import pytest
import vail as hl


def test_invalid_regex_is_deferred_until_its_row_is_selected():
    table = hl.Table.parallelize([
        {'keep': False, 'text': 'bad', 'pattern': '['},
        {'keep': True, 'text': 'abc', 'pattern': '^a'},
    ])
    selected = table.filter(table.keep)
    result = selected.annotate(match=selected.text.matches(selected.pattern))
    assert [row.match for row in result.collect()] == [True]
    with pytest.raises(RuntimeError, match='[Rr]eg|pattern|parse'):
        table.annotate(match=table.text.matches(table.pattern)).collect()


def test_dead_branch_does_not_evaluate_invalid_regex():
    invalid = hl.literal('abc').matches('[')
    assert hl.eval(hl.if_else(False, invalid, True)) is True
    assert hl.eval(hl.if_else(hl.missing(hl.tbool), invalid, True)) is None


def test_integer_negation_scalar_array_and_null_have_one_overflow_policy():
    minimum = -(1 << 63)
    assert hl.eval(-hl.int64(minimum)) == minimum
    assert hl.eval(hl.array([hl.int64(minimum), hl.int64(9), hl.missing(hl.tint64)]).map(lambda x: -x)) == [minimum, -9, None]


def test_reused_random_handle_keeps_context_and_distinct_streams():
    table = hl.range_table(24, n_partitions=3)
    random = hl.rand_unif(seed=17)
    result = table.annotate(first=random, same=random, other=hl.rand_unif(seed=18))
    first = result.collect()
    assert first == result.collect()
    assert all(row.first == row.same for row in first)
    assert any(row.first != row.other for row in first)
    assert len({row.first for row in first}) == 24


def test_folded_nested_collection_types_remain_distinct():
    value = hl.struct(array=hl.array([1, 2]), aset=hl.set([1, 2]), pair=hl.tuple([1, 'x']))
    assert hl.eval(value) == hl.Struct(array=[1, 2], aset={1, 2}, pair=(1, 'x'))
    table = hl.range_table(3).annotate(value=value)
    assert table.value.dtype == value.dtype
    assert [row.value for row in table.collect()] == [hl.eval(value)] * 3


def test_range_step_validation_stays_after_filter():
    table = hl.Table.parallelize([{'keep': False, 'step': 0}, {'keep': True, 'step': 1}])
    selected = table.filter(table.keep)
    result = selected.annotate(values=hl.range(0, 3, selected.step))
    assert [row['values'] for row in result.collect()] == [[0, 1, 2]]
    with pytest.raises(RuntimeError, match='step|zero'):
        table.annotate(values=hl.range(0, 3, table.step)).collect()


def test_fold_state_retains_nested_collection_metadata_across_updates_and_merge():
    table = hl.range_table(7, n_partitions=3)
    state = hl.agg.fold(
        hl.empty_set(hl.tint32),
        lambda previous: previous.add(table.idx),
        lambda left, right: left.union(right),
    )
    assert table.aggregate(state) == set(range(7))



def test_scalar_collection_grouping_preserves_nested_sets():
    values = hl.array([hl.struct(key=0, value=hl.set([1, 2])), hl.struct(key=0, value=hl.set([3]))])
    grouped = hl.group_by(lambda value: value.key, values)
    result = hl.eval(grouped)
    assert result == {0: [hl.Struct(key=0, value={1, 2}), hl.Struct(key=0, value={3})]}
    table = hl.range_table(2).annotate(grouped=grouped)
    assert [row.grouped for row in table.collect()] == [result, result]


def test_shuffle_retains_nested_collection_types():
    values = hl.array([hl.set([1, 2]), hl.set([3]), hl.empty_set(hl.tint32)])
    shuffled = hl.shuffle(values, seed=918)
    result = hl.eval(shuffled)
    assert all(isinstance(value, set) for value in result)
    assert {frozenset(value) for value in result} == {frozenset([1, 2]), frozenset([3]), frozenset()}
    assert hl.eval(shuffled) == result


def test_nested_randomness_and_selected_errors_preserve_behavior():
    values = hl.range(0, 12).map(lambda index: hl.rand_unif(seed=405) + index)
    result = hl.eval(values)
    assert hl.eval(values) == result
    assert all(index <= value < index + 1 for index, value in enumerate(result))
    selected = hl.array(['[', '^a']).filter(lambda pattern: pattern != '[')
    assert hl.eval(selected.map(lambda pattern: hl.literal('abc').matches(pattern))) == [True]


def test_configured_aggregates_preserve_distinct_results_across_groups_and_partitions():
    table = hl.range_table(18, n_partitions=3)
    result = table.group_by(group=table.idx % 3).aggregate(
        small=hl.agg.take(table.idx, 2, ordering=table.idx),
        large=hl.agg.take(table.idx, 4, ordering=-table.idx),
    )
    assert {row.group: (row.small, row.large) for row in result.collect()} == {
        group: ([group, group + 3], [group + 15, group + 12, group + 9, group + 6])
        for group in range(3)
    }


def test_actual_table_planning_accepts_depth64_shared_graph():
    table = hl.range_table(2)
    expression = hl.int64(table.idx)
    for _ in range(64):
        expression = expression + expression
    planned = table.annotate(value=expression)
    assert planned.value.dtype == hl.tint64
    # Annotate constructs and validates the actual table/schema plan. The
    # runner applies an external resource bound without evaluating huge values.
    small = hl.int64(table.idx)
    for _ in range(8):
        small = small + small
    assert [row.value for row in table.annotate(value=small).collect()] == [0, 256]


def test_scan_adapters_preserve_independent_filtered_and_grouped_results():
    table = hl.range_table(17, n_partitions=4)
    result = table.annotate(
        first=hl.scan.sum(table.idx),
        repeated=hl.scan.sum(table.idx),
        shifted=hl.scan.sum(table.idx + 1),
        selected=hl.scan.filter(table.idx % 2 == 0, hl.scan.sum(table.idx)),
        grouped=hl.scan.group_by(table.idx % 3, hl.scan.sum(table.idx)),
    )
    for row in result.collect():
        prior = list(range(row.idx))
        assert row.first == row.repeated == sum(prior)
        assert row.shifted == sum(value + 1 for value in prior)
        assert row.selected == sum(value for value in prior if value % 2 == 0)
        assert row.grouped == {group: sum(value for value in prior if value % 3 == group)
                               for group in {value % 3 for value in prior}}


def test_lambda_projection_rebinds_heterogeneous_columns_and_nested_variables():
    table = hl.Table.parallelize([{'unused': [9, 8], 'target': 1}, {'unused': [], 'target': 2}],
                                 schema=hl.tstruct(unused=hl.tarray(hl.tint32), target=hl.tint32))
    result = table.annotate(
        mapped=hl.range(3).map(lambda item: hl.or_missing(item == table.target, item)),
        nested=hl.range(2).map(lambda outer: hl.range(3).map(lambda inner: inner + outer + table.target)),
    )
    rows = result.collect()
    for row in rows:
        assert row.mapped == [item if item == row.target else None for item in range(3)]
        assert row.nested == [[inner + outer + row.target for inner in range(3)] for outer in range(2)]


def test_scalar_captures_preserve_nested_values_in_scalar_and_mixed_callbacks():
    captured = hl.literal(list(range(4096)))
    scalar = hl.range(4).map(lambda index: hl.struct(value=captured[index], tags=hl.set([index])))
    assert hl.eval(scalar) == [hl.Struct(value=index, tags={index}) for index in range(4)]
    table = hl.range_table(5)
    mixed = table.annotate(values=hl.range(3).map(lambda offset: captured[table.idx + offset]))
    assert [row['values'] for row in mixed.collect()] == [list(range(index, index + 3)) for index in range(5)]
    empty = table.filter(False)
    invalid = hl.range(2).map(lambda index: hl.literal('abc').matches('[') | (index == empty.idx))
    assert empty.annotate(values=invalid).collect() == []


def test_native_random_binding_preserves_callback_and_aggregate_scopes():
    table = hl.range_table(12, n_partitions=3)
    shared = hl.rand_unif(seed=124)
    result = table.annotate(values=hl.range(3).map(lambda index: shared + index), same=shared)
    rows = result.collect()
    assert rows == result.collect()
    assert all(all(index <= value < index + 1 for index, value in enumerate(row['values'])) for row in rows)
    assert len({row.same for row in rows}) == 12
    grouped = table.group_by(group=table.idx % 3).aggregate(values=hl.agg.collect(shared))
    expected = {group: sorted(row.same for row in rows if row.idx % 3 == group) for group in range(3)}
    assert {row.group: sorted(row['values']) for row in grouped.collect()} == expected
    assert {row.group: sorted(row['values']) for row in grouped.collect()} == expected
    expression = shared
    for _ in range(64):
        expression = expression + expression
    assert table.annotate(value=expression).value.dtype == hl.tfloat64


def test_nullable_random_condition_preserves_selected_branch_errors():
    table = hl.range_table(12)
    condition = hl.or_missing(table.idx != 0, hl.rand_bool(.5, seed=63))
    result = table.annotate(value=hl.if_else(condition, table.idx, -table.idx))
    rows = result.collect()
    assert rows == result.collect()
    assert rows[0].value is None
    assert all(abs(row.value) == row.idx for row in rows[1:])
    invalid = hl.literal('abc').matches('[')
    assert hl.eval(hl.if_else(hl.missing(hl.tbool), invalid, invalid)) is None
