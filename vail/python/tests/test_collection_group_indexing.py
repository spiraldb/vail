"""Native grouping preserves collection order, duplicate policy, and missingness."""

import vail as hl


def test_dictionary_duplicate_resolution_and_missing_keys():
    assert hl.eval(hl.dict(hl.array([(2, 3), (1, 4), (2, 5)]))) == {1: 4, 2: 5}
    table = hl.range_table(3, 2)
    table = table.annotate(
        pairs=hl.array(
            [
                (hl.missing(hl.tint32), table.idx + 1),
                (hl.int32(2), table.idx + 2),
                (hl.missing(hl.tint32), table.idx + 3),
            ]
        )
    )
    assert table.annotate(d=hl.dict(table.pairs)).d.collect() == [
        {None: i + 1, 2: i + 2} for i in range(3)
    ]


def test_grouped_arrays_preserve_input_order_and_missing_collections():
    table = hl.Table.parallelize(
        [
            {"idx": 0, "a": [3, 1, 2, 3, None]},
            {"idx": 1, "a": []},
            {"idx": 2, "a": None},
        ],
        schema=hl.tstruct(idx=hl.tint32, a=hl.tarray(hl.tint32)),
        n_partitions=3,
    )
    result = table.annotate(g=hl.group_by(lambda x: x % 2, table.a))
    assert result.g.collect() == [{0: [2], 1: [3, 1, 3], None: [None]}, {}, None]
    assert hl.eval(hl.group_by(lambda x: x % 2, hl.set([1, 2, 3, 4]))) == {
        0: {2, 4},
        1: {1, 3},
    }


def test_dictionary_grouping_retains_empty_struct_values():
    table = hl.range_table(2)
    table = table.annotate(
        d=hl.dict(
            hl.array(
                [
                    hl.tuple([table.idx, hl.struct()]),
                    hl.tuple([table.idx, hl.missing(hl.tstruct())]),
                ]
            )
        )
    )
    assert table.d.collect() == [{0: hl.Struct()}, {1: hl.Struct()}]


def test_matrix_list_grouping_uses_the_same_key_groups():
    matrix = hl.range_matrix_table(3, 7, 2)
    matrix = matrix.annotate_entries(x=matrix.row_idx * 100 + matrix.col_idx)
    matrix = matrix.filter_entries(matrix.col_idx != 2)
    grouped = matrix.annotate_rows(
        g=hl.agg.group_by(
            matrix.col_idx % 3,
            hl.agg.collect(matrix.x),
        )
    )
    assert grouped.g.collect() == [
        {
            0: [r * 100 + c for c in [0, 3, 6]],
            1: [r * 100 + c for c in [1, 4]],
            2: [r * 100 + 5],
        }
        for r in range(3)
    ]


def test_nested_set_binary_uses_canonical_groups():
    dtype = hl.tset(hl.tstruct(members=hl.tset(hl.tint32)))
    left = hl.literal([{"members": [2, 1]}, {"members": [3]}, None], dtype)
    right = hl.literal([{"members": [1, 2]}, {"members": [4]}, None], dtype)
    from vail.collections import _cast
    # Inspect the physical members without requesting unsupported set ordering.
    def members(expr):
        return sorted(
            (None if row is None else sorted(row.members)
             for row in hl.eval(_cast(expr, hl.tarray(dtype.element_type)))),
            key=lambda value: (value is None, value or []),
        )
    assert members(left.intersection(right)) == [[1, 2], None]
    assert members(left.difference(right)) == [[3]]
    assert members(left.union(right)) == [[1, 2], [3], [4], None]
    assert hl.eval(hl.missing(dtype).union(left)) is None


def test_batched_dictionary_lookup_scalar_and_row_dictionaries():
    table = hl.range_table(6, 2)
    dictionary = hl.literal({0: 'zero', 2: None, None: 'missing'},
                            hl.tdict(hl.tint32, hl.tstr))
    result = table.annotate(
        found=dictionary.contains(table.idx),
        value=dictionary.get(table.idx, 'absent'),
        missing_key=dictionary.get(hl.missing(hl.tint32)),
        own=hl.dict(hl.array([(table.idx, table.idx + 1)])).get(table.idx),
    ).collect()
    assert [row.found for row in result] == [True, False, True, False, False, False]
    assert [row.value for row in result] == ['zero', 'absent', None, 'absent', 'absent', 'absent']
    assert [row.missing_key for row in result] == ['missing'] * 6
    assert [row.own for row in result] == list(range(1, 7))
    assert hl.eval(hl.missing(dictionary.dtype).get(0, 'absent')) is None
    assert hl.eval(hl.empty_dict(hl.tint32, hl.tstr).contains(0)) is False
