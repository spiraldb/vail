"""Partitioned Table joins over interval-derived key ranges."""

from collections import Counter

import vail as hl


def _partitioned(rows, schema, *, key, partitions):
    """Create genuinely range-partitioned keyed data with uneven partition bounds."""
    return (
        hl.Table.parallelize(rows, schema=schema, key=key, n_partitions=partitions)
        .repartition(partitions, shuffle=True)
    )


def _row_tuples(table):
    return [(row.k, row.lv, row.rv) for row in table.collect()]


def _assert_rows(actual, expected):
    assert Counter(actual) == Counter(expected)


def test_interval_partition_joins_all_outer_modes_match_across_different_bounds():
    left_schema = hl.tstruct(k=hl.tint32, lv=hl.tstr)
    right_schema = hl.tstruct(rk=hl.tint32, rv=hl.tstr)
    left = _partitioned(
        [
            {"k": k, "lv": value}
            for k, value in [
                (0, "l0"), (2, "l2"), (4, "l4a"), (4, "l4b"),
                (7, "l7"), (10, "l10"), (14, "l14"), (None, "lnull"),
            ]
        ],
        left_schema,
        key="k",
        partitions=4,
    )
    right = _partitioned(
        [
            {"rk": k, "rv": value}
            for k, value in [
                (1, "r1"), (4, "r4a"), (4, "r4b"), (5, "r5"),
                (7, "r7"), (11, "r11"), (15, "r15"), (None, "rnull"),
            ]
        ],
        right_schema,
        key="rk",
        partitions=3,
    )

    # Matching keys sit at different range boundaries on each input. Duplicate
    # key rows must form a Cartesian product exactly once across the partitions.
    inner = left.join(right, "inner")
    expected_matches = [
        (4, "l4a", "r4a"), (4, "l4a", "r4b"),
        (4, "l4b", "r4a"), (4, "l4b", "r4b"),
        (7, "l7", "r7"),
    ]
    _assert_rows(_row_tuples(inner), expected_matches)

    _assert_rows(_row_tuples(left.join(right, "left")),
        expected_matches
        + [
            (0, "l0", None), (2, "l2", None),
            (10, "l10", None), (14, "l14", None), (None, "lnull", None),
        ]
    )
    _assert_rows(_row_tuples(left.join(right, "right")),
        expected_matches
        + [
            (1, None, "r1"), (5, None, "r5"),
            (11, None, "r11"), (15, None, "r15"), (None, None, "rnull"),
        ]
    )
    outer = left.join(right, "outer")
    outer_rows = _row_tuples(outer)
    expected_outer = expected_matches + [
        (0, "l0", None), (2, "l2", None),
        (10, "l10", None), (14, "l14", None),
        (None, "lnull", None),
        (1, None, "r1"), (5, None, "r5"),
        (11, None, "r11"), (15, None, "r15"), (None, None, "rnull"),
    ]
    _assert_rows(outer_rows, expected_outer)
    assert [row[0] for row in outer_rows] == [
        0, 1, 2, 4, 4, 4, 4, 5, 7, 10, 11, 14, 15, None, None,
    ]
    # In particular, unmatched rows from disjoint range cells are emitted once.
    assert len(outer_rows) == len(expected_outer)


def test_interval_partition_join_prefix_keeps_suffix_keys_and_filters_each_side():
    left = _partitioned(
        [
            {"a": a, "b": b, "lv": value}
            for a, b, value in [
                (1, "a", "la"), (1, "b", "lb"), (2, "a", "lc"),
                (3, "z", "ld"), (5, "q", "le"), (6, "m", "lf"),
                (7, "n", "lg"), (8, "o", "lh"),
            ]
        ],
        hl.tstruct(a=hl.tint32, b=hl.tstr, lv=hl.tstr),
        key=["a", "b"],
        partitions=6,
    )
    right = _partitioned(
        [{"x": x, "suffix": suffix, "rv": value} for x, suffix, value in [
            (1, "r1", "ra"), (1, "r2", "rb"),
            (3, "r3", "rc"), (4, "r4", "rd"),
            (5, "r5", "re"), (7, "r7", "rf"), (8, "r8", "rg"),
        ]],
        hl.tstruct(x=hl.tint32, suffix=hl.tstr, rv=hl.tstr),
        key=["x", "suffix"],
        partitions=6,
    )
    # Each side's filter executes over its own partitioning before the join.
    left = left.filter(left.a != 2)
    right = right.filter(right.x != 4)

    joined = left.join(right, _join_key=1)
    assert list(joined.key) == ["a", "b", "suffix"]
    assert list(joined.row) == ["a", "b", "lv", "suffix", "rv"]
    rows = joined.collect()
    assert [(r.a, r.b, r.lv, r.suffix, r.rv) for r in rows] == [
        (1, "a", "la", "r1", "ra"), (1, "a", "la", "r2", "rb"),
        (1, "b", "lb", "r1", "ra"), (1, "b", "lb", "r2", "rb"),
        (3, "z", "ld", "r3", "rc"), (5, "q", "le", "r5", "re"),
        (7, "n", "lg", "r7", "rf"), (8, "o", "lh", "r8", "rg"),
    ]
    assert len(rows) == 8


def test_interval_partition_join_reads_stored_multifile_inputs(tmp_path):
    left = _partitioned(
        [{"k": k, "lv": f"l{k}"} for k in [0, 2, 4, 6, 8, 10]],
        hl.tstruct(k=hl.tint32, lv=hl.tstr),
        key="k",
        partitions=4,
    )
    right = _partitioned(
        [{"rk": k, "rv": f"r{k}"} for k in [1, 2, 5, 6, 9, 10]],
        hl.tstruct(rk=hl.tint32, rv=hl.tstr),
        key="rk",
        partitions=3,
    )
    left_path = tmp_path / "left"
    right_path = tmp_path / "right"
    left.write(left_path)
    right.write(right_path)
    assert len(list(left_path.rglob("*.vortex"))) > 1
    assert len(list(right_path.rglob("*.vortex"))) > 1

    stored_left = hl.read_table(left_path)
    stored_right = hl.read_table(right_path)
    joined = stored_left.join(stored_right, "outer")
    _assert_rows(_row_tuples(joined), [
        (2, "l2", "r2"), (6, "l6", "r6"), (10, "l10", "r10"),
        (0, "l0", None), (4, "l4", None), (8, "l8", None),
        (1, None, "r1"), (5, None, "r5"), (9, None, "r9"),
    ])


def test_interval_partition_join_reads_only_files_needed_for_selected_output(tmp_path):
    left = _partitioned(
        [{"k": k, "lv": f"l{k}"} for k in range(24)],
        hl.tstruct(k=hl.tint32, lv=hl.tstr),
        key="k",
        partitions=6,
    )
    right = _partitioned(
        [{"rk": k, "rv": f"r{k}"} for k in range(24)],
        hl.tstruct(rk=hl.tint32, rv=hl.tstr),
        key="rk",
        partitions=6,
    )
    left_path = tmp_path / "selective-left"
    right_path = tmp_path / "selective-right"
    left.write(left_path)
    right.write(right_path)

    # read_table captures the partition metadata while the files still exist.
    stored_left = hl.read_table(left_path)
    stored_right = hl.read_table(right_path)
    joined = stored_left.join(stored_right)
    expected_first_partition = _row_tuples(joined._filter_partitions([0]))
    assert expected_first_partition

    left_files = sorted(left_path.rglob("*.vortex"))
    right_files = sorted(right_path.rglob("*.vortex"))
    assert len(left_files) > 1
    assert len(right_files) > 1
    for data_file in left_files[1:] + right_files[1:]:
        data_file.unlink()

    # This must plan source partition membership before touching distant files.
    selected = joined._filter_partitions([0])
    _assert_rows(_row_tuples(selected), expected_first_partition)


def test_multi_way_zip_join_has_independent_interval_partitions():
    first = hl.range_table(12, n_partitions=3)
    second = hl.range_table(12, n_partitions=4)
    third = hl.range_table(12, n_partitions=2)
    second = second.filter(second.idx % 2 == 0)
    third = third.filter(third.idx >= 8)
    joined = hl.Table.multi_way_zip_join([first, second, third], "data", "g")
    rows = joined.collect()
    assert [row.idx for row in rows] == list(range(12))
    for row in rows:
        assert [value is not None for value in row.data] == [True, row.idx % 2 == 0, row.idx >= 8]
    assert "HashJoinExec" not in joined._native.physical_plan()


def test_empty_interval_joins_keep_an_executable_empty_result(tmp_path):
    empty = hl.Table.parallelize([], schema=hl.tstruct(k=hl.tint32), key="k")
    empty.write(tmp_path / "empty")
    empty = hl.read_table(tmp_path / "empty")
    for how in ["inner", "left", "right", "outer"]:
        joined = empty.join(empty, how)
        assert joined.collect() == []
    zipped = hl.Table.multi_way_zip_join([empty, empty], "data", "g")
    assert zipped.collect() == []


def test_nested_join_after_changing_sort_key_replays_correctly():
    left = hl.Table.parallelize(
        [{"k": i, "c": 3 - i, "a": i} for i in range(4)], key="k", n_partitions=2)
    right = hl.Table.parallelize(
        [{"k": i, "b": i + 10} for i in range(4)], key="k", n_partitions=2)
    third = hl.Table.parallelize(
        [{"c": i, "z": i + 20} for i in range(4)], key="c", n_partitions=2)
    result = left.join(right).key_by("c").join(third).key_by("k")
    expected = [(i, 3 - i, i, i + 10, 23 - i) for i in range(4)]
    for _ in range(2):
        assert [(r.k, r.c, r.a, r.b, r.z) for r in result.collect()] == expected


def test_join_chain_keeps_existing_key_order_without_another_sort():
    result = hl.Table.parallelize(
        [{"k": i, "v0": i} for i in range(4)], key="k", n_partitions=2)
    for index in range(1, 4):
        right = hl.Table.parallelize(
            [{"k": i, f"v{index}": i + index} for i in range(4)], key="k", n_partitions=2)
        result = result.join(right).key_by("k")
    expected = [(i, i, i + 1, i + 2, i + 3) for i in range(4)]
    assert [(r.k, r.v0, r.v1, r.v2, r.v3) for r in result.collect()] == expected


def test_rekeying_to_identical_keys_preserves_sorted_rows():
    joined = hl.range_table(8, n_partitions=2).join(hl.range_table(8, n_partitions=2))
    assert joined.key_by("idx").collect() == joined.collect()
