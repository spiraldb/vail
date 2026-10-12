import vail as hl
import pytest
from vail.types import tint32, tstruct


def _saved_table(path, rows, key, schema=None):
    table = hl.Table.parallelize(rows, schema=schema, key=key)
    table.write(str(path), overwrite=True)
    return str(path)


def test_query_table_scalar_point_prefix_and_range(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": 0, "sub": 0, "value": "a"},
        {"k": 1, "sub": 1, "value": "b"},
        {"k": 2, "sub": 0, "value": "c"},
    ], ["k", "sub"])
    assert hl.eval(hl.query_table(path, 1)) == [hl.Struct(k=1, sub=1, value="b")]
    assert hl.eval(hl.query_table(path, hl.struct(k=1))) == [hl.Struct(k=1, sub=1, value="b")]
    assert hl.eval(hl.query_table(path, hl.struct(other=1))) == [hl.Struct(k=1, sub=1, value="b")]
    assert hl.eval(hl.query_table(path, hl.interval(hl.struct(point=0), hl.struct(point=2)))) == [
        hl.Struct(k=0, sub=0, value="a"), hl.Struct(k=1, sub=1, value="b")
    ]
    assert hl.eval(hl.query_table(path, 9)) == []
    assert hl.eval(hl.query_table(path, hl.missing(tint32))) == []
    compound = _saved_table(tmp_path / "compound.vail", [
        {"k": 1, "sub": 2, "value": "compound"},
    ], ["k", "sub"])
    assert hl.eval(hl.query_table(compound, hl.struct(second=1, first=2))) == [
        hl.Struct(k=1, sub=2, value="compound")
    ]


def test_query_table_duplicate_matches_across_partitions(tmp_path):
    path = hl.Table.parallelize([
        {"k": 1, "value": "first"}, {"k": 1, "value": "second"},
        {"k": 2, "value": "third"},
    ], key="k", n_partitions=2)
    path.write(str(tmp_path / "dups.vail"), overwrite=True)
    matches = hl.eval(hl.query_table(str(tmp_path / "dups.vail"), 1))
    assert matches == [hl.Struct(k=1, value="first"), hl.Struct(k=1, value="second")]


def test_query_table_row_varying_and_global_queries(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": 0, "value": "a"}, {"k": 2, "value": "b"},
    ], "k")
    outer = hl.Table.parallelize([{"q": 0}, {"q": 1}, {"q": 2}], key="q")
    result = outer.annotate(matches=hl.query_table(path, outer.q)).select("matches").collect()
    assert [row.matches for row in result] == [
        [hl.Struct(k=0, value="a")], [], [hl.Struct(k=2, value="b")]
    ]
    global_query = outer.annotate_globals(g=2)
    assert global_query.annotate(matches=hl.query_table(path, global_query.g)).collect()[0].matches == [
        hl.Struct(k=2, value="b")
    ]


def test_query_table_row_varying_range_and_missing(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": 0}, {"k": 1}, {"k": 2}, {"k": 3},
    ], "k")
    outer = hl.Table.parallelize([
        {"start": 0, "end": 2}, {"start": 2, "end": 4},
    ], schema=tstruct(start=tint32, end=tint32))
    query = hl.interval(outer.start, outer.end, includes_end=True)
    got = outer.annotate(matches=hl.query_table(path, query)).select("matches").collect()
    assert [row.matches for row in got] == [
        [hl.Struct(k=0), hl.Struct(k=1), hl.Struct(k=2)],
        [hl.Struct(k=2), hl.Struct(k=3)],
    ]


def test_query_table_missing_range_endpoints(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": None}, {"k": 0}, {"k": 1},
    ], "k")
    assert hl.eval(hl.query_table(path, hl.interval(None, 1, includes_end=True))) == []
    assert hl.eval(hl.query_table(path, hl.interval(1, None, includes_end=True))) == [
        hl.Struct(k=1), hl.Struct(k=None)
    ]
    outer = hl.Table.parallelize([
        {"start": None, "end": 1}, {"start": 1, "end": None},
    ], schema=tstruct(start=tint32, end=tint32))
    query = hl.interval(outer.start, outer.end, includes_end=True)
    results = outer.annotate(matches=hl.query_table(path, query)).select("matches").collect()
    assert [row.matches for row in results] == [[], [hl.Struct(k=1), hl.Struct(k=None)]]


def test_query_table_missing_row_query_and_validation(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [{"k": None}, {"k": 0}, {"k": 1}], "k")
    outer = hl.Table.parallelize([{"q": None}, {"q": 1}], schema=tstruct(q=tint32))
    result = outer.annotate(matches=hl.query_table(path, outer.q)).select("matches").collect()
    assert [row.matches for row in result] == [[hl.Struct(k=None)], [hl.Struct(k=1)]]
    assert hl.eval(hl.query_table(path, hl.missing(tint32))) == [hl.Struct(k=None)]
    assert hl.eval(hl.query_table(path, hl.missing(tstruct(k=tint32)))) == [hl.Struct(k=None)]
    compound = _saved_table(tmp_path / "compound.vail", [
        {"k": None, "sub": 0}, {"k": None, "sub": 1}, {"k": 1, "sub": 0},
    ], ["k", "sub"])
    assert hl.eval(hl.query_table(compound, hl.struct(first=hl.missing(tint32)))) == [
        hl.Struct(k=None, sub=0), hl.Struct(k=None, sub=1)
    ]
    with pytest.raises(ValueError, match="empty struct"):
        hl.query_table(path, hl.struct())
    with pytest.raises(ValueError, match="has 2 field"):
        hl.query_table(path, hl.struct(a=1, b=2))
    with pytest.raises(ValueError, match="key mismatch"):
        hl.query_table(path, "wrong type")
    unkeyed = hl.Table.parallelize([{"k": 0}])
    unkeyed_path = tmp_path / "unkeyed.vail"
    unkeyed.write(str(unkeyed_path), overwrite=True)
    with pytest.raises(ValueError, match="unkeyed"):
        hl.query_table(str(unkeyed_path), 0)


def test_query_table_interval_key_range_uses_key_order(tmp_path):
    path = _saved_table(tmp_path / "interval.vail", [
        {"k": 0, "value": "a"}, {"k": 1, "value": "b"},
    ], "k")
    # The outer interval ranges over interval-valued keys, so it is a range
    # lookup. The exact interval key itself remains a point lookup.
    interval_path = _saved_table(tmp_path / "interval_keys.vail", [
        {"key": hl.Interval(0, 2), "value": "a"},
        {"key": hl.Interval(4, 6), "value": "b"},
    ], "key")
    expected = [hl.Struct(key=hl.Interval(0, 2), value="a")]
    assert hl.eval(hl.query_table(interval_path, hl.interval(hl.Interval(0, 1), hl.Interval(4, 5)))) == expected
    assert hl.eval(hl.query_table(interval_path, hl.Interval(0, 2))) == [
        hl.Struct(key=hl.Interval(0, 2), value="a")
    ]
    outer = hl.Table.parallelize([
        {"start": hl.Interval(0, 1), "end": hl.Interval(4, 5)},
    ])
    result = outer.annotate(matches=hl.query_table(interval_path, hl.interval(outer.start, outer.end)))
    assert result.collect()[0].matches == [hl.Struct(key=hl.Interval(0, 2), value="a")]


def test_query_table_prunes_files_and_remains_lazy(tmp_path):
    import json
    from pathlib import Path
    import pytest

    path = tmp_path / 'source'
    hl.range_table(20, n_partitions=4).write(str(path))
    index_path = next(p for p in path.rglob('*.json')
                      if isinstance((value := json.loads(p.read_text())), dict)
                      and 'fragments' in value)
    fragments = json.loads(index_path.read_text())['fragments']
    assert len(fragments) >= 4
    for fragment in fragments[1:]:
        Path(fragment['identity']['file_path'].removeprefix('file://')).unlink()
    query = hl.query_table(str(path), 1)
    assert hl.eval(query) == [hl.Struct(idx=1)]

    # Constructing the expression must not cache or read the matching data.
    lazy_query = hl.query_table(str(path), 2)
    Path(fragments[0]['identity']['file_path'].removeprefix('file://')).unlink()
    with pytest.raises(FileNotFoundError):
        hl.eval(lazy_query)
