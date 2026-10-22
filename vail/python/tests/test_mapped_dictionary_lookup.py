"""Mapped dictionary probes retain per-row dictionaries and lazy defaults."""
import vail as hl


def test_mapped_dictionary_lookup_varied_rows():
    t = hl.Table.parallelize([
        {'d': {1: 10, 2: None, None: 30}, 'q': [1, 2, 9, None, 1], 'fallback': 40},
        {'d': {1: 50}, 'q': [1, 2], 'fallback': 60},
        {'d': {}, 'q': [], 'fallback': 70},
        {'d': None, 'q': [1, None], 'fallback': 80},
        {'d': {1: 1}, 'q': None, 'fallback': 90},
    ], schema=hl.tstruct(d=hl.tdict(hl.tint32, hl.tint32), q=hl.tarray(hl.tint32), fallback=hl.tint32), n_partitions=3)
    got = t.annotate(result=t.q.map(lambda k: t.d.get(k, t.fallback))).result.collect()
    assert got == [[10, None, 40, 30, 10], [50, 60], [], [None, None], None]
    found = t.annotate(result=t.q.map(lambda k: t.d.contains(k))).result.collect()
    assert found == [[True, True, False, True, True], [True, False], [], [None, None], None]


def test_mapped_dictionary_lookup_query_capture_and_missing_default():
    t = hl.range_table(4, 2)
    d = hl.literal({'0': 5, '2': None, '4': 9}, hl.tdict(hl.tstr, hl.tint32))
    t = t.annotate(result=hl.range(3).map(lambda k: d.get(hl.str(t.idx + k))))
    assert t.result.collect() == [[5, None, None], [None, None, None], [None, None, 9], [None, 9, None]]


def test_mapped_dictionary_lookup_default_is_lazy():
    t = hl.range_table(3)
    t = t.annotate(d=hl.dict([(0, t.idx)]))
    result = t.annotate(a=hl.array([0]).map(lambda k: t.d.get(k, hl.int32(1) // t.idx)))
    assert result.a.collect() == [[0], [1], [2]]
