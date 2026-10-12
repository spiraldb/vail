import vail as hl

for partitions in (1, 2):
    t = hl.range_table(3, partitions).key_by()
    t = t.annotate(ordinal=hl.scan.count())
    target = t.key_by("ordinal")
    rows = t.annotate(match=target[t.ordinal]).collect()
    assert [(r.idx, r.ordinal, r.match.idx) for r in rows] == [(i, i, i) for i in range(3)]
    print(partitions, rows)
