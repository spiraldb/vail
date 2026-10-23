import vail as hl
for repetition in range(3):
    t=hl.utils.range_table(100000,n_partitions=32)
    t=t.annotate(xs=hl.range(100).map(lambda i:hl.struct(value=t.idx+i,key=(hl.int64(t.idx)*17+i)%100003)))
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x.value,3,ordering=x.key),t.xs))
    assert len(result)==100
    for i,values in enumerate(result):
        keys=[((v-i)*17+i)%100003 for v in values]
        assert len(values)==3 and keys==sorted(keys) and max(keys)<10
    print(repetition,result[:2])
