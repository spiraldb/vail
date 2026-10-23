import vail as hl
for repetition in range(4):
    t=hl.utils.range_table(100000,n_partitions=32)
    t=t.annotate(xs=hl.range(100).map(lambda i:t.idx+i))
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.approx_quantiles(x,[0.,.5,1.],k=128),t.xs))
    assert len(result)==100
    for i,qs in enumerate(result):
        assert i<=qs[0]<i+10000 and 90000+i<qs[2]<=99999+i and abs(qs[1]-(50000+i))<10000
    print(repetition,result[:1])
