import math
import vail as hl
for repetition in range(4):
    t=hl.utils.range_table(100000,n_partitions=32)
    t=t.annotate(xs=hl.range(100).map(lambda i:hl.struct(x=hl.float64(t.idx+i),y=hl.float64((t.idx+i)%17))))
    result=t.aggregate(hl.agg.array_agg(lambda pair:hl.agg.corr(pair.x,pair.y),t.xs))
    assert len(result)==100 and all(math.isfinite(x) and abs(x)<=1 for x in result)
    print(repetition,result[:3])
