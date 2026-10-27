import json
import vail as h
t=h.utils.range_table(12,n_partitions=3)
t=t.annotate(source_index=t.idx,r=t.idx%2,c=(t.idx//2)%2,value=t.idx,payload=h.str(t.idx)+':'+'x'*3)
t=t.key_by().select('source_index','r','c','value','payload')
t=t.order_by(t.source_index).key_by().select('r','c','value','payload')
m=t.to_matrix_table(['r'],['c']);localized=m.localize_entries('entries','columns')
localized=localized.annotate(__h091_columns=localized.columns)
rows=localized.collect()
assert sorted(row.r for row in rows)==[0,1]
for row in rows:
 assert [c.c for c in row['__h091_columns']]==[0,1]
 assert len(row.entries)==2
 for c,e in enumerate(row.entries):
  n=row.r+2*c+2*4
  assert e.value==n and e.payload==str(n)+':xxx'
print(json.dumps({'status':'PASS','complete_rows':2,'complete_cols':2,'duplicates_per_coordinate':3}))
