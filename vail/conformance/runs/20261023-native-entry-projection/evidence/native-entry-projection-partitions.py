import json
import vail as hl
path='/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt'
result={}
for name in ('none','entries','rows','both'):
 m=hl.read_matrix_table(path)
 if name in ('entries','both'): m=m.select_entries('DP')
 if name in ('rows','both'): m=m.select_rows()
 result[name]={'matrix_source_partitions':m.n_partitions(),'entry_table_partitions':m.entries().n_partitions()}
print(json.dumps(result))
