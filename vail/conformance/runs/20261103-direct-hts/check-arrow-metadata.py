import json,pyarrow as pa,vail as hl
from pathlib import Path
r=Path(__file__).parent
mt=hl.read_matrix_table('/Users/danielking/.cache/vail-benchmarks/indexed-20261103/profile-first-200.mt')
q=hl.split_multi_hts(mt)
for mode,name,actual in [('internal','direct-arrow-reference.arrow',q._mir.core.to_arrow()),('public','direct-public-arrow-reference.arrow',q.localize_entries('entries','columns').to_arrow())]:
 with pa.memory_map(str(r/name),'r') as source:expected=pa.ipc.open_file(source).read_all()
 assert actual.equals(expected,check_metadata=True)
 print(json.dumps({'boundary':mode,'check_metadata':True,'equal':True,'rows':actual.num_rows,'baseline_arrow_bytes':expected.nbytes,'direct_arrow_bytes':actual.nbytes,'reference':str(r/name)}),flush=True)
