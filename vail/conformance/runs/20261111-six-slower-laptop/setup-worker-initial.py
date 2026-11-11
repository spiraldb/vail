from pathlib import Path
import json,hashlib
import vail as hl,vail._native as native
r=Path(__file__).parent;p=json.loads((r/'pins.json').read_text());assert hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()==p['native_sha256']
source=p['fixtures']['profile']['root'];target=p['fixtures']['native_profile']['root'];mt=hl.MatrixTable.read_legacy(source);assert list(mt.count())==[24885,2535];assert mt.n_partitions()==16;mt.write(target)
mt=hl.read_matrix_table(target);assert list(mt.count())==[24885,2535];assert mt.n_partitions()==16
(r/'setup-result.json').write_text(json.dumps({'passed':True,'source':source,'target':target,'dimensions':[24885,2535],'partitions':16,'native_sha256':p['native_sha256']},indent=2)+'\n');print('FULL NATIVE INPUT READY',flush=True)
