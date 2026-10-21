import json, tempfile, os
import hail as hl
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.schema import parse
with tempfile.TemporaryDirectory() as folder:
 p=os.path.join(folder,'rows.avro')
 with DataFileWriter(open(p,'wb'),DatumWriter(),parse('{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}')) as w:
  for i in range(10):w.append({'x':i})
 hl.init(master='local[2]',quiet=True,log='/tmp/vail-avro-interval-oracle.log')
 try:
  t=hl.import_avro([p],key=['x'],intervals=[hl.Interval(3,6,point_type=hl.tint64)])
  print(json.dumps({'count':t.count(),'rows':[dict(r) for r in t.collect()]}))
 finally:hl.stop()
