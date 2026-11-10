from pathlib import Path
import hashlib,json,os
from owned import run
r=Path(__file__).parent;assert json.loads((r/'complete.json').read_text())['all_groups_absent'];p=json.loads((r/'pins.json').read_text());rows=[]
for mode in ['vail','hail']:
 e={k:v for k,v in os.environ.items() if not k.startswith('VAIL_') and k!='PYTHONPATH'};e.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=p['hail_python'],PYSPARK_DRIVER_PYTHON=p['hail_python'],RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1');out=r/('display-values-'+mode)
 rec=run([p[mode+'_python'],str(r/'display-values.py'),mode,str(out)],out,e,180);rows.append({'mode':mode,'exit':rec})
a=r/'display-values-vail/values.json';b=r/'display-values-hail/values.json';assert a.read_bytes()==b.read_bytes()
(r/'display-validation.json').write_text(json.dumps({'rows':100,'columns':10,'entries':1000,'exact_logical_values_equal':True,'values_sha256':hashlib.sha256(a.read_bytes()).hexdigest(),'workers':rows,'scope':'After all timing, separate public validation of the100x10matrix displayed cells. No renderer byte equality or full dataset claim.'},indent=2)+'\n');print('DISPLAY VALUES MATCH',flush=True)
