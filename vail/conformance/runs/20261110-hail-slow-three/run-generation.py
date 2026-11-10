from pathlib import Path
import os,json
from owned import run
r=Path(__file__).parent;p=json.loads((r/'pins-initial.json').read_text());e={k:v for k,v in os.environ.items() if not k.startswith('VAIL_') and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};e.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=p['hail_python'],PYSPARK_DRIVER_PYTHON=p['hail_python'],OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
print(run([p['hail_python'],str(r/'generate.py')],r/'generation-run',e,600),flush=True)
