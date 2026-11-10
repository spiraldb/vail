from pathlib import Path
import os,json
from owned import run
r=Path(__file__).parent;e={k:v for k,v in os.environ.items() if not k.startswith('VAIL_') and k!='PYTHONPATH'}
e.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=str(r/'hail-env/bin/python'),PYSPARK_DRIVER_PYTHON=str(r/'hail-env/bin/python'),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
print(run([str(r/'hail-env/bin/python'),str(r/'generate-hail.py')],r/'generation-run',e,180),flush=True)
