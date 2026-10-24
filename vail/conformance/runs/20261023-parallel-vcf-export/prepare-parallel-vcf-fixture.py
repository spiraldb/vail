import os, pathlib, subprocess, sys
b=pathlib.Path('/home/danielking/parallel-vcf-export'); d=b/'data';d.mkdir(exist_ok=True)
if sys.argv[1]=='hail':
 import hail as hl
 path=d/'profile.vcf.bgz'
 if not path.exists(): subprocess.run(['curl','-Lfs','https://storage.googleapis.com/hail-common/benchmark/profile.vcf.bgz','-o',str(path)],check=True)
 hl.init(master='local[32]',quiet=True,log=str(b/'results/fixture-hail.log'),spark_conf={'spark.driver.memory':'32g','spark.executor.memory':'32g','spark.ui.enabled':'false'})
 m=hl.import_vcf(str(path),min_partitions=16)
 m.write(str(d/'profile.mt'),overwrite=True)
 print(m.count(),flush=True)
 hl.stop()
else:
 import vail as hl
 m=hl.MatrixTable.read_legacy(d/'profile.mt');m.write(d/'profile-native.mt',overwrite=True)
 print(m.count(),flush=True)
