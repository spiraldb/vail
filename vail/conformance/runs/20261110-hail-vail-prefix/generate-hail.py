from pathlib import Path
import json
import hail as h
r=Path(__file__).parent
h.init(master='local[4]',quiet=True,log=str(r/'generation-run/hail.log'),tmp_dir=str(r/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.default.parallelism':'4','spark.sql.shuffle.partitions':'4','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
try:
 m=h.import_vcf(str(r/'first-1000.vcf'),reference_genome='GRCh37',min_partitions=4)
 m=m.repartition(4,shuffle=False)
 assert m.count()==(1000,2535);assert m.n_partitions()==4
 (r/'inputs').mkdir(exist_ok=True)
 m.write(str(r/'inputs/hail.mt'))
 (r/'generation.json').write_text(json.dumps({'rows':1000,'columns':2535,'partitions':4,'method':'Hail import of first1000retainedVCFrows, repartition4 without shuffle, native write; fresh layout, public logical-prefix equality required before timing'},indent=2)+'\n')
 print('GENERATED',flush=True)
finally:h.stop()
