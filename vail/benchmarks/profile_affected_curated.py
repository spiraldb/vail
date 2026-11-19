"""Instrument affected curated cases, including sample and variant scaling."""
from __future__ import annotations
import argparse,importlib,json,statistics,time,traceback
from pathlib import Path
from types import SimpleNamespace
from benchmark_telemetry import Measurement
from compare_hail import make_cases
from upstream_benchmark_coverage import _signal_fixture_prefetch
import signal

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--engine',choices=['hail','vail'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--samples',type=int,required=True);p.add_argument('--rows',type=int,required=True);p.add_argument('--vcf-rows',type=int,required=True);p.add_argument('--baseline',action='store_true');args=p.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    hl=importlib.import_module(args.engine)
    if args.engine=='hail':hl.init(master='local[14]',spark_conf={'spark.driver.memory':'16g','spark.executor.memory':'16g','spark.ui.enabled':'false'},quiet=True,log=str(args.output/'hail.log'))
    names=['matrix_force_count_rows','matrix_entry_sum','matrix_filter_rows_columns','vcf_import_entry_sum']
    if args.baseline:names=['table_inner_join','table_foreign_key_lookup','table_write','table_read']+names
    config=SimpleNamespace(cores=14,scale=1.,matrix_rows=args.rows,matrix_columns=args.samples,vcf_samples=args.samples,vcf_rows=args.vcf_rows,engine=args.engine,workdir=args.output,case=names)
    cases=make_cases(hl,config);report={'engine':args.engine,'samples':args.samples,'matrix_rows':args.rows,'vcf_rows':args.vcf_rows,'warmups':1,'iterations':3,'cases':[]}
    def save():(args.output/'results.json').write_text(json.dumps(report,indent=2)+'\n')
    save()
    for name in names:
        query,parameters=cases[name];record={'name':name,'parameters':parameters,'iterations':[],'status':'running'};report['cases'].append(record);save()
        try:
            _signal_fixture_prefetch(signal.SIGSTOP)
            try:query()
            finally:_signal_fixture_prefetch(signal.SIGCONT)
            for i in range(3):
                _signal_fixture_prefetch(signal.SIGSTOP)
                measurement=Measurement(args.output/name/f'iteration-{i}').start()
                start=time.perf_counter()
                try:query()
                finally:
                    seconds=time.perf_counter()-start
                    stats=measurement.stop()
                    _signal_fixture_prefetch(signal.SIGCONT)
                record['iterations'].append({'seconds':seconds,**stats});save()
            record['median_seconds']=statistics.median(r['seconds'] for r in record['iterations']);record['median_cpu_seconds']=statistics.median(r['cpu_seconds'] for r in record['iterations']);record['status']='passed'
        except BaseException as e:record['status']='failed';record['error']=f'{type(e).__name__}: {e}';record['traceback']=traceback.format_exc()
        save();print(name,record['status'],record.get('median_seconds'),flush=True)
    if args.engine=='hail':hl.stop()
