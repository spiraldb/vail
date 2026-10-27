#!/usr/bin/env python3
"""Serial Vail-only cost guards; separate from all cross-engine speedup reports."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import runpy
import signal
import statistics
import subprocess
import sys
import time
import traceback

SPECS = {
 'check_allele_classification_cost.py': '16rows; allelelength128,512,2048',
 'check_arrow_runtime_cost.py': '2Mrows/8Arrowbatches; oversizedscalar16Mrows',
 'check_call_adapter_cost.py': '1000,10000phasedcalls',
 'check_conditional_cost.py': '16rows/8192codepoints; switch/case16,64,256branches',
 'check_csv_cost.py': '64,256,1024headerfields;16,256,4096missingmarkers',
 'check_display_cost.py': 'array/nested/string/sentinel4096,65536,1048576;wide64,256,1024',
 'check_expression_input_cost.py': 'mixed128,512,2048;shared4,8,12,16',
 'check_native_boundary_cost.py': 'expression32,64;Arrow;wide64,256;locus64,4096',
 'check_qc_cost.py': 'QC128,1024,8192samples;32,256alleles;HWE100,10000,1000000',
 'check_query_cost.py': 'range/point4shapes;range1,4,16,64parts;literal3sizes;scalar2modes/2sizes',
 'check_random_cost.py': 'DAG8,12,16;callback64,512,2048,8192;cat_shared/cat_rows16,256,4096;shuffle4096,65536,262144;ndarray32,256,512;dirichlet16,256,4096;hyperboundary1000,2B;multi_hyper100,10000',
 'check_reference_genome_cost.py': 'comparison/classification/PAR64,4096contigs;10000queries',
 'check_reference_genome_export_cost.py': '64,256,1024contigs/PARs;extra many-sex-contigs64,256',
 'check_stream_cost.py': 'streamscan1024,4096,16384,65536;3keyedheads2Bsource;wide64,256,1024',
 'check_string_scalar_cost.py': '1000,10000rows;8192sharedscalarcodepoints',
 'check_string_traversal_cost.py': '16rows;128,512,2048codepoints',
 'check_switch_cost.py': '100rows;16,64,256branches',
 'check_variant_cost.py': 'long128,1024,8192,65536;PL8,32,128,256alleles',
 'check_vcf_cost.py': 'export4shapes;rows/DP/all4shapes;expressiondepth1,16,64,256',
}

def atomic(path, value):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(value,indent=2,default=str)+'\n');tmp.replace(path)


def worker(script, output, extras):
 from benchmark_telemetry import Measurement
 import vail as hl
 import vail._native as native
 before=resource.getrusage(resource.RUSAGE_SELF);childbefore=resource.getrusage(resource.RUSAGE_CHILDREN)
 result=dict(script=str(script),native_sha256=hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
             package_path=hl.__file__,source_sha256=hashlib.sha256(script.read_bytes()).hexdigest(),status='running')
 atomic(output,result)
 measurement=Measurement(output.parent/(output.stem+'-telemetry')).start()
 start=time.perf_counter()
 oldargv=sys.argv
 try:
  sys.argv=[str(script),*extras]
  runpy.run_path(str(script),run_name='__main__')
  result['status']='passed'
 except SystemExit as error:
  result.update(status='passed' if error.code in (None,0) else 'failed',script_exit=error.code)
 except BaseException as error:
  result.update(status='failed',error=f'{type(error).__name__}: {error}',traceback=traceback.format_exc())
  if isinstance(error, subprocess.CalledProcessError):
   result.update(child_stdout=error.stdout,child_stderr=error.stderr,child_returncode=error.returncode,child_command=error.cmd)
 finally:
  sys.argv=oldargv
  result['wall_seconds']=time.perf_counter()-start
  result['telemetry']=measurement.stop()
  after=resource.getrusage(resource.RUSAGE_SELF);childafter=resource.getrusage(resource.RUSAGE_CHILDREN)
  result['accounted_cpu_seconds']=(after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime +
       childafter.ru_utime+childafter.ru_stime-childbefore.ru_utime-childbefore.ru_stime)
  result['cpu_accounting']='RUSAGE_SELF plus reaped RUSAGE_CHILDREN, including exited worker descendants; process-tree sample counters retained separately'
  atomic(output,result)


def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--source-root',type=Path)
 p.add_argument('--output',type=Path,required=True)
 p.add_argument('--python',default=sys.executable)
 p.add_argument('--timeout',type=float,default=180)
 p.add_argument('--rss-gib',type=int,default=32)
 p.add_argument('--cores',type=int,default=8)
 p.add_argument('--only',action='append')
 p.add_argument('--guard-worker',type=Path)
 p.add_argument('--extra',action='append',default=[])
 a=p.parse_args();a.output=a.output.resolve()
 if a.guard_worker:
  worker(a.guard_worker.resolve(),a.output,a.extra);return
 if not a.source_root:p.error('--source-root required (frozen repository root)')
 source=(a.source_root/'vail'/'benchmarks').resolve()
 found={path.name for path in source.glob('check_*_cost.py')}
 missing=set(SPECS)-found
 if missing:p.error('missing frozen guards '+repr(sorted(missing)))
 if found-set(SPECS):p.error('new guards require explicit inventory '+repr(sorted(found-set(SPECS))))
 selected=set(a.only) if a.only else set(SPECS)
 if selected-set(SPECS):p.error('unknown guards '+repr(sorted(selected-set(SPECS))))
 a.output.mkdir(parents=True,exist_ok=True)
 jobs=[dict(name=name,id=name.removesuffix('.py'),args=[],scales=SPECS[name]) for name in sorted(selected)]
 if 'check_reference_genome_export_cost.py' in selected:
  jobs.append(dict(name='check_reference_genome_export_cost.py',id='check_reference_genome_export_cost-many-sex-contigs',args=['--many-sex-contigs'],scales='64,256sexcontigs/PARs'))
 for job in jobs:job['source_sha256']=hashlib.sha256((source/job['name']).read_bytes()).hexdigest()
 metadata=dict(source_revision=os.environ.get('VAIL_SOURCE_REVISION'),platform=platform.platform(),
  settings=vars(a),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
  jobs=jobs,guard_scripts=len(selected),scope='Vail-only resource/correctness guards; no cross-engine speedup ratios',
  timeout_policy='180s per guard process by default; internal bounded subprocesses retain own15/20/30s limits; later guards continue')
 atomic(a.output/'inventory.json',metadata)
 results=[]
 for job in jobs:
  receipt=a.output/(job['id']+'.json');log=a.output/(job['id']+'.log')
  if receipt.exists():
   old=json.loads(receipt.read_text())
   if old.get('job')==job and old.get('status')=='passed':results.append(old);continue
  env=dict(os.environ,OMP_NUM_THREADS=str(a.cores),OPENBLAS_NUM_THREADS=str(a.cores),MKL_NUM_THREADS=str(a.cores),
          RAYON_NUM_THREADS=str(a.cores),VAIL_BENCHMARK_MAX_RSS_GIB=str(a.rss_gib))
  cmd=[a.python,str(Path(__file__)),'--guard-worker',str(source/job['name']),'--output',str(receipt)]
  for extra in job['args']:cmd+=['--extra='+extra]
  timedout=False;start=time.perf_counter()
  with log.open('w') as stream:
   process=subprocess.Popen(cmd,env=env,stdout=stream,stderr=stream,start_new_session=True)
   try:code=process.wait(timeout=a.timeout)
   except subprocess.TimeoutExpired:
    timedout=True;os.killpg(process.pid,signal.SIGTERM)
    try:process.wait(timeout=5)
    except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
    code=process.returncode
   finally:
    try:os.killpg(process.pid,signal.SIGTERM)
    except ProcessLookupError:pass
  result=json.loads(receipt.read_text()) if receipt.exists() else {}
  result.update(job=job,elapsed_including_import_seconds=time.perf_counter()-start,returncode=code,log=str(log))
  records=[]
  for line in log.read_text(errors='replace').splitlines():
   try:row=json.loads(line)
   except (ValueError,TypeError):continue
   if isinstance(row,dict):records.append(row)
  result['script_json_records']=records
  result['inner_timeouts']=[r for r in records if r.get('timeout_seconds')]
  result['inner_failed_guards']=[r for r in records if r.get('passed') is False]
  if timedout:result['status']='timeout'
  elif code or result.get('status') not in ('passed','failed'):result['status']='worker_error'
  elif result['status']=='passed' and result['inner_timeouts']:result['status']='partial_timeout'
  elif result['status']=='passed' and result['inner_failed_guards']:result['status']='failed'
  atomic(receipt,result);results.append(result)
  atomic(a.output/'results.json',dict(metadata=metadata,
       totals={s:sum(r['status']==s for r in results) for s in {r['status'] for r in results}},results=results))
  print(job['id'],result['status'],result.get('wall_seconds'),flush=True)

if __name__=='__main__':main()
