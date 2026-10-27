#!/usr/bin/env python3
"""Durable serial three-mode scan; fixtures/conversion are outside query timing."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import time
import upstream_benchmark_coverage as coverage

MODES = ('hail-hail', 'vail-hail', 'vail-vail')

def atomic(path, value):
    coverage._write_report(path, value)

def run_engine_case(case, mode, timeout, fixture_timeout, data_dir, args):
    engine = 'hail' if mode == 'hail-hail' else 'vail'
    executable = args.hail_python if engine == 'hail' else args.vail_python
    # Launch a clean interpreter instead of changing multiprocessing executable:
    # spawn otherwise copies the coordinator environment's site-packages path.
    with tempfile.TemporaryDirectory(prefix='vail-scan-attempt-') as raw:
        output = Path(raw) / 'result.json'
        command = [executable, str(Path(__file__).with_name('upstream_benchmark_coverage.py')),
                   '--engine', engine, '--case', case['id'], '--timeout', str(timeout),
                   '--fixture-timeout', str(fixture_timeout), '--data-dir', str(data_dir),
                   '--output', str(output)]
        started = time.monotonic()
        completed = subprocess.run(command, check=False)
        if output.is_file():
            report = json.loads(output.read_text())
            if len(report.get('cases', [])) == 1:
                result = dict(report['cases'][0])
                for name in case: result.pop(name, None)
                result['interpreter'] = executable
                return result
        return {'status':'failed','phase':'harness_dispatch','interpreter':executable,
                'error':f'Benchmark interpreter exited{completed.returncode} without one result',
                'elapsed_seconds':time.monotonic()-started}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hail-python', default=sys.executable)
    p.add_argument('--vail-python', default=sys.executable)
    p.add_argument('--data-dir', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--repetitions', type=int, default=3)
    p.add_argument('--timeout', type=float, default=600)
    p.add_argument('--fixture-timeout', type=float, default=3600)
    p.add_argument('--case', action='append')
    p.add_argument('--prepare-only', action='store_true')
    p.add_argument('--skip-prepare', action='store_true')
    p.add_argument('--skip-policy', type=Path)
    p.add_argument('--shard-index', type=int, default=0)
    p.add_argument('--shards', type=int, default=1)
    a=p.parse_args()
    if a.shards < 1 or not 0 <= a.shard_index < a.shards:
        p.error('invalid shard')
    os.environ.setdefault('VAIL_BENCHMARK_MAX_RSS_GIB','96')
    cases=coverage.inventory()
    priorities={"benchmark_table.py":0,"benchmark_matrix_table.py":1,"benchmark_methods.py":2}
    cases.sort(key=lambda case: (priorities.get(case["module"],3),case["module"],case["id"]))
    if a.case:
        selected=set(a.case)
        missing=selected-{c['id'] for c in cases}
        if missing: p.error('unknown cases: '+repr(sorted(missing)))
        cases=[c for c in cases if c['id'] in selected]
    cases=[c for i,c in enumerate(cases) if i % a.shards == a.shard_index]
    a.output_dir.mkdir(parents=True,exist_ok=True)
    policy_path=a.skip_policy or a.output_dir/"skip-policy.json"
    skip_policy=json.loads(policy_path.read_text()) if policy_path.exists() else {"cases":{}}
    metadata={'schema_version':1,'comparison_scope_change':'User narrowed Hail timings to benchmarks with an available Vail comparison; explicit confirmed-missing-API/harness evidence only; product failures retained',
              'skip_policy':str(policy_path) if policy_path.exists() else None,
              'skip_policy_sha256':hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path.exists() else None,
              'source_revision':os.environ.get('VAIL_SOURCE_REVISION'),
              'upstream':str(coverage.UPSTREAM),'upstream_sha256':{name:hashlib.sha256((coverage.UPSTREAM/name).read_bytes()).hexdigest() for name in (*coverage.SOURCES,'fixtures.py')},
              'runner_sha256':{name:hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() for name in ('run_scan.py','upstream_benchmark_coverage.py','benchmark_telemetry.py')},
              'fixture_policy':'Hail-written shared inputs; Vail native converted from those inputs outside query timing',
              'cache_policy':'OS cache uncontrolled; rotating serial modes; fresh worker per query; no warmup',
              'repeat_policy':'Three actual successful measurements; after first nonpass, later repetitions reuse status only and actual_sample:false',
              'ordering_policy':'table then matrix then methods then other upstream families',
              'interpreters':{'hail':a.hail_python,'vail':a.vail_python},'query_timeout_cap_seconds':a.timeout,'upstream_timeout_overridden':True,'repetitions':a.repetitions,'cases':cases,'started_at':time.time()}
    atomic(a.output_dir/'inventory.json',metadata)
    # Serial preparation prevents conversion and Spark jobs competing with timed queries.
    if not a.skip_prepare:
        os.environ['VAIL_SCAN_PREPARE_ONLY']='1'
        seen=set()
        for case in cases:
            if any(mark["name"] == "skip" for mark in case["upstream_marks"]):
                continue
            key=coverage._fixture_manifest(a.data_dir,case['fixtures'],case['fixture_parameters']).stem
            if key in seen: continue
            seen.add(key)
            for mode in ('hail-hail','vail-vail'):
                receipt=a.output_dir/'preparation'/mode/(key+'.json')
                if receipt.exists() and json.loads(receipt.read_text()).get('status')=='prepared': continue
                os.environ['VAIL_SCAN_MODE']=mode
                os.environ['VAIL_BENCHMARK_TELEMETRY_DIR']=str(a.output_dir/'preparation-telemetry'/mode/key)
                result=run_engine_case(case,mode,a.timeout,a.fixture_timeout,a.data_dir,a)
                atomic(receipt,{'case':case,'mode':mode,**result})
                print('PREP',mode,case['id'],result['status'],flush=True)
        del os.environ['VAIL_SCAN_PREPARE_ONLY']
    if a.prepare_only: return
    results=[]
    first_nonpass={}
    for rep in range(a.repetitions):
        for index,case in enumerate(cases):
            shift=(rep+index)%len(MODES)
            for mode in MODES[shift:]+MODES[:shift]:
                label=case['id'].replace('::','--').replace('/','_')
                receipt=a.output_dir/f'rep-{rep+1:02}'/mode/(label+'.json')
                if receipt.exists():
                    result=json.loads(receipt.read_text())
                elif mode == 'hail-hail' and case['id'] in skip_policy['cases']:
                    result={'case':case,'mode':mode,'repetition':rep+1,
                            'status':'comparison_not_applicable','actual_sample':False,
                            'reason':skip_policy['cases'][case['id']]['reason'],
                            'policy':str(policy_path),'policy_case':case['id']}
                    atomic(receipt,result)
                elif (case['id'],mode) in first_nonpass:
                    original,original_receipt=first_nonpass[(case['id'],mode)]
                    result={'case':case,'mode':mode,'repetition':rep+1,'status':original['status'],
                            'reused_nonpass_status':True,'actual_sample':False,
                            'original_receipt':str(original_receipt)}
                    atomic(receipt,result)
                else:
                    os.environ['VAIL_SCAN_MODE']=mode
                    os.environ['VAIL_BENCHMARK_TELEMETRY_DIR']=str(a.output_dir/f'rep-{rep+1:02}'/'telemetry'/mode)
                    timeout=a.timeout
                    started=time.time()
                    result={'case':case,'mode':mode,'repetition':rep+1,'started_at':started,'actual_sample':True,
                            'format_applicability':'shared-table-matrix-fixture' if any(n.endswith(('_ht','_mt')) or n=='gnomad_dp_sim' for n in case['fixtures']) else 'format-independent',
                            **run_engine_case(case,mode,timeout,a.fixture_timeout,a.data_dir,a)}
                    atomic(receipt,result)
                if result['status'] != 'passed' and (case['id'],mode) not in first_nonpass:
                    first_nonpass[(case['id'],mode)]=(result,receipt)
                results.append(result)
                totals={}
                for r in results: totals[r['status']]=totals.get(r['status'],0)+1
                atomic(a.output_dir/'results.json',{'metadata':metadata,'totals':totals,'results':results})
                print('QUERY',rep+1,mode,case['id'],result['status'],result.get('benchmark_seconds'),flush=True)
if __name__=='__main__': main()
