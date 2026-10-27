#!/usr/bin/env python3
"""Frozen curated suite plus shared-source three-format legacy scaling.

Preparation and conversion run serially before measurement. Each measurement
job gets a fresh engine process, one warmup, three measured iterations, durable
per-iteration telemetry, and no competing benchmark jobs on this host.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
MODES = ('hail-hail', 'vail-hail', 'vail-vail')
CURATED = ('table_range_force_count', 'table_key_by_shuffle', 'table_group_sorted',
 'table_group_unsorted', 'table_annotate_100_fields', 'table_stats', 'table_inner_join',
 'table_foreign_key_lookup', 'table_scan_sum', 'table_write', 'table_read',
 'matrix_force_count_rows', 'matrix_entry_sum', 'matrix_filter_rows_columns',
 'vcf_import_entry_sum')
MATRIX_CASES = ('matrix_decode_count', 'matrix_decode_count_gt', 'matrix_entries_count', 'matrix_sum_dp')
TABLE_CASES = ('table_force_count', 'table_scalar_sum')
DEFAULT_SHAPES = ('25000x2500', '100000x2500', '25000x10000', '100000x10000')


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    temporary.replace(path)


def initialize(engine, cores, workdir):
    hl = importlib.import_module(engine)
    if engine == 'hail':
        hl.init(master=f'local[{cores}]', quiet=True, log=str(workdir / 'hail.log'),
                spark_conf={'spark.driver.memory': os.environ.get('HAIL_BENCHMARK_MEMORY', '64g'),
                            'spark.executor.memory': os.environ.get('HAIL_BENCHMARK_MEMORY', '64g'),
                            'spark.driver.maxResultSize': '8g', 'spark.ui.enabled': 'false',
                            'spark.sql.shuffle.partitions': str(cores)})
    return hl


def schema(data, kind):
    attrs = ('row', 'key', 'globals') if kind == 'table' else ('row', 'row_key', 'col', 'col_key', 'entry', 'globals')
    return {attr: str(getattr(data, attr).dtype) for attr in attrs}


def preparation_job(job, hl):
    """Reuse frozen Hail matrix generator; write only owned fresh paths."""
    target = Path(job['target'])
    marker = Path(job['receipt'])
    if marker.exists():
        receipt = json.loads(marker.read_text())
        expected = '_SUCCESS' if job['engine'] == 'hail' else f"_vail_{job['kind']}.json"
        if receipt.get('status') == 'prepared' and (target / expected).is_file():
            return receipt
        raise RuntimeError(f'incomplete preparation receipt: {marker}')
    if target.exists():
        raise FileExistsError(f'unmarked owned fixture exists: {target}; inspect before retry')
    target.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    if job['operation'] == 'convert':
        cls = hl.Table if job['kind'] == 'table' else hl.MatrixTable
        data = cls.read_legacy(job['source'])
    elif job['kind'] == 'matrix':
        from legacy_scale_fixtures import matrix_fixture
        data = matrix_fixture(hl, job['rows'], job['columns'], job['cores'])
    else:
        data = hl.utils.range_table(job['rows'], n_partitions=job['cores'])
        if job['id'] == 'curated_table':
            data = data.annotate(x=data.idx * 7)
        else:
            data = data.annotate(x=hl.int64(data.idx) * 3, s=hl.str(data.idx),
                                 a=hl.range(8).map(lambda i: data.idx + i))
    input_schema = schema(data, job['kind'])
    data.write(str(target), overwrite=False)
    receipt = dict(status='prepared', id=job['id'], engine=job['engine'], kind=job['kind'],
                   source=job.get('source'), path=str(target), rows=job['rows'],
                   columns=job.get('columns'), schema=input_schema,
                   seconds=time.perf_counter()-started,
                   file_bytes=sum(f.stat().st_size for f in target.rglob('*') if f.is_file()))
    atomic(marker, receipt)
    return receipt


def query(job, hl, workdir):
    if job['family'] == 'curated' and job['case'] != 'table_read':
        from compare_hail import make_cases
        config = SimpleNamespace(cores=job['cores'], scale=job['scale'],
              matrix_rows=job['curated_matrix_rows'], matrix_columns=job['curated_matrix_columns'],
              vcf_samples=job['curated_matrix_columns'], vcf_rows=job['vcf_rows'],
              case=[job['case']], engine=job['engine'], workdir=workdir)
        fn, params = make_cases(hl, config)[job['case']]
        return fn, params
    read_table = hl.read_table if job['mode'] != 'vail-hail' else hl.Table.read_legacy
    read_matrix = hl.read_matrix_table if job['mode'] != 'vail-hail' else hl.MatrixTable.read_legacy
    case, path = job['case'], job['path']
    def run():
        if job['kind'] == 'table':
            table = read_table(path)
            value = table._force_count() if case in ('table_force_count', 'table_read') else table.aggregate(hl.agg.sum(table.x))
            expected = job['rows'] if case != 'table_scalar_sum' else 3*job['rows']*(job['rows']-1)//2
            assert value == expected, (case, value, expected)
            return value
        matrix = read_matrix(path)
        if case == 'matrix_decode_count_gt':
            value = matrix.select_entries('GT')._force_count_rows()
        elif case == 'matrix_decode_count':
            value = matrix._force_count_rows()
        elif case == 'matrix_entries_count':
            value = matrix.entries()._force_count()
        else:
            value = matrix.aggregate_entries(hl.agg.sum(matrix.DP))
        if case in ('matrix_decode_count', 'matrix_decode_count_gt'):
            assert value == job['rows'], (case, value, job['rows'])
        elif case == 'matrix_entries_count':
            # Frozen generator defines every entry struct (fields may be null).
            assert value == job['rows'] * job['columns'], (case, value, job['rows'] * job['columns'])
        return value
    return run, {key: job.get(key) for key in ('rows', 'columns', 'kind', 'path', 'fixture')}


def worker(job_path):
    job = json.loads(Path(job_path).read_text())
    output = Path(job['output'])
    workdir = output.parent / (output.stem + '-work')
    workdir.mkdir(parents=True, exist_ok=True)
    hl = None
    result = {'job': job, 'status': 'running', 'iterations': []}
    atomic(output, result)
    try:
        started = time.perf_counter()
        hl = initialize(job['engine'], job['cores'], workdir)
        result['initialization_seconds'] = time.perf_counter()-started
        result['package_path'] = hl.__file__
        result['runtime_version'] = getattr(hl, '__version__', None)
        if job['engine'] == 'vail':
            import vail._native as native
            result['native_sha256'] = hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()
        if job['operation'] != 'measure':
            result.update(preparation_job(job, hl))
            return
        from benchmark_telemetry import Measurement
        fn, result['parameters'] = query(job, hl, workdir)
        values = []
        for index in range(job['warmups'] + job['runs']):
            warmup = index < job['warmups']
            telemetry = Measurement(workdir / f'iteration-{index:02}').start()
            started = time.perf_counter()
            try:
                value = fn()
            finally:
                elapsed = time.perf_counter()-started
                counters = telemetry.stop()
            values.append(value)
            assert all(item == value for item in values), ('unstable query output', values)
            result['iterations'].append(dict(warmup=warmup, seconds=elapsed, value=value,
                                             telemetry=counters, load_average=os.getloadavg()))
            atomic(output, result)
            print(job['id'], index, elapsed, value, flush=True)
        measured = [i for i in result['iterations'] if not i['warmup']]
        result.update(status='passed', value=values[-1],
             median_seconds=statistics.median(i['seconds'] for i in measured),
             median_cpu_seconds=statistics.median(i['telemetry']['cpu_seconds'] for i in measured),
             max_rss_gib=max(i['telemetry']['peak_rss_gib'] for i in measured),
             correctness='frozen inline public result assertions' if job['family']=='curated' and job['case']!='table_read'
                         else 'returned values compared across modes plus count/sum expectations where deterministic')
    except BaseException as error:
        result.update(status='unsupported' if isinstance(error, (NotImplementedError, AttributeError)) else 'failed',
                      error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
    finally:
        atomic(output, result)
        if hl is not None and job['engine']=='hail':
            hl.stop()


def launch(job, args, timeout):
    output = Path(job['output'])
    if output.exists():
        previous = json.loads(output.read_text())
        if previous.get('status') in ('passed', 'prepared') and previous.get('job') == job:
            return previous
    job_path = output.with_suffix('.job.json')
    atomic(job_path, job)
    env = os.environ.copy()
    env.update(RAYON_NUM_THREADS=str(args.cores), OMP_NUM_THREADS=str(args.cores),
               OPENBLAS_NUM_THREADS=str(args.cores), MKL_NUM_THREADS=str(args.cores),
               VAIL_BENCHMARK_MAX_RSS_GIB=str(args.rss_gib))
    python = args.hail_python if job['engine']=='hail' else args.vail_python
    # Keep configured Python paths: VM uses exact source checkout for Vail.
    with output.with_suffix('.log').open('w') as log:
        process = subprocess.Popen([python, str(Path(__file__)), '--worker-job', str(job_path)],
                                   env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            code = -signal.SIGTERM
        finally:
            # Catch a JVM left behind after an os._exit/native crash/RSS guard.
            try: os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError: pass
    result = json.loads(output.read_text()) if output.exists() else {'job':job}
    if result.get('status') == 'running' or code:
        result.update(status='timeout' if code == -signal.SIGTERM else 'worker_error', returncode=code)
        atomic(output, result)
    print(job['id'], result['status'], result.get('median_seconds'), flush=True)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--worker-job')
    p.add_argument('--hail-python', default=sys.executable)
    p.add_argument('--vail-python', default=sys.executable)
    p.add_argument('--output', type=Path)
    p.add_argument('--data-dir', type=Path)
    p.add_argument('--cores', type=int, default=32)
    p.add_argument('--rss-gib', type=int, default=96)
    p.add_argument('--timeout', type=float, default=1200)
    p.add_argument('--fixture-timeout', type=float, default=7200)
    p.add_argument('--runs', type=int, default=3)
    p.add_argument('--warmups', type=int, default=1)
    p.add_argument('--scale', type=float, default=1)
    p.add_argument('--table-rows', type=int, default=100_000_000)
    p.add_argument('--matrix-shapes', nargs='+', default=list(DEFAULT_SHAPES))
    p.add_argument('--curated-matrix-rows', type=int, default=100_000)
    p.add_argument('--curated-matrix-columns', type=int, default=10)
    p.add_argument('--vcf-rows', type=int, default=50_000)
    p.add_argument('--prepare-only', action='store_true')
    p.add_argument('--skip-prepare', action='store_true')
    p.add_argument('--case', action='append')
    p.add_argument('--family', choices=('all', 'curated', 'legacy'), default='all')
    args = p.parse_args()
    if args.worker_job:
        worker(args.worker_job)
        return
    if not args.output or not args.data_dir:
        p.error('--output and --data-dir required')
    args.output, args.data_dir = args.output.resolve(), args.data_dir.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    args.data_dir.mkdir(parents=True, exist_ok=True)
    if args.runs < 1 or args.warmups < 0 or args.cores < 1:
        p.error('invalid repetition/thread setting')
    shapes = [tuple(map(int, shape.lower().split('x'))) for shape in args.matrix_shapes]
    fixtures = [{'id':'curated_table','kind':'table','rows':max(args.cores, int(1_000_000*args.scale))},
                {'id':f'table_{args.table_rows}','kind':'table','rows':args.table_rows}]
    fixtures += [dict(id=f'matrix_{r}x{c}',kind='matrix',rows=r,columns=c) for r,c in shapes]
    base = dict(cores=args.cores, runs=args.runs, warmups=args.warmups, scale=args.scale,
                curated_matrix_rows=args.curated_matrix_rows, curated_matrix_columns=args.curated_matrix_columns,
                vcf_rows=args.vcf_rows)
    jobs=[]
    if args.family in ('all','curated'):
        for case in CURATED:
            modes = MODES if case=='table_read' else ('hail-hail','vail-vail')
            for mode in modes:
                fixture = fixtures[0]
                jobs.append(dict(base, id=f'curated--{case}--{mode}',family='curated',case=case,mode=mode,
                    engine='hail' if mode=='hail-hail' else 'vail',operation='measure',
                    format_applicability='stored-table' if case=='table_read' else
                     ('engine-output-format' if case=='table_write' else 'format-independent'),
                    kind='table',rows=fixture['rows'],fixture=fixture['id'],
                    path=str(args.data_dir/('hail' if mode!='vail-vail' else 'vail-from-hail')/'curated_table.ht')))
    if args.family in ('all','legacy'):
        for fixture in fixtures[1:]:
            cases = TABLE_CASES if fixture['kind']=='table' else MATRIX_CASES
            for case in cases:
                for mode in MODES:
                    suffix = '.ht' if fixture['kind']=='table' else '.mt'
                    path = args.data_dir/('hail' if mode!='vail-vail' else 'vail-from-hail')/(fixture['id']+suffix)
                    jobs.append(dict(base, **{k:v for k,v in fixture.items() if k!='id'},
                         id=f'legacy--{fixture["id"]}--{case}--{mode}', family='legacy',
                         case=case,mode=mode,engine='hail' if mode=='hail-hail' else 'vail',
                         operation='measure',path=str(path),fixture=fixture['id'],
                         format_applicability='shared-stored-fixture'))
    if args.case:
        unknown=set(args.case)-{j['case'] for j in jobs}
        if unknown: p.error('unknown cases '+repr(sorted(unknown)))
        jobs=[j for j in jobs if j['case'] in args.case]
    # Rotate mode order per case while never overlapping work on this host.
    groups={}
    for job in jobs: groups.setdefault((job['family'],job['fixture'],job['case']),[]).append(job)
    jobs=[]
    for i,group in enumerate(groups.values()):
        shift=i%len(group)
        jobs.extend(group[shift:]+group[:shift])
    for job in jobs: job['output']=str(args.output/'queries'/(job['id']+'.json'))
    metadata=dict(source_revision=os.environ.get('VAIL_SOURCE_REVISION'), platform=platform.platform(),
       cpus=os.cpu_count(), settings=vars(args), scripts_sha256={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in HERE.glob('*.py')},
       comparison='same Hail files; native converted outside timing; serial warm-cache1warmup+3measured by default',
       fixtures=fixtures,jobs=jobs)
    atomic(args.output/'inventory.json',metadata)
    if not args.skip_prepare:
        needed={j['fixture'] for j in jobs if j['family']=='legacy' or j['case']=='table_read'}
        for fixture in fixtures:
            if fixture['id'] not in needed: continue
            suffix='.ht' if fixture['kind']=='table' else '.mt'
            for engine in ('hail','vail'):
                folder='hail' if engine=='hail' else 'vail-from-hail'
                prep=dict(fixture,id=fixture['id'],engine=engine,cores=args.cores,
                     operation='prepare' if engine=='hail' else 'convert',
                     target=str(args.data_dir/folder/(fixture['id']+suffix)),
                     source=str(args.data_dir/'hail'/(fixture['id']+suffix)),
                     receipt=str(args.data_dir/folder/(fixture['id']+'.prepared.json')),
                     output=str(args.output/'preparation'/(fixture['id']+'--'+engine+'.json')))
                launch(prep,args,args.fixture_timeout)
    if args.prepare_only: return
    results=[]
    for job in jobs:
        results.append(launch(job,args,args.timeout))
        comparisons=[]
        grouped={}
        for result in results:
            ident=(result['job']['family'],result['job']['fixture'],result['job']['case'])
            grouped.setdefault(ident,[]).append(result)
        for ident,items in grouped.items():
            passed=[r for r in items if r['status']=='passed']
            values=[r.get('value') for r in passed]
            comparisons.append(dict(family=ident[0],fixture=ident[1],case=ident[2],
                modes={r['job']['mode']:r['status'] for r in items},
                output_values_equal=all(value==values[0] for value in values) if values else None,
                substantive_return_values=any(value is not None for value in values)))
        totals={status:sum(r['status']==status for r in results) for status in {r['status'] for r in results}}
        atomic(args.output/'results.json',dict(metadata=metadata,totals=totals,results=results,comparisons=comparisons))

if __name__=='__main__': main()
