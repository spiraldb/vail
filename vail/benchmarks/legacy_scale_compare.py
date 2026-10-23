"""Run sequential paired legacy-reader benchmarks from a scale-fixture manifest.

Example (inherit the supported Hail JAVA_HOME from the shell):
  python benchmarks/legacy_scale_compare.py --data-dir /tmp/legacy-scale \
    --output-dir /tmp/legacy-results --vail-python /path/to/vail/python \
    --hail-python /path/to/hail/python --reject-compilers

Each engine/fixture uses a fresh child process. Interrupted attempts and logs are
retained; --resume reuses only completed, validated records with matching inputs,
engine fingerprints and run configuration. Timings are warm-cache measurements.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import signal
import uuid

TABLE_CASES = ['table_force_count', 'table_scalar_sum']
MATRIX_CASES = ['matrix_decode_count', 'matrix_decode_count_gt', 'matrix_sum_dp']


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    os.replace(temporary, path)


def progress(event, **fields):
    print(json.dumps(dict(timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          event=event, **fields)), flush=True)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fingerprint(python, engine):
    # One import per engine, without initializing Spark or opening a fixture.
    code = '''import hashlib,json,pathlib,platform,sys
import ENGINE as package
result=dict(python=sys.executable,python_version=platform.python_version(),version=getattr(package,'__version__',None))
if ENGINE_NAME == 'vail':
 import vail._native as native
 p=pathlib.Path(native.__file__)
 result.update(native_path=str(p.resolve()),native_sha256=hashlib.sha256(p.read_bytes()).hexdigest())
 h=hashlib.sha256()
 for source in sorted(pathlib.Path(package.__file__).parent.rglob('*.py')):
  h.update(str(source.relative_to(pathlib.Path(package.__file__).parent)).encode());h.update(source.read_bytes())
 result['python_sources_sha256']=h.hexdigest()
print(json.dumps(result))
'''.replace('ENGINE_NAME', repr(engine)).replace('import ENGINE as', 'import ' + engine + ' as')
    result = subprocess.run([python, '-c', code], capture_output=True, text=True,
                            check=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def validate_result(raw, engine, fixture, config):
    identity = fixture['identity']
    cases = TABLE_CASES if identity['kind'] == 'table' else MATRIX_CASES
    for key in ['warmup', 'runs', 'reject_compilers']:
        if raw.get(key) != config[key]:
            raise ValueError(f'{engine}: mismatched {key}')
    if raw.get('engine') != engine or set(raw.get('results', {})) != set(cases):
        raise ValueError(f'{engine}: incorrect engine or cases')
    expected_path = 'table' if identity['kind'] == 'table' else 'matrix'
    if Path(raw[expected_path]).resolve() != Path(fixture['path']).resolve():
        raise ValueError(f'{engine}: incorrect fixture path')
    if engine == 'hail' and raw.get('hail_threads') != config['hail_threads']:
        raise ValueError('incorrect Hail thread count')
    for case in cases:
        result = raw['results'][case]
        if 'error' in result:
            raise ValueError(f'{engine} {case}: {result["error"]}')
        for field in ['seconds', 'engine_cpu_seconds']:
            if len(result.get(field, [])) != config['runs'] or any(not math.isfinite(x) or x <= 0 for x in result[field]):
                raise ValueError(f'{engine} {case}: invalid timing samples')
        expected = None
        if case in ['table_force_count', 'matrix_decode_count', 'matrix_decode_count_gt']:
            expected = identity['rows']
        elif case == 'table_scalar_sum':
            expected = 3 * identity['rows'] * (identity['rows'] - 1) // 2
        if expected is not None and result['value'] != expected:
            raise ValueError(f'{engine} {case}: {result["value"]} != expected {expected}')


def summarize(fixtures, records):
    summary = []
    for fixture_id, fixture in fixtures.items():
        if any(f'{fixture_id}/{engine}' not in records or
               records[f'{fixture_id}/{engine}']['status'] != 'complete'
               for engine in ['hail', 'vail']):
            continue
        by_engine = {engine: json.loads(Path(records[f'{fixture_id}/{engine}']['output']).read_text())['results']
                     for engine in ['hail', 'vail']}
        item = dict(fixture=fixture_id, **fixture['identity'],
                    file_bytes=fixture['file_bytes'],
                    compressed_part_file_bytes=fixture['compressed_part_file_bytes'], cases={})
        for case, hail in by_engine['hail'].items():
            vail = by_engine['vail'][case]
            if hail['value'] != vail['value']:
                raise ValueError(f'{fixture_id} {case}: engines returned different values')
            timings = {}
            for engine, result in [('hail', hail), ('vail', vail)]:
                wall, cpu = result['median'], result['engine_cpu_median']
                timings[engine] = dict(wall_seconds=wall, engine_cpu_seconds=cpu,
                                       average_cores=cpu / wall,
                                       rows_per_second=fixture['identity']['rows'] / wall,
                                       entries_per_second=(fixture['identity']['cells'] / wall
                                                           if fixture['identity']['cells'] is not None else None))
            item['cases'][case] = dict(value=hail['value'], **timings,
                                      wall_speedup_hail_over_vail=hail['median'] / vail['median'],
                                      cpu_speedup_hail_over_vail=hail['engine_cpu_median'] / vail['engine_cpu_median'])
        summary.append(item)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--vail-python', required=True)
    parser.add_argument('--hail-python', required=True)
    parser.add_argument('--only', nargs='+')
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--hail-threads', type=int, default=14)
    parser.add_argument('--reject-compilers', action='store_true')
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    if args.warmup < 0 or args.runs <= 0 or args.hail_threads <= 0:
        parser.error('warmup must be nonnegative; runs and hail-threads must be positive')
    data_dir, output_dir = args.data_dir.expanduser().resolve(), args.output_dir.expanduser().resolve()
    fixtures = json.loads((data_dir / 'manifest.json').read_text())['fixtures']
    if args.only:
        unknown = set(args.only) - fixtures.keys()
        if unknown:
            parser.error('unknown fixture IDs: ' + ', '.join(sorted(unknown)))
        fixtures = {key: value for key, value in fixtures.items() if key in args.only}
    if not fixtures:
        parser.error('no fixtures selected')
    # Retain the manifest order: small tables first, then the matrix size grid.
    fixtures = dict(fixtures)
    for fixture_id, fixture in fixtures.items():
        path = Path(fixture['path']).resolve()
        marker = data_dir / (fixture_id + '.complete.json')
        if not (path / '_SUCCESS').is_file() or json.loads(marker.read_text()) != fixture:
            raise ValueError(f'incomplete or mismatched fixture: {fixture_id}')
        fixture['compressed_part_file_bytes'] = sum(
            p.stat().st_size for p in path.rglob('*')
            if p.is_file() and p.parent.name == 'parts' and not p.name.startswith('.'))
    runner = Path(__file__).with_name('legacy_paired.py').resolve()
    config = dict(warmup=args.warmup, runs=args.runs, hail_threads=args.hail_threads,
                  reject_compilers=args.reject_compilers, runner_sha256=sha(runner),
                  orchestrator_sha256=sha(Path(__file__)),
                  environment={k: os.environ.get(k) for k in ['JAVA_HOME', 'PYTHONPATH', 'SPARK_LOCAL_IP']})
    interpreters = dict(vail=args.vail_python, hail=args.hail_python)
    engines = {engine: fingerprint(python, engine) for engine, python in interpreters.items()}
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / 'results.json'
    if any(output_dir.iterdir()) and not args.resume:
        raise FileExistsError(f'output directory is not empty: {output_dir}; use --resume')
    if state_path.exists():
        state = json.loads(state_path.read_text())
        if state['config'] != config or state['engines'] != engines:
            raise ValueError('resume configuration or engine fingerprint differs')
        for fixture_id, fixture in fixtures.items():
            if fixture_id in state['fixtures'] and state['fixtures'][fixture_id] != fixture:
                raise ValueError(f'resume fixture identity differs: {fixture_id}')
        state['fixtures'].update(fixtures)
    else:
        if any(output_dir.iterdir()):
            raise ValueError('cannot resume an output directory without results.json')
        state = dict(config=config, engines=engines, fixtures=fixtures, records={}, summary=[])
    atomic_json(state_path, state)
    for index, (fixture_id, fixture) in enumerate(fixtures.items()):
        order = ['hail', 'vail'] if index % 2 == 0 else ['vail', 'hail']
        for engine in order:
            key = f'{fixture_id}/{engine}'
            previous = state['records'].get(key)
            if previous and previous['status'] == 'complete':
                if sha(Path(previous['output'])) != previous['output_sha256']:
                    raise ValueError(f'resume result changed: {key}')
                validate_result(json.loads(Path(previous['output']).read_text()), engine, fixture, config)
                progress('reused', fixture=fixture_id, engine=engine)
                continue
            attempt = fixture_id + '-' + engine + '-' + uuid.uuid4().hex[:8]
            output, log = output_dir / (attempt + '.json'), output_dir / (attempt + '.log')
            cases = TABLE_CASES if fixture['identity']['kind'] == 'table' else MATRIX_CASES
            command = [interpreters[engine], str(runner), engine, '--output', str(output),
                       '--warmup', str(args.warmup), '--runs', str(args.runs),
                       '--hail-threads', str(args.hail_threads),
                       '--table', fixture['path'], '--matrix', fixture['path'], '--cases', *cases]
            if args.reject_compilers:
                command.append('--reject-compilers')
            record = dict(status='running', output=str(output), log=str(log), command=command)
            state['records'][key] = record
            atomic_json(state_path, state)
            progress('running', fixture=fixture_id, engine=engine, log=str(log))
            try:
                with log.open('w') as stream:
                    process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                               start_new_session=True)
                    try:
                        status = process.wait(timeout=1200)
                        if status:
                            raise subprocess.CalledProcessError(status, command)
                    except BaseException:
                        # Include Hail's gateway JVM when aborting an attempt.
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                            process.wait(timeout=10)
                        except ProcessLookupError:
                            pass
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                        raise
                validate_result(json.loads(output.read_text()), engine, fixture, config)
                record.update(status='complete', output_sha256=sha(output))
                state['summary'] = summarize(state['fixtures'], state['records'])
                atomic_json(state_path, state)
                progress('complete', fixture=fixture_id, engine=engine)
            except BaseException as error:
                record.update(status='failed', error=repr(error))
                atomic_json(state_path, state)
                progress('failed', fixture=fixture_id, engine=engine, error=repr(error), log=str(log))
                raise
    state['summary'] = summarize(state['fixtures'], state['records'])
    atomic_json(state_path, state)
    progress('finished', results=str(state_path))


if __name__ == '__main__':
    main()
