"""Supplement pytest collection blockers with isolated original-body attempts.

Hail imports/initialization/backend decorators are omitted, as in the existing
Table/Matrix runners. Assertions and parametrize values are unchanged. Missing
setup names are blockers, never passes. No Hail implementation is executed.
"""
import argparse
import ast
import copy
import inspect
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback
from upstream_paths import doctest_data_dir
from concurrent.futures import ThreadPoolExecutor, as_completed


def worker(source, name, output, suite):
    import vail as hl
    import numpy as np
    import pandas as pd
    import pytest
    import math, random, gzip, bz2, operator, unittest, string, re, shutil
    from timeit import default_timer as timer
    from upstream_matrix_cases import BackendDependency
    tree = ast.parse(source.read_text())
    report = dict(module=str(source.relative_to(suite)), name=name, variants=[], setup_errors=[])
    def save():
        output.write_text(json.dumps(report, indent=2))
    with tempfile.TemporaryDirectory(prefix='vail-body-') as folder:
        temporary = Path(folder)
        def new_temp_file(prefix='', extension=None):
            return str(temporary / (prefix + next(tempfile._get_candidate_names()) + ('.' + extension if extension else '')))
        def no_decorator(*args, **kwargs):
            return lambda f: f
        scope = dict(hl=hl, hail=hl, agg=hl.agg, np=np, numpy=np, pd=pd,
                     pytest=pytest, math=math, random=random, gzip=gzip, bz2=bz2,
                     operator=operator, unittest=unittest, string=string, re=re,
                     shutil=shutil, json=json, os=os, Path=Path, timer=timer,
                     new_temp_file=new_temp_file, new_temp_dir=lambda: str(temporary),
                     resource=lambda f: str(suite.parents[2] / 'hail/test/resources' / f),
                     ir=BackendDependency('ir'), Env=BackendDependency('Env'),
                     ExpressionException=hl.ExpressionException,
                     hl_init_for_test=lambda **kw: None, hl_stop_for_test=lambda: None,
                     with_flags=no_decorator, test_timeout=no_decorator,
                     __name__='vail_upstream_bodies', _dataset=None)
        scope['_doctest_dir'] = doctest_data_dir(suite)
        scope.update({n: getattr(hl, n) for n in dir(hl) if n.startswith('t')})
        def compile_node(node, filename):
            node = copy.deepcopy(node)
            outer = [node] + (node.body if isinstance(node, ast.ClassDef) else [])
            for child in outer:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    child.decorator_list = [d for d in child.decorator_list
                                            if isinstance(d, ast.Name) and d.id in ('staticmethod', 'classmethod', 'property')]
            exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                         str(filename), 'exec', flags=__import__('__future__').annotations.compiler_flag), scope)
        helpers = suite / 'helpers.py'
        for node in ast.parse(helpers.read_text()).body:
            if isinstance(node, ast.FunctionDef) and node.name not in scope:
                try:
                    compile_node(node, helpers)
                except Exception:
                    pass
        # Import non-Hail dependencies and bind available public Vail symbols.
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith('hail'):
                if node.module.startswith('hailtop'):
                    continue
                for alias in node.names:
                    if hasattr(hl, alias.name):
                        scope[alias.asname or alias.name] = getattr(hl, alias.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.level:
                    continue
                if isinstance(node, ast.Import) and any(a.name.startswith('hail') for a in node.names):
                    continue
                try:
                    compile_node(node, source)
                except Exception as exc:
                    report['setup_errors'].append(str(exc))
        target_node = None
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                target_node = node
            if isinstance(node, ast.ClassDef) and name.startswith(node.name + '.'):
                target_node = next(m for m in node.body if isinstance(m, ast.FunctionDef) and m.name == name.split('.', 1)[1])
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Assign, ast.AnnAssign)):
                try:
                    compile_node(node, source)
                except Exception as exc:
                    report['setup_errors'].append(f'line {node.lineno}: {type(exc).__name__}: {exc}')
        try:
            if '.' in name:
                cls, method = name.split('.', 1)
                instance = scope[cls](methodName=method) if issubclass(scope[cls], unittest.TestCase) else scope[cls]()
                target = getattr(instance, method)
            else:
                target = scope[name]
            parameters = [{}]
            for decorator in target_node.decorator_list:
                if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute) and decorator.func.attr == 'parametrize':
                    evaluate = lambda node: eval(compile(ast.Expression(node), str(source), 'eval'), scope)
                    names = evaluate(decorator.args[0])
                    names = [n.strip() for n in names.split(',')] if isinstance(names, str) else names
                    variants = []
                    for value in evaluate(decorator.args[1]):
                        if hasattr(value, 'values') and hasattr(value, 'marks'):
                            value = value.values
                        elif len(names) == 1:
                            value = (value,)
                        variants.append(dict(zip(names, value)))
                    parameters = [dict(a, **b) for a, b in itertools.product(parameters, variants)]
        except BaseException as exc:
            report.update(status='blocked', phase='setup_or_parameters', error_type=type(exc).__name__, error=str(exc), traceback=traceback.format_exc())
            save()
            return
        cache = dict(init_hail=None, tmp_path=temporary, tmpdir=temporary)
        def fixture(name):
            if name not in cache:
                factory = scope[name]
                cache[name] = factory(**{p: fixture(p) for p in inspect.signature(factory).parameters})
            return cache[name]
        for params in parameters:
            phase = 'fixture'
            outcome = dict(status='passed', parameters={k: repr(v)[:300] for k,v in params.items()})
            try:
                kwargs = dict(params)
                for key, parameter in inspect.signature(target).parameters.items():
                    if key not in kwargs and parameter.default is inspect.Parameter.empty:
                        kwargs[key] = fixture(key)
                phase = 'body'
                if inspect.iscoroutinefunction(target):
                    import asyncio
                    asyncio.run(target(**kwargs))
                else:
                    target(**kwargs)
            except BaseException as exc:
                outcome.update(status='blocked' if phase == 'fixture' or isinstance(exc, (NameError, KeyError)) else 'failed',
                               phase=phase, error_type=type(exc).__name__, error=str(exc), traceback=traceback.format_exc())
            report['variants'].append(outcome)
            report['status'] = 'passed' if all(v['status']=='passed' for v in report['variants']) else 'failed_or_blocked'
            save()
        if not parameters:
            report['status'] = 'empty_parameterization'
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--worker-source', type=Path)
    parser.add_argument('--worker-name')
    parser.add_argument('--jobs', type=int, default=1)
    args = parser.parse_args()
    if args.worker_name:
        if args.output.exists():
            return
        worker(args.worker_source, args.worker_name, args.output, args.suite)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    from all_upstream_table_cases import definitions
    cases = [(s, name) for s in sorted(args.suite.rglob('test_*.py'))
             if 'backend' not in s.relative_to(args.suite).parts
             if str(s.relative_to(args.suite)) not in ('table/test_table.py', 'matrixtable/test_matrix_table.py')
             for name, node in definitions(ast.parse(s.read_text()))]
    results = {}
    def run(index, source, name):
        path = args.output / f'{index:04d}.json'
        if path.exists():
            return index, json.loads(path.read_text())
        command = [sys.executable, str(Path(__file__).resolve()), '--suite', str(args.suite), '--output', str(path),
                   '--worker-source', str(source), '--worker-name', name]
        with path.with_suffix('.log').open('w') as log:
            try:
                proc = subprocess.run(command, stdout=log, stderr=log, timeout=45)
                problem = f'worker exited {proc.returncode}' if proc.returncode else None
            except subprocess.TimeoutExpired:
                problem = 'worker exceeded 45 seconds'
        result = json.loads(path.read_text()) if path.exists() else dict(module=str(source.relative_to(args.suite)), name=name)
        if problem:
            result.update(status='worker_blocked', worker_error=problem)
        path.write_text(json.dumps(result, indent=2))
        return index, result
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = [pool.submit(run, i, source, name) for i, (source, name) in enumerate(cases)]
        for future in as_completed(futures):
            index, result = future.result()
            results[index] = result
            (args.output / 'results-parallel.json').write_text(json.dumps([results[i] for i in sorted(results)], indent=2))
            print(f'{len(results)}/{len(cases)} {result.get("status")} {result["module"]}:{result["name"]}', flush=True)


if __name__ == '__main__':
    main()
