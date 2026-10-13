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
from types import SimpleNamespace
from upstream_paths import doctest_data_dir, resource_path, FixtureModule
from concurrent.futures import ThreadPoolExecutor, as_completed


def worker(source, name, output, suite):
    import vail as hl
    # Original tests catch Hail's user execution error. Native Vail kernels
    # expose execution failures as RuntimeError; retain their message checks.
    if not hasattr(hl.utils, 'HailUserError'):
        hl.utils.HailUserError = RuntimeError
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
        hl = FixtureModule(hl, suite.parents[2] / 'hail/test/resources', temporary / 'fixtures')
        def new_temp_file(prefix='', extension=None):
            return str(temporary / (prefix + next(tempfile._get_candidate_names()) + ('.' + extension if extension else '')))
        def no_decorator(*args, **kwargs):
            return lambda f: f
        scope = dict(hl=hl, hail=hl, agg=hl.agg, np=np, numpy=np, pd=pd,
                     pytest=pytest, math=math, random=random, gzip=gzip, bz2=bz2,
                     operator=operator, unittest=unittest, string=string, re=re,
                     shutil=shutil, json=json, os=os, Path=Path, timer=timer,
                     new_temp_file=new_temp_file, new_temp_dir=lambda: str(temporary),
                     resource=lambda f: resource_path(suite.parents[2] / 'hail/test/resources', f, temporary / 'fixtures'),
                     ir=BackendDependency('ir'), Env=hl.utils.Env,
                     ExpressionException=hl.ExpressionException,
                     hl_init_for_test=lambda **kw: None, hl_stop_for_test=lambda: None,
                     with_flags=no_decorator, test_timeout=no_decorator,
                     __name__='vail_upstream_bodies', _dataset=None)
        scope['_doctest_dir'] = doctest_data_dir(suite)
        from vail.types import is_type
        scope.update({n: getattr(hl, n) for n in dir(hl)
                      if n.startswith('t') and (is_type(getattr(hl, n))
                                               or isinstance(getattr(hl, n), type))})
        def compile_node(node, filename):
            if source.name == 'test_file_formats.py' and isinstance(node, ast.AsyncFunctionDef) and node.name == 'collect_paths':
                # Original fixture enumerates local legacy snapshots through a
                # cloud filesystem router. Preserve its exact files/parameters
                # without requiring cloud SDKs for a local directory listing.
                async def collect_local_paths():
                    directory = Path(scope['resource_dir'])
                    versions = sorted(p for p in directory.iterdir() if p.is_dir())
                    def paths(component):
                        return [str(path) for version in versions
                                if (version / component).exists()
                                for path in sorted((version / component).iterdir())]
                    return paths('table'), paths('matrix_table')
                scope['collect_paths'] = collect_local_paths
                report.setdefault('fixture_adapters', []).append('Local legacy snapshot enumeration: original 56 Table / 60 Matrix paths')
                return
            node = copy.deepcopy(node)
            for imported in ast.walk(node):
                if isinstance(imported, ast.ImportFrom) and imported.module and (imported.module == 'hail' or imported.module.startswith('hail.')):
                    imported.module = 'vail' + imported.module[4:]
                elif isinstance(imported, ast.Import):
                    for alias in imported.names:
                        if alias.name == 'hail' or alias.name.startswith('hail.'):
                            alias.name = 'vail' + alias.name[4:]
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
        # Retain original directory-local fixture bodies (including ht/mt in
        # extract_intervals) instead of substituting synthetic test datasets.
        # Compile only fixtures: Hail initialization and conftest imports are
        # managed by this adapter's existing scope.
        fixture_directories = [suite] + [suite.joinpath(*source.parent.relative_to(suite).parts[:i])
                                         for i in range(1, len(source.parent.relative_to(suite).parts) + 1)]
        fixture_parameters = {}
        for directory in fixture_directories:
            fixture_source = directory / 'conftest.py'
            if not fixture_source.exists():
                continue
            for node in ast.parse(fixture_source.read_text()).body:
                if not isinstance(node, ast.FunctionDef):
                    continue
                if not any((isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
                            and d.func.attr == 'fixture')
                           or (isinstance(d, ast.Attribute) and d.attr == 'fixture')
                           for d in node.decorator_list):
                    continue
                if node.name.startswith('init_hail'):
                    continue
                try:
                    compile_node(node, fixture_source)
                    for decorator in node.decorator_list:
                        if isinstance(decorator, ast.Call):
                            for keyword in decorator.keywords:
                                if keyword.arg == 'params':
                                    fixture_parameters[node.name] = eval(compile(ast.Expression(keyword.value), str(fixture_source), 'eval'), scope)
                except Exception as exc:
                    report['setup_errors'].append(f'{fixture_source}: {node.name}: {exc}')
        # Import non-Hail dependencies and bind available public Vail symbols.
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith('hail'):
                if node.module.startswith('hailtop'):
                    try:
                        import types
                        if 'hailtop' not in sys.modules:
                            package = types.ModuleType('hailtop')
                            package.__path__ = [str(suite.parents[1] / 'hailtop')]
                            sys.modules['hailtop'] = package
                        compile_node(node, source)
                    except Exception as exc:
                        report['setup_errors'].append(str(exc))
                    continue
                for alias in node.names:
                    if hasattr(hl, alias.name):
                        scope[alias.asname or alias.name] = getattr(hl, alias.name)
                    else:
                        try:
                            import importlib
                            module = importlib.import_module('vail' + node.module[4:])
                            scope[alias.asname or alias.name] = getattr(module, alias.name)
                        except (ImportError, AttributeError) as exc:
                            report['setup_errors'].append(str(exc))
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
            def fixture_dependencies(name, visited):
                if name in visited or name not in scope:
                    return
                visited.add(name)
                for dependency in inspect.signature(scope[name]).parameters:
                    if dependency != 'request':
                        fixture_dependencies(dependency, visited)
            dependencies = set()
            for name in inspect.signature(target).parameters:
                if not all(name in variant for variant in parameters):
                    fixture_dependencies(name, dependencies)
            for name in sorted(dependencies):
                if name in fixture_parameters:
                    parameters = [dict(variant, **{'__fixture_param_' + name: value})
                                  for variant, value in itertools.product(parameters, fixture_parameters[name])]
        except BaseException as exc:
            report.update(status='blocked', phase='setup_or_parameters', error_type=type(exc).__name__, error=str(exc), traceback=traceback.format_exc())
            save()
            return
        cache = dict(init_hail=None, tmp_path=temporary, tmpdir=temporary)
        def fixture(name):
            if name not in cache:
                factory = scope[name]
                cache[name] = factory(**{p: SimpleNamespace(param=params['__fixture_param_' + name])
                                       if p == 'request' and '__fixture_param_' + name in params
                                       else fixture(p)
                                       for p in inspect.signature(factory).parameters})
            return cache[name]
        for params in parameters:
            cache = dict(init_hail=None, tmp_path=temporary, tmpdir=temporary)
            phase = 'fixture'
            outcome = dict(status='passed', parameters={k: repr(v)[:300] for k,v in params.items()})
            try:
                kwargs = {key: value for key, value in params.items() if not key.startswith('__fixture_param_')}
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
