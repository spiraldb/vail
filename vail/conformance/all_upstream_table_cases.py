"""Attempt every upstream Table test body, with isolated subprocesses.

Imports and decorators are omitted. Original functions, class helpers, assertions,
parameter values and local data fixtures are preserved. Hail initialization is
omitted, and temporary-file/resource helpers are supplied by this harness.
"""
from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import inspect
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from timeit import default_timer as timer
import traceback
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT.parent / 'hail/hail/python/test/hail/table/test_table.py'


def definitions(tree):
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
            yield node.name, node
        elif isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, ast.FunctionDef) and method.name.startswith('test_'):
                    yield f'{node.name}.{method.name}', method


def compile_node(node, namespace, filename):
    node = copy.deepcopy(node)
    for child in ast.walk(node):
        if isinstance(child, (ast.FunctionDef, ast.ClassDef)):
            child.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                 filename, 'exec'), namespace)


def worker(test_id, output):
    import numpy as np
    import pandas as pd
    import pytest
    import vail as hl

    tree = ast.parse(SOURCE.read_text())
    target = dict(definitions(tree))[test_id]
    outcomes = []
    with tempfile.TemporaryDirectory(prefix='vail-upstream-table-') as tmp:
        def new_temp_file(prefix='', extension=None):
            return str(Path(tmp) / (prefix + next(tempfile._get_candidate_names())
                       + ('.' + extension if extension else '')))

        ns = dict(hl=hl, agg=hl.agg, Env=hl.utils.Env, ExpressionException=hl.ExpressionException,
                  pytest=pytest, unittest=unittest, np=np, pd=pd, os=os, timer=timer,
                  new_temp_file=new_temp_file,
                  resource=lambda name: str(ROOT.parent / 'hail/hail/hail/test/resources' / name),
                  __name__='vail_all_upstream_table_bodies')
        ns.update({name: getattr(hl, name) for name in dir(hl) if name.startswith('t')})
        helpers = SOURCE.parent.parent / 'helpers.py'
        for node in ast.parse(helpers.read_text()).body:
            if isinstance(node, ast.FunctionDef) and node.name in {
                'convert_struct_to_dict', 'create_all_values', 'prefix_struct',
                'create_all_values_table', 'create_all_values_matrix_table',
                'create_all_values_datasets', 'assert_time', 'schema_eq',
            }:
                compile_node(node, ns, str(helpers))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                compile_node(node, ns, str(SOURCE))
            elif isinstance(node, ast.Assign):
                compile_node(node, ns, str(SOURCE))
        if '.' in test_id:
            cls, name = test_id.split('.')
            fn = getattr(ns[cls](methodName=name), name)
        else:
            fn = ns[test_id]

        def evaluate(node):
            return eval(compile(ast.Expression(node), str(SOURCE), 'eval'), ns)

        try:
            parameters = [{}]
            for decorator in target.decorator_list:
                if (isinstance(decorator, ast.Call)
                        and isinstance(decorator.func, ast.Attribute)
                        and decorator.func.attr == 'parametrize'):
                    names = evaluate(decorator.args[0])
                    names = [n.strip() for n in names.split(',')] if isinstance(names, str) else names
                    variants = []
                    for values in evaluate(decorator.args[1]):
                        if hasattr(values, 'values') and hasattr(values, 'marks'):
                            values = values.values
                        elif len(names) == 1:
                            values = (values,)
                        variants.append(dict(zip(names, values)))
                    parameters = [dict(a, **b) for a, b in itertools.product(parameters, variants)]
        except Exception as error:
            outcomes.append(dict(status='blocked', phase='parameterization',
                                 error_type=type(error).__name__, error=str(error),
                                 traceback=traceback.format_exc()))
            parameters = []

        fixture_cache = {'init_hail': None}

        def fixture(name):
            if name not in fixture_cache:
                factory = ns[name]
                fixture_cache[name] = factory(**{n: fixture(n) for n in inspect.signature(factory).parameters})
            return fixture_cache[name]

        for params in parameters:
            row = dict(parameters={k: repr(v) for k, v in params.items()}, status='passed')
            phase = 'fixture'
            try:
                kwargs = dict(params)
                for name in inspect.signature(fn).parameters:
                    if name not in kwargs:
                        kwargs[name] = fixture(name)
                phase = 'body'
                fn(**kwargs)
            except BaseException as error:
                row.update(status='failed' if phase == 'body' and not isinstance(error, NameError) else 'blocked', phase=phase,
                           error_type=type(error).__name__, error=str(error),
                           traceback=traceback.format_exc())
            outcomes.append(row)
            Path(output).write_text(json.dumps(outcomes, indent=2))
    Path(output).write_text(json.dumps(outcomes, indent=2))


def main():
    global SOURCE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker')
    parser.add_argument('--source', type=Path, default=SOURCE)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args()
    SOURCE = args.source.resolve()
    if args.worker:
        worker(args.worker, args.output)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    start = time.time()
    tests = list(definitions(ast.parse(SOURCE.read_text())))
    for index, (test_id, node) in enumerate(tests, 1):
        case_path = args.output / f'{test_id}.json'
        command = [sys.executable, str(Path(__file__).resolve()), '--worker', test_id,
                   '--output', str(case_path), '--source', str(SOURCE)]
        with (args.output / f'{test_id}.log').open('w') as log:
            try:
                proc = subprocess.run(command, stdout=log, stderr=log, timeout=args.timeout)
                problem = None if proc.returncode == 0 else f'worker exit {proc.returncode}'
            except subprocess.TimeoutExpired:
                problem = f'worker exceeded {args.timeout}s'
        cases = json.loads(case_path.read_text()) if case_path.exists() else []
        for case in cases:
            if case.get('error_type') == 'NameError':
                case['status'] = 'blocked'
        if problem:
            cases.append(dict(status='blocked', phase='worker', error=problem))
        status = ('blocked' if any(c['status'] == 'blocked' for c in cases)
                  else 'failed' if any(c['status'] == 'failed' for c in cases)
                  else 'passed')
        results.append(dict(test_id=test_id, source_line=node.lineno, status=status, cases=cases))
        report = dict(source=str(SOURCE), source_sha256=hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
                      mode='all-original-test-bodies-against-vail', timeout_seconds=args.timeout,
                      limitations=__doc__, elapsed_seconds=time.time()-start,
                      definitions_summary={s: sum(r['status']==s for r in results)
                                           for s in ('passed', 'failed', 'blocked')},
                      cases_summary={s: sum(c['status']==s for r in results for c in r['cases'])
                                     for s in ('passed', 'failed', 'blocked')}, results=results)
        (args.output / 'results.json').write_text(json.dumps(report, indent=2))
        print(f'{index}/{len(tests)} {status}: {test_id}', flush=True)
    print(json.dumps(report['definitions_summary']), flush=True)


if __name__ == '__main__':
    main()
