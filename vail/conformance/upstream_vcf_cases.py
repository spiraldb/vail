"""Run the original VCFTests bodies with Vail; retain assertions and helpers.

Backend exception names map to Vail's RuntimeError/ValueError; no-codegen flags
are irrelevant to this backend. Everything else is executed as written, including
known dependencies on export, Hail snapshots, and unrelated shared APIs.
"""
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import traceback
import unittest
from unittest import mock

import pytest
import vail as hl

SOURCE = Path('/Users/danielking/projects/vail/hail/hail/python/test/hail/methods/test_impex.py')
RESOURCES = SOURCE.parents[4] / 'hail/test/resources'
module = ast.parse(SOURCE.read_text())
vcf_class = next(n for n in module.body if isinstance(n, ast.ClassDef) and n.name == 'VCFTests')
for method in vcf_class.body:
    if isinstance(method, ast.FunctionDef):
        method.decorator_list = []
nodes = [n for n in module.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith('_FLOAT_') for t in n.targets)]
nodes += [n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == '_vcf_unsorted_alleles']
nodes += [vcf_class]


def run(case=None, all_tests=False):
    with tempfile.TemporaryDirectory(prefix='vail-vcf-upstream-') as temp:
        def new_temp_file(prefix=None, extension=None):
            import uuid
            return str(Path(temp) / ((prefix or 'tmp') + uuid.uuid4().hex + ('.' + extension if extension else '')))
        scope = dict(hl=hl, json=json, os=os, re=re, shutil=shutil, unittest=unittest, mock=mock,
                     pytest=pytest, resource=lambda name: str(RESOURCES / name),
                     new_temp_file=new_temp_file, new_local_temp_file=new_temp_file,
                     FatalError=(RuntimeError, ValueError), HailUserError=(RuntimeError, ValueError),
                     with_flags=lambda **kwargs: lambda function: function)
        exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(SOURCE), 'exec'), scope)
        instance = scope['VCFTests']()
        cases = [case] if case else [n.name for n in vcf_class.body if isinstance(n, ast.FunctionDef) and (all_tests and n.name.startswith('test_') or n.name.startswith('test_import_vcf') or n.name in {
            'test_info_char','test_info_float64','test_glob','test_undeclared_info','test_can_import_bad_number_flag',
            'test_malformed','test_not_identical_headers','test_filter','test_find_replace','test_haploid','test_call_fields',
            'test_vcf_unsorted_alleles','test_vcf_unsorted_alleles_no_codegen','test_import_gvcfs','test_import_gvcfs_long_line',
            'test_vcf_parser_golden_master__ex_GRCh37','test_vcf_parser_golden_master__sample_GRCh37',
            'test_vcf_parser_golden_master__gvcf_GRCh37','test_flag_at_eol','test_missing_float_entries','test_vcf_different_info_errors',
            'test_empty_import_vcf_group_by_collect','test_custom_rg_import',
        })]
        results = []
        for name in cases:
            started = time.perf_counter()
            try:
                getattr(instance, name)()
                result = dict(case=name, status='passed')
            except BaseException as error:
                result = dict(case=name, status='failed_or_blocked', error_type=type(error).__name__, message=str(error), traceback=traceback.format_exc())
            result['seconds'] = time.perf_counter() - started
            results.append(result)
            print(name, result['status'], result.get('message', '')[:250], flush=True)
        return dict(source=str(SOURCE), source_sha256=hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
                    adaptations=['Hail replaced with Vail', 'Decorators removed', 'FatalError/HailUserError mapped to RuntimeError or ValueError', 'No-codegen flag omitted'], results=results)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--case')
    parser.add_argument('--all', action='store_true', help='Attempt every VCFTests definition, including export and combiner tests')
    parser.add_argument('--output', type=Path, default=Path('/private/tmp/vail-vcf-upstream.json'))
    args = parser.parse_args()
    report = run(args.case, args.all)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(sum(r['status'] == 'passed' for r in report['results']), '/', len(report['results']))
