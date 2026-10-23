"""Run unchanged, AST-extracted upstream bodies with Vail-bound hl."""
import ast
from pathlib import Path
import unittest
import numpy as np
import pytest
import pickle
import dill
import vail as hl
ROOT = Path('/Users/danielking/projects/vail/hail/hail/python/test/hail')
namespace = dict(hl=hl, Call=hl.Call, np=np, pytest=pytest, unittest=unittest,
                 pickle=pickle, dill=dill)
count = 0
for relative, selected in [
    ('genetics/test_call.py', {'test_hom_ref', 'test_het', 'test_hom_var', 'test_haploid', 'test_zeroploid', 'test_call_rich_comparison'}),
    ('expr/test_expr.py', {'test_struct_expression_expr_rename'}),
    ('utils/test_pickle.py', {'test_frozendict_pickle', 'test_frozendict_dill'}),
    ('linalg/test_linalg.py', {'test_array_windows', 'test_array_windows_illegal_arguments'}),
]:
    tree = ast.parse((ROOT / relative).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in selected:
            exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), str(ROOT / relative), 'exec'), namespace)
            function = namespace[node.name]
            if node.args.args and node.args.args[0].arg == 'self':
                function(unittest.TestCase())
                count += 1
            elif node.name == 'test_array_windows_illegal_arguments':
                for mark in function.pytestmark:
                    if mark.name == 'parametrize':
                        for values in mark.args[1]:
                            function(*values)
                            count += 1
            else:
                function()
                count += 1
            print('PASS', relative, node.name, flush=True)
print('PASSED_UPSTREAM_CASES', count)
