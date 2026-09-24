#!/usr/bin/env python3
"""Reproducibly enumerate table-related tests in the pinned Hail checkout.

This is deliberately a conservative inventory scanner, not a test runner or a
semantic Python/Scala parser. Every row retains its source location and an
explicit disposition so a reviewer can find ambiguous candidates.
"""
from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from collections import defaultdict, deque
from pathlib import Path

from hail_inventory_report import enrich_requirements, write_report

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
CLONE = WORKSPACE / "hail"
HAIL = CLONE / "hail"
PYTEST = HAIL / "python" / "test" / "hail"
SCALA_ROOT = HAIL / "hail" / "test" / "src"
OUT = ROOT / "conformance" / "hail-test-inventory.json"
DOC = ROOT / "docs" / "HAIL_TEST_INVENTORY.md"
EVIDENCE = ROOT / "conformance" / "upstream-table-results.json"
PIN = subprocess.check_output(["git", "-C", str(CLONE), "rev-parse", "HEAD"], text=True).strip()

DEDICATED = {
    "hail/python/test/hail/table/test_table.py",
    "hail/python/test/hail/table/test_grouped_table.py",
}

TABLE_CALLS = {
    "range_table", "parallelize", "from_pandas", "from_spark", "read_table",
    "import_table", "export", "write", "annotate", "annotate_globals", "select",
    "select_globals", "drop", "rename", "filter", "filter_intervals", "key_by",
    "order_by", "repartition", "coalesce", "head", "tail", "take", "collect",
    "count", "aggregate", "group_by", "group_by_key", "join", "join_right",
    "union", "distinct", "explode", "transmute", "flatten", "persist", "cache",
    "checkpoint", "n_partitions", "_force_count", "_same", "to_matrix_table",
    "_unlocalize", "_localize",
}
TABLE_CONSTRUCTORS = {"Table", "GroupedTable"}
MATRIX_TOKENS = {"MatrixTable", "BlockMatrix", "range_matrix_table", "from_entry_exprs"}
FEATURE_RULES = {
    "construction": {"range_table", "parallelize", "from_pandas", "from_spark"},
    "schema-and-rows": {"annotate", "select", "drop", "rename", "flatten", "transmute"},
    "globals": {"annotate_globals", "select_globals", "globals", "aggregate_globals"},
    "keying-and-order": {"key_by", "order_by", "repartition", "coalesce", "n_partitions", "tail", "head"},
    "filtering": {"filter", "filter_intervals", "filter_rows"},
    "aggregation": {"aggregate", "group_by", "group_by_key", "aggregate_by"},
    "joins-and-set-ops": {"join", "join_right", "union", "distinct"},
    "actions": {"collect", "take", "count", "_force_count"},
    "persistence-and-io": {"persist", "cache", "checkpoint", "write", "read_table", "import_table", "export"},
    "expression-semantics": {"annotate", "filter", "aggregate", "group_by"},
}


def rel(path: Path) -> str:
    return path.relative_to(CLONE).as_posix()


def decorator_text(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    return [ast.unparse(x) for x in node.decorator_list]


def static_length(expr: ast.AST, tree: ast.Module) -> int | None:
    if isinstance(expr, (ast.List, ast.Tuple, ast.Set)):
        return len(expr.elts)
    if isinstance(expr, ast.Name):
        assigned = next((item.value for item in tree.body if isinstance(item, (ast.Assign, ast.AnnAssign)) and any(isinstance(target, ast.Name) and target.id == expr.id for target in (item.targets if isinstance(item, ast.Assign) else [item.target]))), None)
        return static_length(assigned, tree) if assigned is not None else None
    if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name) and expr.func.id == "range":
        args = [static_int(a, tree) for a in expr.args]
        if any(a is None for a in args):
            return None
        try:
            return len(range(*args))
        except (TypeError, ValueError):
            return None
    if isinstance(expr, ast.ListComp):
        result = 1
        for gen in expr.generators:
            if gen.ifs:
                return None  # Filtering cannot be counted as a Cartesian product.
            n = static_length(gen.iter, tree)
            if n is None:
                return None
            result *= n
        return result
    return None


def static_int(expr: ast.AST, tree: ast.Module) -> int | None:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, int):
        return expr.value
    if isinstance(expr, ast.Name):
        assigned = next((item.value for item in tree.body if isinstance(item, (ast.Assign, ast.AnnAssign)) and any(isinstance(target, ast.Name) and target.id == expr.id for target in (item.targets if isinstance(item, ast.Assign) else [item.target]))), None)
        return static_int(assigned, tree) if assigned is not None else None
    return None


def fixture_param_count(node: ast.FunctionDef | ast.AsyncFunctionDef, tree: ast.Module) -> int | None:
    for dec in node.decorator_list:
        if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute) and dec.func.attr == "fixture":
            params = next((kw.value for kw in dec.keywords if kw.arg == "params"), None)
            if params is not None:
                return static_length(params, tree)
    return None


def param_count(decs: list[str], tree: ast.Module | None = None) -> dict:
    params = [d for d in decs if "parametrize" in d or "parameterized" in d.lower()]
    if not params:
        return {"kind": "single_definition", "known_case_count": 1, "decorators": []}
    counts = []
    for d in params:
        # Count literal rows in common @pytest.mark.parametrize('x', [...]) form.
        try:
            expr = ast.parse(d, mode="eval").body
            vals = next((kw.value for kw in expr.keywords if kw.arg in {"argvalues", "params"}), None)
            if vals is None and len(expr.args) > 1:
                vals = expr.args[1]
            counts.append(static_length(vals, tree) if tree is not None else None)
        except (SyntaxError, ValueError):
            counts.append(None)
    known = 1
    for n in counts:
        if n is None:
            return {"kind": "parameterized", "known_case_count": None, "unresolved_reason": "dynamic or unparsed parameter source", "decorators": params}
        known *= n
    # A directly requested pytest fixture can add a static number of cases.
    # Only count fixture functions defined in this module and avoid recursion.
    return {"kind": "parameterized", "known_case_count": known, "decorators": params}


def call_names(node: ast.AST) -> list[str]:
    out = set()
    for x in ast.walk(node):
        if isinstance(x, ast.Call):
            f = x.func
            if isinstance(f, ast.Name):
                out.add(f.id)
            elif isinstance(f, ast.Attribute):
                out.add(f.attr)
    return sorted(out)


def py_functions(path: Path):
    source = path.read_text()
    tree = ast.parse(source, filename=str(path))
    defs = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    by_name = defaultdict(list)
    for n in defs:
        by_name[n.name].append(n)
    fixtures = set(by_name)
    return source, tree, defs, by_name, fixtures


def imported_dependencies(path: Path, tree: ast.Module, calls: set[str]) -> list[dict]:
    """Resolve imports to local Python test files when names and paths are static."""
    result = []
    package_parts = list(path.relative_to(PYTEST).parent.parts)
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        module_parts = package_parts[: max(0, len(package_parts) - node.level + 1)]
        if node.module:
            module_parts.extend(node.module.split("."))
        module_path = PYTEST.joinpath(*module_parts)
        for alias in node.names:
            local_name = alias.asname or alias.name
            if local_name not in calls:
                continue
            targets = []
            for candidate in (module_path.with_suffix(".py"), module_path / "__init__.py"):
                if candidate.is_file():
                    try:
                        target_tree = ast.parse(candidate.read_text(), filename=str(candidate))
                        for item in ast.walk(target_tree):
                            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and item.name == alias.name:
                                targets.append({"source": rel(candidate), "line": item.lineno, "symbol": alias.name})
                    except SyntaxError:
                        pass
            result.append({"local_name": local_name, "import": ast.unparse(node), "resolved_targets": targets, "resolution": "resolved" if targets else "import known, target not statically found"})
    return result


def fixture_provenance(path: Path, args: list[str], local_defs) -> dict:
    """Follow fixture-to-fixture references in the module and nearest conftest."""
    pending = list(args)
    seen = set()
    records = []
    matrix_provenance = False
    while pending:
        name = pending.pop(0)
        if name in seen:
            continue
        seen.add(name)
        found = []
        found.extend((node, path) for node in local_defs.get(name, []))
        if not found:
            for directory in [path.parent, *path.parent.parents]:
                if not directory.is_relative_to(PYTEST):
                    continue
                for fixture_file in (directory / "conftest.py", directory / "helpers.py"):
                    if not fixture_file.is_file():
                        continue
                    try:
                        fixture_tree = ast.parse(fixture_file.read_text(), filename=str(fixture_file))
                    except SyntaxError:
                        continue
                    found.extend((node, fixture_file) for node in ast.walk(fixture_tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
                if found:
                    break
        for node, owner_file in found:
            body = ast.get_source_segment(owner_file.read_text(), node) or ""
            body_words = set(re.findall(r"[A-Za-z_]\w*", body))
            body_calls = set(call_names(node))
            is_matrix = bool(body_words & MATRIX_TOKENS) or "read_matrix_table" in body_calls or bool(re.search(r"\bmt\s*\.\s*(?:rows|entries)\s*\(", body))
            matrix_provenance |= is_matrix
            records.append({"name": name, "source": rel(owner_file), "line": node.lineno, "fixture_arguments": [a.arg for a in node.args.args if a.arg not in {"self", "cls", "request"}], "matrix_provenance": is_matrix, "called_names": sorted(body_calls)})
            pending.extend(a.arg for a in node.args.args if a.arg not in {"self", "cls", "request"})
    return {"resolved_fixtures": records, "matrix_provenance": matrix_provenance}


def py_row(path: Path, node, source: str, tree, by_name, dedicated: bool) -> dict:
    body = ast.get_source_segment(source, node) or ""
    calls = call_names(node)
    calls.extend(ast.unparse(dec).split("(")[0].split(".")[-1] for dec in node.decorator_list)
    names = set(calls)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    ancestors = []
    cursor = node
    while cursor in parents:
        cursor = parents[cursor]
        if isinstance(cursor, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            ancestors.append(cursor)
    qualified_name = "::".join([a.name for a in reversed(ancestors)] + [node.name])
    expression_calls = sorted({ast.unparse(c.func) for c in ast.walk(node)
                               if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                               and ast.unparse(c.func).startswith(("hl.", "agg.", "ir."))})
    fixture_args = [a.arg for a in node.args.args if a.arg not in {"self", "cls"}]
    fixture_info = fixture_provenance(path, fixture_args, by_name)
    words = set(re.findall(r"[A-Za-z_]\w*", body))
    constructor_hit = any(any(isinstance(part, ast.Attribute) and part.attr in TABLE_CONSTRUCTORS for part in ast.walk(c.func)) or isinstance(c.func, ast.Name) and c.func.id in TABLE_CONSTRUCTORS for c in ast.walk(node) if isinstance(c, ast.Call))
    table_path = rel(path) in DEDICATED
    receivers = set()
    for stmt in ast.walk(node):
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            value = stmt.value
            value_text = ast.get_source_segment(source, value) or ""
            if re.search(r"(?:range_table|Table\.parallelize|Table\.from_|read_table|import_table|_table\()", value_text):
                targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
                receivers.update(t.id for t in targets if isinstance(t, ast.Name))
            elif isinstance(value, ast.Name) and value.id in receivers:
                targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
                receivers.update(t.id for t in targets if isinstance(t, ast.Name))
    receivers.update(a.arg for a in node.args.args if a.arg.lower() in {"table", "ht", "kt", "grouped", "table_ir"})
    receiver_calls = set()
    for call in ast.walk(node):
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name) and call.func.value.id in receivers:
            receiver_calls.add(call.func.attr)
    rows_conversion = any(isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute) and c.func.attr in {"rows", "entries"} for c in ast.walk(node))
    fixture_operation_tokens = sorted({called for fixture in fixture_info["resolved_fixtures"] for called in fixture["called_names"] if re.fullmatch(r"Table[A-Z][A-Za-z0-9_]*", called) and called != "MatrixTable"})
    operation_tokens = sorted({word for word in words if re.fullmatch(r"Table[A-Z][A-Za-z0-9_]*", word) and word != "MatrixTable"})
    table_fixture = "table_ir" in receivers
    ir_evidence = bool(operation_tokens or fixture_operation_tokens or "TableIR" in words or table_fixture)
    api_hits = sorted((names & TABLE_CALLS) if table_path else (receiver_calls & TABLE_CALLS))
    table_producer_calls = sorted(names & {"range_table", "read_table", "import_table", "from_pandas", "from_spark", "genomic_range_table"})
    table_evidence = table_path or api_hits or constructor_hit or bool(table_producer_calls) or rows_conversion or ir_evidence or table_fixture
    matrix_evidence = bool(words & MATRIX_TOKENS) or bool(re.search(r"\b(?:mt|mtd|bm)\s*\.", body)) or bool(words & {"balding_nichols_model", "read_matrix_table", "range_matrix_table"}) or fixture_info["matrix_provenance"]

    if dedicated:
        disposition = "candidate_table_only" if table_evidence else "ambiguous_table_suite_member"
        if matrix_evidence:
            # A Table fixture can be MatrixTable-derived without the behavior
            # under test crossing that API. Preserve it for manual disposition.
            disposition = "table_candidate_with_matrix_dependency_review"
    elif table_evidence and matrix_evidence:
        disposition = "table_candidate_with_matrix_dependency_review"
    elif table_evidence and any("table" in x.lower() and x in by_name for x in calls):
        disposition = "ambiguous_table_fixture_dependency"
    elif table_evidence:
        disposition = "table_candidate_outside_dedicated_suite"
    else:
        disposition = "not_table_candidate"

    features = sorted({feature for feature, needles in FEATURE_RULES.items() if names & needles})
    ir_feature_map = {
        "TableRange": {"construction", "actions"}, "TableRead": {"persistence-and-io"}, "TableWrite": {"persistence-and-io"},
        "TableMapRows": {"schema-and-rows", "expression-semantics"}, "TableFilter": {"filtering"}, "TableFilterIntervals": {"filtering"},
        "TableKeyBy": {"keying-and-order"}, "TableOrderBy": {"keying-and-order"}, "TableJoin": {"joins-and-set-ops"},
        "TableUnion": {"joins-and-set-ops"}, "TableAggregate": {"aggregation", "expression-semantics"}, "TableCount": {"actions"},
        "TableCollect": {"actions"}, "TableHead": {"keying-and-order", "actions"}, "TableTail": {"keying-and-order", "actions"},
        "TableGetGlobals": {"globals"}, "TableMapGlobals": {"globals"}, "TableExplode": {"schema-and-rows"},
    }
    for op in operation_tokens:
        features.extend(ir_feature_map.get(op, set()))
    if table_fixture:
        features.append("table-ir")
    if ir_evidence:
        features.append("table-ir")
    features = sorted(set(features))
    if constructor_hit:
        features = sorted(set(features) | {"construction"})
    direct_helpers = sorted((set(calls) & set(by_name)) - {node.name})
    transitive = set(direct_helpers)
    q = deque(direct_helpers)
    while q:
        helper = q.popleft()
        for called in call_names(by_name[helper][0]):
            if called in by_name and called not in transitive:
                transitive.add(called)
                q.append(called)
    helper_returns_table = any(re.search(r"(?:range_table|Table\.parallelize|Table\.from_|Table\()", ast.get_source_segment(source, by_name[h][0]) or "") for h in direct_helpers)
    if helper_returns_table and not table_path and not matrix_evidence and disposition == "not_table_candidate":
        disposition = "ambiguous_table_fixture_dependency"
        table_evidence = True
    unresolved = []
    if matrix_evidence:
        unresolved.append("static scan found MatrixTable/BlockMatrix or matrix-style variable usage; inspect whether it is fixture provenance or asserted API behavior")
    if any(x in calls for x in ("create_all_values_table", "create_all_values_datasets", "resource")):
        unresolved.append("external helper/resource implementation is outside this module")
    detailed = []
    body_l = body.lower()
    detail_patterns = [
        (r"filter[\s\S]{0,500}(?:missing|is_missing|hl\.missing)", "filter-missing-value-semantics"),
        (r"filter[\s\S]{0,500}(?:keep|keep=False|keep=True)", "filter-keep-flag"),
        (r"(?:join|join_right|join\()[\s\S]{0,1000}(?:missing|multiplicity|duplicate|null)", "join-null-or-multiplicity-semantics"),
        (r"\bscan\b[\s\S]{0,1000}(?:exclusive|prefix|aggregate)", "scan-prefix-semantics"),
        (r"(?:source|axis|row_idx|col_idx|entry)", "source-or-axis-validation"),
        (r"(?:partition|partitioner|n_partitions|partition_counts?)", "partition-boundaries-and-counts"),
        (r"(?:globals|global)", "table-globals-preservation"),
        (r"(?:interval|filter_intervals)", "key-interval-filtering"),
        (r"(?:order_by|sort|ordering)", "ordering-and-sort-semantics"),
    ]
    for pattern, label in detail_patterns:
        if re.search(pattern, body_l):
            detailed.append(label)
    for call in names & {"key_by", "annotate", "select", "explode", "group_by", "aggregate", "union", "distinct", "head", "tail", "count", "collect"}:
        if call in names:
            detailed.append({"key_by": "key-schema-and-key-order", "annotate": "row-expression-evaluation", "select": "field-projection-and-expression-evaluation", "explode": "row-explosion", "group_by": "group-key-and-output-schema", "aggregate": "aggregation-scope-and-result-type", "union": "union-schema-and-row-concatenation", "distinct": "distinct-key-and-row-semantics", "head": "head-partition-traversal", "tail": "tail-partition-traversal", "count": "row-count-action", "collect": "collect-order-and-localization"}[call])
    ir_semantics = {"TableRange": "range-table-construction", "TableRead": "table-reader-and-storage", "TableWrite": "table-writer-and-storage", "TableMapRows": "row-mapping-expression-semantics", "TableFilter": "filter-missing-value-and-retention-semantics", "TableFilterIntervals": "key-interval-filtering", "TableKeyBy": "key-schema-and-key-order", "TableOrderBy": "ordering-and-sort-semantics", "TableJoin": "join-null-and-multiplicity-semantics", "TableUnion": "union-schema-and-row-concatenation", "TableAggregate": "aggregation-scope-and-result-type", "TableCount": "row-count-action", "TableCollect": "collect-order-and-localization", "TableHead": "head-partition-traversal", "TableTail": "tail-partition-traversal", "TableGetGlobals": "table-globals-preservation", "TableMapGlobals": "table-globals-preservation", "TableExplode": "row-explosion"}
    detailed.extend(ir_semantics[op] for op in operation_tokens if op in ir_semantics)
    if table_fixture:
        detailed.append("TableIR type validation across the statically parameterized TableIR fixture")
    if node.name == "test_show_rows_table":
        features.append("table-display")
        detailed.append("MatrixTable.rows conversion followed by Table field-expression display")
    features = sorted(set(features))
    detailed = sorted(set(detailed))
    imported = imported_dependencies(path, tree, names)
    fixture_defs = []
    for arg in fixture_args:
        if arg in by_name:
            fixture_defs.append({"name": arg, "source": rel(path), "line": by_name[arg][0].lineno})
        else:
            search_dirs = [path.parent, *path.parent.parents]
            matches = []
            for directory in search_dirs:
                if not directory.is_relative_to(PYTEST):
                    continue
                for fixture_file in (directory / "conftest.py", directory / "helpers.py"):
                    if fixture_file.is_file():
                        try:
                            ftree = ast.parse(fixture_file.read_text(), filename=str(fixture_file))
                            for item in ast.walk(ftree):
                                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == arg:
                                    matches.append({"source": rel(fixture_file), "line": item.lineno})
                        except SyntaxError:
                            pass
            fixture_defs.append({"name": arg, "source": None, "resolved_candidates": matches, "resolution": "resolved by name in nearest conftest/helpers" if matches else "fixture not defined in searched conftest/helpers; may be pytest plugin or external fixture"})
    parameterization = param_count(decorator_text(node), tree)
    fixture_case_count = 1
    fixture_case_known = True
    fixture_cases = []
    for arg in fixture_args:
        for fixture_node in by_name.get(arg, []):
            n = fixture_param_count(fixture_node, tree)
            if n is not None:
                fixture_case_count *= n
                fixture_cases.append({"name": arg, "count": n, "source": rel(path), "line": fixture_node.lineno})
            elif any(isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == "fixture" and any(k.arg == "params" for k in d.keywords) for d in fixture_node.decorator_list):
                fixture_case_known = False
    # Include pytest fixtures defined in the nearest conftest.py/helpers.py.
    for arg in fixture_args:
        if arg in by_name:
            continue
        for directory in [path.parent, *path.parent.parents]:
            if not directory.is_relative_to(PYTEST):
                continue
            found = False
            for fixture_file in (directory / "conftest.py", directory / "helpers.py"):
                if not fixture_file.is_file():
                    continue
                try:
                    ftree = ast.parse(fixture_file.read_text(), filename=str(fixture_file))
                except SyntaxError:
                    continue
                for fixture_node in ast.walk(ftree):
                    if not isinstance(fixture_node, (ast.FunctionDef, ast.AsyncFunctionDef)) or fixture_node.name != arg:
                        continue
                    found = True
                    n = fixture_param_count(fixture_node, ftree)
                    if n is not None:
                        fixture_case_count *= n
                        fixture_cases.append({"name": arg, "count": n, "source": rel(fixture_file), "line": fixture_node.lineno})
                    elif any(isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == "fixture" and any(k.arg == "params" for k in d.keywords) for d in fixture_node.decorator_list):
                        fixture_case_known = False
                if found:
                    break
            if found:
                break
    counted_fixtures = {(f["source"], f["line"]) for f in fixture_cases}
    for fixture in fixture_info["resolved_fixtures"]:
        identity = (fixture["source"], fixture["line"])
        if identity in counted_fixtures:
            continue
        fixture_tree = ast.parse((CLONE / fixture["source"]).read_text())
        fixture_node = next(n for n in ast.walk(fixture_tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.lineno == fixture["line"])
        n = fixture_param_count(fixture_node, fixture_tree)
        if n is not None:
            fixture_case_count *= n
            fixture_cases.append({"name": fixture["name"], "count": n, "source": fixture["source"], "line": fixture["line"], "transitive": True})
        elif any(isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == "fixture" and any(k.arg == "params" for k in d.keywords) for d in fixture_node.decorator_list):
            fixture_case_known = False
        counted_fixtures.add(identity)
    if parameterization.get("known_case_count") is not None:
        if fixture_case_known:
            parameterization["known_case_count"] *= fixture_case_count
        else:
            parameterization["known_case_count"] = None
            parameterization["unresolved_reason"] = "one or more fixture parameter sources are dynamic"
    row = {
        "id": f"py:{rel(path)}::{node.name}@{node.lineno}",
        "language": "python", "source": rel(path), "line": node.lineno, "test_id": node.name,
        "qualified_name": qualified_name,
        "collection_kind": "nested_helper_not_independent_case" if any(isinstance(a, (ast.FunctionDef, ast.AsyncFunctionDef)) for a in ancestors) else "test_definition",
        "required_expression_calls": expression_calls,
        "decorators": decorator_text(node), "parameterization": parameterization,
        "disposition": disposition, "static_inference": True,
        "table_evidence": {"matched_calls": api_hits, "table_producer_calls": table_producer_calls, "constructor_call": constructor_hit, "rows_or_entries_conversion": rows_conversion, "helper_constructs_table": helper_returns_table, "table_ir_tokens": operation_tokens, "fixture_table_ir_operation_universe": fixture_operation_tokens, "matrix_tokens": sorted(words & MATRIX_TOKENS)},
        "required_operations": operation_tokens,
        "required_features": features,
        "required_semantics": detailed,
        "fixture_provenance": fixture_info,
        "available_primitives": sorted(set(api_hits + table_producer_calls) & {"range_table", "parallelize", "key_by", "order_by", "head", "tail", "count", "collect", "n_partitions", "_force_count"}),
        "available_native_storage": ["typed primitive key-interval planner only; no persisted index/Python reader"] if "S2-fragment-index" in work_packages(features, body, path, node.name) else [],
        "remaining_requirements": sorted(set(detailed)) or (["manual body/fixture review required; no specific behavior inferred"] if disposition != "not_table_candidate" else []),
        "fixture_parameterization": fixture_cases,
        "dependencies": {"fixture_arguments": fixture_args, "fixture_resolution": fixture_defs, "same_module_direct_helpers": direct_helpers, "same_module_transitive_helpers": sorted(transitive), "imported_helper_calls": imported, "called_names": calls},
        "unresolved_dependencies_or_scope": unresolved,
        "status": "unrun",
        "work_packages": work_packages(features, body, path, node.name),
    }
    if disposition == "not_table_candidate":
        row["status"] = "not_applicable"
        row["work_packages"] = []
        row["required_features"] = []
        row["required_semantics"] = []
        row["required_operations"] = []
        row["remaining_requirements"] = []
    return row


def work_packages(features, body, path, test_id=""):
    packages = []
    if test_id == "test_table_head_and_tail" or re.search(r"\b(?:head|tail|_force_count|n_partitions)\b", body):
        packages.append("T1-tail-partitions")
    if re.search(r"\btest_(?:indexed_read|query_table)\w*", body) or "read_table" in body and any(x in body for x in ("_intervals", "_filter_intervals", "_n_partitions")):
        packages.append("S2-fragment-index")
    if "table-ir" in features or "TableIR" in body or re.search(r"\bTable[A-Z]\w*", body):
        packages.append("T7-upstream-ir-mapping")
    if "test_table_parallelize_error_both_schema_partial_type_defined" in body:
        packages.append("T0-constructor-validation")
    if test_id in {"test_range_table", "test_range_table_zero", "test_large_number_of_partitions"}:
        packages.append("T1-tail-partitions")
    lookup = {
        "construction": "T0-construction-schema", "schema-and-rows": "T0-construction-schema",
        "globals": "T3-globals", "keying-and-order": "T2-key-order",
        "filtering": "T4-filter", "aggregation": "T5-expression-aggregation",
        "joins-and-set-ops": "T6-joins-set-ops", "actions": "T9-table-actions",
        "persistence-and-io": "T8-persistence-io", "expression-semantics": "T5-expression-aggregation",
        "table-display": "T9-table-actions",
    }
    for feature in features:
        pkg = lookup.get(feature)
        if pkg and pkg not in packages:
            packages.append(pkg)
    return list(dict.fromkeys(packages))


def scala_rows():
    rows = []
    annotation_count = 0
    for path in sorted(SCALA_ROOT.rglob("*.scala")):
        relpath = path.relative_to(SCALA_ROOT).as_posix()
        lines = path.read_text().splitlines()
        # Scanner covers JUnit's ordinary and parameterized annotations. It
        # does not expand runtime providers or non-JUnit Scala test styles.
        for i, line in enumerate(lines):
            if not re.match(r"\s*@(Test|ParameterizedTest)\b", line):
                continue
            annotation_count += 1
            block_end = min((j for j in range(i + 1, len(lines)) if re.match(r"\s*@(Test|ParameterizedTest)\b", lines[j])), default=len(lines))
            block = "\n".join(lines[i:block_end])
            m = re.search(r"\bdef\s+([A-Za-z_$][\w$]*)\s*(?:\[[^\]]*\])?\s*(?:\([^\n]*\))?", block)
            if not m:
                # Keep marker-only/dynamic test construct visible.
                rows.append({"id": f"scala:{relpath}@{i+1}", "language": "scala", "source": rel(path), "line": i+1, "test_id": None, "disposition": "review_required", "static_inference": True, "status": "unrun", "required_features": [], "dependencies": {"annotated_block_excerpt": block[:300]}, "work_packages": [], "unresolved_dependencies_or_scope": ["JUnit test annotation did not have a statically found def before the next annotation"]})
                continue
            name = m.group(1)
            def_line = next((j for j in range(i, block_end) if re.search(rf"\bdef\s+{re.escape(name)}\b", lines[j])), i)
            indent = len(lines[def_line]) - len(lines[def_line].lstrip())
            for j in range(def_line + 1, block_end):
                line_j = lines[j]
                current_indent = len(line_j) - len(line_j.lstrip())
                if current_indent == indent and re.match(r"\s*(?:def\b|@)", line_j):
                    block_end = j
                    break
            block = "\n".join(lines[i:block_end])
            words = set(re.findall(r"[A-Za-z_]\w*", block))
            features = sorted({feature for feature, needles in FEATURE_RULES.items() if words & needles})
            required_ops = sorted({word for word in words if re.fullmatch(r"Table[A-Z][A-Za-z0-9_]*", word) and word != "MatrixTable"})
            rvd_markers = sorted(words & {"RVD", "KeyedRVD", "RVDPartitioner", "PartitionBoundOrdering"})
            table_markers = bool(required_ops or "TableIR" in words or rvd_markers)
            if table_markers:
                features = sorted(set(features) | {"table-ir"})
            packages = work_packages(features, block, path)
            if table_markers:
                packages.append("T7-upstream-ir-mapping")
            table_suite = any(token in relpath for token in ("expr/ir/Table", "expr/ir/table/", "expr/ir/lowering/", "rvd/"))
            if table_markers:
                disp = "candidate_table_only"
            elif table_suite:
                disp = "ambiguous_table_related_suite_case"
            else:
                disp = "outside_scope"
            annotation = "@ParameterizedTest" if "@ParameterizedTest" in lines[i] else "@Test"
            param_record = {"kind": "parameterized_annotation", "known_case_count": None, "unresolved_reason": "Scala runtime/provider expansion not statically counted"} if annotation == "@ParameterizedTest" else {"kind": "annotation", "known_case_count": 1, "unresolved_reason": "runtime/provider expansion not statically counted"}
            rows.append({"id": f"scala:{relpath}::{name}@{def_line+1}", "language": "scala", "source": rel(path), "line": def_line+1, "test_id": name, "decorators": [annotation], "parameterization": param_record, "disposition": disp, "static_inference": True, "required_operations": required_ops, "required_features": features, "required_semantics": sorted(set(features) | set(required_ops) | (set(["scan-prefix-semantics"]) if "scan" in words else set()) | (set(["partition-boundaries-and-counts"]) if "partition" in words else set())), "dependencies": {"fixture_or_context": sorted(set(re.findall(r"\b(?:ExecuteContext|TableIR|RVD|RDD|Partitioner)\b", block)) | set(rvd_markers)), "called_names": sorted(set(re.findall(r"\b([A-Za-z_]\w*)\s*\(", block)))}, "status": "unrun" if disp != "outside_scope" else "not_applicable", "work_packages": packages if disp != "outside_scope" else [], "unresolved_dependencies_or_scope": ["Scala body calls and implicit ExecuteContext resolution are text-scanned; inspect source for full transitive helper graph"] if disp != "outside_scope" else []})
    if annotation_count != 1327:
        raise SystemExit(f"expected 1,327 Scala JUnit annotation lines at pinned baseline; found {annotation_count}")
    return rows


def main():
    rows = []
    py_defs = 0
    if not PYTEST.is_dir() or not SCALA_ROOT.is_dir():
        raise SystemExit(f"Hail source roots missing: {PYTEST} / {SCALA_ROOT}")
    dedicated_counts = defaultdict(int)
    for path in sorted(PYTEST.rglob("test_*.py")):
        source, tree, defs, by_name, _ = py_functions(path)
        for node in defs:
            if not node.name.startswith("test_"):
                continue
            py_defs += 1
            if rel(path) in DEDICATED:
                dedicated_counts[rel(path)] += 1
            rows.append(py_row(path, node, source, tree, by_name, rel(path) in DEDICATED))
    rows.extend(scala_rows())
    if sum(dedicated_counts.values()) != 214:
        raise SystemExit(f"expected 214 definitions in dedicated table suites; found {sum(dedicated_counts.values())}: {dict(dedicated_counts)}")
    if not any(r["language"] == "scala" for r in rows):
        raise SystemExit("Scala @Test/@ParameterizedTest scan returned no rows")
    ids = [r["id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise SystemExit("inventory row IDs are not unique")
    for row in rows:
        source_path = CLONE / row["source"]
        if not source_path.is_file():
            raise SystemExit(f"inventory source does not exist: {row['source']}")
        line_count = len(source_path.read_text().splitlines())
        if row["line"] < 1 or row["line"] > line_count:
            raise SystemExit(f"inventory source location out of range: {row['source']}:{row['line']} (file has {line_count} lines)")
    rows.sort(key=lambda r: (r["language"], r["source"], r["line"], r["test_id"] or ""))
    enrich_requirements(rows, ROOT, CLONE, PIN, DEDICATED)
    evidence = json.loads(EVIDENCE.read_text()) if EVIDENCE.is_file() else None
    evidence_by_source_test_line = defaultdict(list)
    if evidence:
        evidence_path = CLONE / evidence["source"]
        if evidence.get("hail_commit") != PIN:
            raise SystemExit(f"conformance evidence commit {evidence.get('hail_commit')} does not match checkout {PIN}")
        observed_sha = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
        if observed_sha != evidence.get("source_sha256"):
            raise SystemExit(f"conformance evidence source SHA mismatch for {evidence['source']}")
        for name, expected_sha in evidence.get("vail_source_sha256", {}).items():
            if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected_sha:
                raise SystemExit(f"conformance evidence is stale for Vail source {name}; rerun the source-body harness")
        case_ids = [outcome["case_id"] for outcome in evidence.get("outcomes", [])]
        if len(case_ids) != len(set(case_ids)):
            raise SystemExit("conformance evidence contains duplicate case IDs")
        for outcome in evidence.get("outcomes", []):
            evidence_by_source_test_line[(evidence["source"], outcome["test_id"], outcome["source_line"])].append(outcome)
        for row in rows:
            exact_id = row["source"] + "::" + row.get("qualified_name", row.get("test_id", ""))
            matches = evidence_by_source_test_line.get((row["source"], exact_id, row["line"]), [])
            if row["language"] != "python" or not matches:
                continue
            row["status"] = "partial"
            case_count = row["parameterization"].get("known_case_count")
            full_body = case_count is not None and len(matches) == case_count and all(x["status"] == "passed" for x in matches)
            row["execution_evidence"] = {"file": "conformance/upstream-table-results.json", "outcome": ("passed_body" if full_body else "partial_body") if all(x["status"] == "passed" for x in matches) else "failed_body", "cases_executed": len(matches), "cases_passed": sum(1 for x in matches if x["status"] == "passed"), "definition_case_count": case_count, "scope_note": "Original extracted test body executed against Vail; this is not a Hail oracle or full upstream pytest pass."}
            row["remaining_requirements"] = ["No missing assertion functionality in the executed source-body scope; Hail oracle, full pytest collection and unrepresented dependencies remain unverified."] if full_body else row["remaining_requirements"]
            if row["test_id"] == "test_table_head_and_tail":
                row["execution_evidence"]["scope_note"] = "Only 80 rt/par cases ran; 80 cache/checkpoint cases remain untested. Keep status partial for this definition."
                row["remaining_requirements"] = ["80 cache/checkpoint variants remain untested and require persistence semantics; Hail oracle/full pytest collection remain unverified."]
    summary = defaultdict(int)
    for r in rows:
        summary[r["disposition"]] += 1
    data = {
        "schema_version": 1,
        "source": {"repository": "hail", "pinned_revision": PIN, "checkout_revision": PIN, "inventory_script": "scripts/inventory_hail_tests.py"},
        "methodology": {"inference": "Static AST for Python; conservative @Test/@ParameterizedTest method scanner for every Scala test/src file; features and dependency edges are inferred from test bodies and tokens.", "limitations": ["Python runtime-generated tests, doctests, parametrizations from imported/dynamic iterables, and helper behavior in other modules cannot always be expanded statically.", "Scala scanner records @Test and @ParameterizedTest methods; generated tests, custom providers and non-JUnit frameworks may be missed.", "Every Python test definition under test_*.py and every Scala JUnit-annotated method is represented, including outside_scope rows, to make scan coverage auditable.", "Status is inventory state only. No upstream Hail test is marked passing based on similarity to Vail tests."], "dedicated_python_suites": sorted(DEDICATED), "dedicated_python_counts": dict(dedicated_counts), "scala_coverage": "All Scala .scala files under hail/test/src were scanned for @Test and @ParameterizedTest methods."},
        "status_legend": {"unrun": "Candidate Hail test has no direct execution evidence in the attached conformance evidence.", "not_applicable": "Explicitly outside the table-test scope; does not express Vail support status.", "partial": "Original test body/cases executed against Vail, but the full Hail test definition or Hail oracle is not established."},
        "work_packages": {
            "T1-tail-partitions": {"owner_hint": "Sol / table_tail_partition_slice", "scope": "tail, _force_count, primitive field.collect, n_partitions, empty globals dtype, schema structural equality, primitive schema string, and range_table; specifically tail/head-related tests and partition-count cases", "blockers": ["Other expression semantics and non-primitive aggregation cases need separate packages"]},
            "S2-fragment-index": {"owner_hint": "Sol / vortex_iceberg_storage", "scope": "native metadata-only typed key interval planner for indexed reads", "blockers": ["Persistence and Python reader remain dependencies; must not imply full-table read sorting"]},
            "T0-construction-schema": {"scope": "Table construction, schema/row shape, range and parallelize"},
            "T2-key-order": {"scope": "Keying, ordering, repartitioning, head/tail"},
            "T3-globals": {"scope": "Global annotations and global actions"},
            "T4-filter": {"scope": "Row and interval filtering"},
            "T5-expression-aggregation": {"scope": "Expression evaluation, aggregation and grouped aggregation"},
            "T6-joins-set-ops": {"scope": "Joins, distinct and union"},
            "T7-upstream-ir-mapping": {"scope": "Scala TableIR/TableGen/RVD/lowering semantics and test-to-runtime mapping"},
            "T8-persistence-io": {"scope": "Persistence, cache/checkpoint, import/export and full reader/writer behavior; independent from S2 indexed-read planning"},
            "T9-table-actions": {"scope": "Display, summarize and Python action adapters beyond the listed primitive operations"},
            "T10-table-domain-methods": {"scope": "Table-only graph/genomic/interval helpers and their native kernels or compatible frontend compositions"},
            "T0-constructor-validation": {"scope": "Mutually exclusive schema/partial_type validation and exact constructor error category/message; completed bounded task"}
        },
        "summary_by_disposition": dict(sorted(summary.items())),
        "python_test_definition_count": py_defs,
        "execution_evidence_summary": evidence.get("summary") if evidence else None,
        "scala_test_definition_count": sum(r["language"] == "scala" for r in rows),
        "tests": rows,
    }
    write_report(data, ROOT, DEDICATED)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print(f"wrote {OUT}: {len(rows)} rows ({py_defs} Python definitions, {data['scala_test_definition_count']} Scala methods)")


if __name__ == "__main__":
    main()
