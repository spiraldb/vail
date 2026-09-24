"""Merge reviewed requirements and render an auditable Table compatibility report."""
from __future__ import annotations

from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path


EXCLUDED = {"not_table_candidate", "outside_scope"}


def enrich_requirements(rows, root, clone, pin, dedicated):
    """Keep inference, reviewed requirements and execution evidence distinct."""
    index = {(row["source"], row["test_id"], row["line"]): row for row in rows}
    reviewed = set()
    files = [root / "conformance/table-test-requirements.json"]
    files += sorted((root / "conformance").glob("extra-table-requirements-*.json"))
    for path in files:
        data = json.loads(path.read_text())
        if isinstance(data, dict):
            revision = data.get("hail_revision", data.get("pinned_revision", data.get("source_revision", pin)))
            if revision != pin:
                raise ValueError(f"Stale reviewed requirements in {path}: {revision}")
            entries = data["tests"]
        else:
            entries = data
        for entry in entries:
            key = (entry["source"], entry["test_id"], entry["line"])
            if key not in index or key in reviewed:
                raise ValueError(f"Unknown or duplicate reviewed test requirement: {key}")
            actual_sha = hashlib.sha256((clone / entry["source"]).read_bytes()).hexdigest()
            if entry.get("source_sha256", actual_sha) != actual_sha:
                raise ValueError(f"Stale reviewed source in {path}: {key}")
            row = index[key]
            row["implementation_requirement"] = entry["implementation_requirement"]
            row["reviewed_scope"] = entry["reviewed_scope"]
            row["requirement_provenance"] = {
                "kind": "source_reviewed",
                "file": path.relative_to(root).as_posix(),
                "source_sha256": actual_sha,
            }
            reviewed.add(key)
    if sum(key[0] in dedicated for key in reviewed) != 214:
        raise ValueError("Every dedicated Table test needs a reviewed implementation requirement")

    for row in rows:
        row["work_packages"] = list(dict.fromkeys(row.get("work_packages", [])))
        candidate = row["disposition"] not in EXCLUDED
        row.setdefault("reviewed_scope", "unreviewed_static_candidate" if candidate else "outside_static_table_scope")
        scope = row["reviewed_scope"]
        row["phase_one_disposition"] = (
            "deferred_legacy_format" if scope == "legacy_format_deferred"
            else "table_compatibility_target" if scope in {"table_only", "table_ir_backend_semantics"}
            else "shared_semantic_dependency" if scope == "shared_table_expression"
            else "table_target_requires_fixture_port" if scope == "table_behavior_with_matrix_fixture"
            else "split_table_assertions_from_deferred_api" if scope == "mixed_matrix_or_blockmatrix_api"
            else "deferred_api_or_backend" if scope in {"backend_specific", "outside_table_api", "matrix_only_in_table_suite", "spark_interop"}
            else "scope_review_required" if candidate else "outside_static_table_scope"
        )
        if not candidate:
            # Generic collection calls do not imply Table functionality.
            for field in ("required_features", "required_semantics", "remaining_requirements", "available_primitives"):
                row[field] = []
            continue
        if "implementation_requirement" not in row:
            operations = row.get("required_operations", [])
            exprs = row.get("required_expression_calls", [])
            # This is evidence for review, not an assertion-level semantic analysis.
            pieces = []
            if operations:
                pieces.append("Native equivalents of " + ", ".join(operations))
            if exprs:
                pieces.append("called expression/API functions: " + ", ".join(exprs))
            if not pieces:
                features = row.get("required_semantics") or row.get("required_features")
                pieces.append("Inferred behaviors: " + ", ".join(features) if features else "Helper/fixture-backed behavior needs source review")
            row["implementation_requirement"] = "; ".join(pieces) + ". Confirm assertion semantics and scope before implementation."
            row["requirement_provenance"] = {"kind": "static_inference"}
        row["remaining_requirements"] = [row["implementation_requirement"]]
        row["available_primitives"] = [name for name in row.get("available_primitives", []) if name != "read_table"]
        row["available_primitives_note"] = "Related API implementations are not evidence that this original test passes. Scalar projections and nested struct access have a native expression path; higher-order collections and Python storage read/write remain unsupported."
        # Explicit source-reviewed requirements expose dependencies missed by call-name tags.
        req = row["implementation_requirement"].lower()
        for words, package in (
            (("random", "scan", "aggregat", "expression", "literal", "nested", "ndarray"), "T5-expression-aggregation"),
            (("global",), "T3-globals"),
            (("indexing", "lookup", "join", "distinct", "union"), "T6-joins-set-ops"),
            (("persist", "checkpoint", "export", "import", "pandas", "write", "reopen"), "T8-persistence-io"),
            (("render", "show(", "_same"), "T9-table-actions"),
            (("maximal_independent_set", "segment_intervals", "split_multi_hts"), "T10-table-domain-methods"),
        ):
            if row["requirement_provenance"]["kind"] == "source_reviewed" and any(word in req for word in words):
                if package not in row["work_packages"]:
                    row["work_packages"].append(package)
        if row["language"] == "scala" and row["reviewed_scope"] == "table_ir_backend_semantics":
            if "T7-upstream-ir-mapping" not in row["work_packages"]:
                row["work_packages"].append("T7-upstream-ir-mapping")


def cell(text):
    return str(text).replace("|", "&#124;").replace("\n", " ")


def source_link(row, pin):
    return f"[{row['test_id']}](https://github.com/hail-is/hail/blob/{pin}/{row['source']}#L{row['line']})"


def case_label(row):
    if row.get("collection_kind") == "nested_helper_not_independent_case":
        return "nested helper"
    count = row.get("parameterization", {}).get("known_case_count")
    return str(count) if count is not None else "dynamic"


def evidence_label(row):
    evidence = row.get("execution_evidence")
    if not evidence:
        return "unrun" if row["status"] != "not_applicable" else "outside scope"
    return f"{evidence['outcome']}: {evidence['cases_passed']}/{evidence['definition_case_count'] or '?'}"


def render_groups(rows, pin, *, dedicated=False):
    groups = defaultdict(list)
    for row in rows:
        groups[row["source"]].append(row)
    lines = []
    for source, members in sorted(groups.items()):
        lines += [f"### `{source}`", "", "| Test / source | Cases* | Scope | Functionality required | Evidence |", "|---|---:|---|---|---|"]
        for row in members:
            requirement = row["implementation_requirement"]
            if row.get("execution_evidence"):
                requirement += " **Remaining:** " + " ".join(row["remaining_requirements"])
            if row["requirement_provenance"]["kind"] == "static_inference":
                requirement = "**Static inference:** " + requirement
            lines.append("| " + " | ".join((source_link(row, pin), case_label(row), cell(row["reviewed_scope"]), cell(requirement), evidence_label(row))) + " |")
        lines.append("")
    return lines


def write_report(data, root, dedicated):
    rows = data["tests"]
    pin = data["source"]["pinned_revision"]
    candidates = [r for r in rows if r["disposition"] not in EXCLUDED]
    dedicated_rows = [r for r in rows if r["source"] in dedicated]
    extra_python = [r for r in candidates if r["language"] == "python" and r["source"] not in dedicated]
    scala = [r for r in candidates if r["language"] == "scala"]
    scopes = Counter(r["reviewed_scope"] for r in dedicated_rows)
    all_scopes = Counter(r["reviewed_scope"] for r in candidates)
    review_count = sum(r.get("requirement_provenance", {}).get("kind") == "source_reviewed" for r in candidates)
    body_results = Counter(r["execution_evidence"]["outcome"] for r in rows if "execution_evidence" in r)
    common = [
        "*Cases are statically countable decorator/fixture combinations, not a pytest collection result. Dynamic providers remain explicit; nested test-named helpers are not independent test cases. Backend/decorator runtime expansion is not counted.*", "",
        "Requirements describe complete assertion functionality. Related primitive implementations are recorded separately in the JSON and do not imply that a test passes. `passed_body` means the extracted original assertions passed against Vail; decorators/imports/Hail fixtures were omitted. No full Hail pytest, Hail oracle or original Scala execution is claimed.", "",
    ]
    lines = [
        "# Remaining Hail Table tests", "",
        f"Pinned Hail source: `{pin}`. This inventory scans **{data['python_test_definition_count']:,} Python test-named definitions** and **{data['scala_test_definition_count']:,} annotated Scala methods**, preserving every scanned definition in [the JSON inventory](../conformance/hail-test-inventory.json).", "",
        f"The two dedicated Python suites contain **214 definitions**. Each has a source-reviewed description below. The broader scan retains **{len(extra_python)} additional Python candidates** and **{len(scala)} Scala candidates** in [the supplementary matrix](HAIL_TEST_INVENTORY_EXTRA.md); those counts include mixed APIs, shared expression tests and backend boundaries, not just Table-only cases. Across the matrices, **{review_count} requirements are source-reviewed** and {len(candidates) - review_count} remain static inference.", "",
        "The [implementation task map](TEST_IMPLEMENTATION_TASKS.md) explains dependencies and agent assignments. The JSON additionally records operation names, work packages, available primitive subsets, helper/fixture provenance, decorators and execution evidence. Every source link is pinned to the audited revision.", "",
        "## Dedicated-suite scope", "", "| Scope | Definitions |", "|---|---:|",
    ]
    lines += [f"| {scope} | {count} |" for scope, count in sorted(scopes.items())]
    remaining = len(dedicated_rows) - body_results["passed_body"]
    pure_remaining = sum(r["reviewed_scope"] == "table_only" and r.get("execution_evidence", {}).get("outcome") != "passed_body" for r in dedicated_rows)
    passed_cases = (data.get("execution_evidence_summary") or {}).get("passed", 0)
    lines += ["", f"**{pure_remaining} of the {scopes['table_only']} dedicated Table-only definitions still have unimplemented or unexecuted assertion scope.** Across all dedicated-suite scopes, {remaining} definitions lack a complete passing source body. **{body_results['passed_body']} complete source bodies pass.** `test_table_head_and_tail` additionally has 80 passing `rt`/`par` cases and 80 remaining cache/checkpoint cases. That is **{passed_cases} passing concrete cases**, not {passed_cases} complete definitions.", ""]
    lines += common
    lines += ["## Per-test implementation requirements", ""] + render_groups(dedicated_rows, pin, dedicated=True)
    lines += [
        "## Reproduce and limits", "", "Run from the Vail project directory:", "", "```sh", "uv run --no-sync python -m conformance.upstream_table_cases", "uv run --no-sync python scripts/inventory_hail_tests.py", "```", "",
        "The generator reads the sibling Hail checkout and the source-reviewed requirement overlays in `conformance/`. It verifies source paths/lines, unique IDs, pinned counts, reviewed source hashes, and conformance evidence source hashes. Regenerate the test-body evidence before the inventory if implementation files change.", "",
        "The source universe is Python `test_*.py` under `hail/python/test/hail` and Scala JUnit `@Test`/`@ParameterizedTest` methods under `hail/hail/test/src`. Doctests, generated methods, dynamic parameter providers and helpers outside the resolved graph are not claimed complete. Outside-candidate scope and remaining unresolved dependencies remain visible in the supplementary matrix and JSON. Scala implementation-specific plan assertions need explicit native equivalents. Legacy `.ht` decoding and compatibility are deferred by user direction; relevant semantic fixtures will be recreated directly in the new Vortex/Iceberg format.", "",
        "The storage implementation uses Iceberg Rust PR #2835. Its persisted snapshot-bound interval dictionary certifies primitive sorted write boundaries and schedules live files in key order without sorting rows on read. A fresh-reopen integration test uses three real Vortex data files. Range reads still enumerate all manifest tasks before pruning data files; Python storage APIs and broader key types remain required.", "",
    ]
    (root / "docs/HAIL_TEST_INVENTORY.md").write_text("\n".join(lines))
    extra = ["# Supplementary Hail Table test candidates", "",
             "See [the main inventory](HAIL_TEST_INVENTORY.md) for scope, evidence and the dedicated Table suites. This appendix retains shared expression tests, mixed Matrix/BlockMatrix/Spark/backend cases and static candidates so none are silently counted as a confirmed Table-only pass.", ""]
    extra += common + ["## Other Python test modules", ""] + render_groups(extra_python, pin)
    extra += ["## Native Table and related Scala regressions", ""] + render_groups(scala, pin)
    (root / "docs/HAIL_TEST_INVENTORY_EXTRA.md").write_text("\n".join(extra))
    data["review_summary"] = {
        "dedicated_scope_counts": dict(sorted(scopes.items())),
        "all_candidate_scope_counts": dict(sorted(all_scopes.items())),
        "source_reviewed_candidate_count": review_count,
        "additional_python_candidate_count": len(extra_python),
        "scala_candidate_count": len(scala),
        "dedicated_definitions_without_full_body_evidence": remaining,
        "dedicated_table_only_definitions_without_full_body_evidence": pure_remaining,
        "evidence_definition_counts": dict(body_results),
    }
