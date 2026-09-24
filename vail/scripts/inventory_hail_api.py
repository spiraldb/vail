#!/usr/bin/env python3
"""Inventory the pinned Hail Table surface without importing Hail."""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
from pathlib import Path


def signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    rendered = "(" + ast.unparse(node.args) + ")"
    if node.returns:
        rendered += " -> " + ast.unparse(node.returns)
    return rendered


def decorator_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    return [ast.unparse(decorator) for decorator in node.decorator_list]


def kind(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    if any(ast.unparse(decorator) == "property" for decorator in node.decorator_list):
        return "property"
    if any(ast.unparse(decorator).endswith(".setter") for decorator in node.decorator_list):
        return "property_setter"
    return "method"


def ir_dependencies(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    found = set()
    for item in ast.walk(node):
        if isinstance(item, ast.Attribute) and isinstance(item.value, ast.Name) and item.value.id == "ir":
            found.add(item.attr)
    return sorted(found)


def function_record(node, package_root: Path, role: str, inherited_by: list[str] | None = None):
    deps = ir_dependencies(node)
    record = {
        "name": node.name,
        "kind": kind(node),
        "role": role,
        "signature": signature(node),
        "source_line": node.lineno,
        "doc_first_line": (ast.get_docstring(node, clean=True) or "").splitlines()[0].strip()
        if ast.get_docstring(node, clean=True)
        else "",
        "decorators": decorator_names(node),
        "direct_ir_dependencies": deps,
        "ir_mapping_status": "direct-ir-reference-found" if deps else "unknown-no-direct-ir-reference",
        "transitive_ir_dependencies": "unknown: helper calls and expression/aggregator expansion were not resolved",
    }
    if inherited_by is not None:
        record["inherited_by"] = inherited_by
    return record


def rel_source(path: Path, package_root: Path) -> str:
    return path.relative_to(package_root.parent).as_posix()


def class_records(
    tree: ast.Module,
    name: str,
    package_root: Path,
    *,
    include_private: bool = False,
    special: set[str] | None = None,
):
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            records = []
            for member in node.body:
                if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not include_private and member.name.startswith("_") and member.name not in (special or set()):
                    continue
                records.append(function_record(member, package_root, "direct"))
            return {"source_line": node.lineno, "members": records}
    raise ValueError(f"class {name!r} not found")


def standalone_record(path: Path, name: str, package_root: Path, role: str):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            record = function_record(node, package_root, role)
            record["source"] = rel_source(path, package_root)
            return record
    return {
        "name": name,
        "role": role,
        "mapping_status": "unknown-not-found-in-pinned-source",
        "source": rel_source(path, package_root),
        "direct_ir_dependencies": [],
        "transitive_ir_dependencies": "unknown",
    }



def public_inventory(source_argument: Path, tree: ast.Module) -> dict:
    """Keep the original compact public API inventory format for research consumers."""
    result = {"source": source_argument.as_posix(), "classes": {}}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name not in {"Table", "GroupedTable"}:
            continue
        members = []
        for member in node.body:
            if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if member.name.startswith("_") and member.name != "__getitem__":
                continue
            is_property = any(
                ast.unparse(decorator) == "property" or ast.unparse(decorator).endswith(".setter")
                for decorator in member.decorator_list
            )
            doc = ast.get_docstring(member, clean=True) or ""
            members.append({
                "name": member.name,
                "kind": "property" if is_property else "method",
                "signature": "" if is_property else signature(member),
                "source_line": member.lineno,
                "doc_first_line": doc.splitlines()[0].strip() if doc else "",
            })
        result["classes"][node.name] = {"source_line": node.lineno, "public_api": members}
    return result


def git_commit_for(path: Path) -> str:
    for directory in (path.parent, *path.parents):
        if (directory / ".git").exists():
            try:
                return subprocess.check_output(
                    ["git", "-C", str(directory), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
                ).strip()
            except (OSError, subprocess.CalledProcessError):
                continue
    return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Path to hail/python/hail/table.py")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--format", choices=("public", "ledger"), default="public")
    parser.add_argument("--ledger", action="store_true", help="Alias for --format ledger")
    parser.add_argument("--commit", help="Source commit override; defaults to the checked-out Hail git SHA")
    args = parser.parse_args()
    source = args.source.resolve()
    package_root = source.parent
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    base = class_records(tree, "BaseTable", package_root, include_private=True)
    # BaseTable protocols and dynamic field support are inherited; Table.__getitem__ overrides none of BaseTable.
    for record in base["members"]:
        record["source"] = rel_source(source, package_root)
        if record["name"] == "__init__":
            record["role"] = "base-constructor"
            record["inherited_by"] = []
        else:
            record["role"] = "inherited-protocol" if record["name"].startswith("__") else "inherited-support"
            record["inherited_by"] = ["Table", "GroupedTable"]
    classes = {}
    table = class_records(tree, "Table", package_root, special={"__init__", "__getitem__", "_from_java"})
    grouped = class_records(tree, "GroupedTable", package_root, special={"__init__"})
    for name, record in (("Table", table), ("GroupedTable", grouped)):
        for member in record["members"]:
            member["source"] = rel_source(source, package_root)
            if member["name"] == "__init__":
                member["role"] = "constructor"
            elif member["name"] == "_from_java":
                member["role"] = "private-factory"
            elif member["name"] == "parallelize":
                member["role"] = "constructor"
        record["bases"] = [ast.unparse(base) for base in next(
            node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name
        ).bases]
        classes[name] = record
    base["bases"] = []
    classes["BaseTable"] = base

    impex_path = package_root / "methods" / "impex.py"
    misc_path = package_root / "utils" / "misc.py"
    standalone = [
        standalone_record(impex_path, "read_table", package_root, "reader-constructor"),
        standalone_record(misc_path, "range_table", package_root, "constructor-helper"),
    ]
    ledger = {
        "source_commit": args.commit or git_commit_for(source),
        "scope": "AST-only inventory; does not import Hail. Direct Table/GroupedTable declarations plus BaseTable inherited methods/protocols, Table constructors/factory, read_table and range_table.",
        "limitations": [
            "Direct AST ir.* references are listed; transitive helper, expression, aggregator, and backend dependencies are explicitly unknown.",
            "Overload declarations are retained as separate signatures when present.",
            "Dynamic fields produced at runtime are not statically enumerated; BaseTable field lookup/set/delete behavior is represented by protocol methods.",
            "This inventory is not a claim of executable or complete compatibility.",
        ],
        "classes": classes,
        "standalone": standalone,
    }
    inventory = ledger if args.ledger or args.format == "ledger" else public_inventory(args.source, tree)
    rendered = json.dumps(inventory, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
