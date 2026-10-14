#!/usr/bin/env python3
"""Inventory and bounded attempts for upstream Hail table benchmarks.

The upstream benchmark files are loaded as source with only their imports and
pytest decorators removed. Each case runs in a fresh child process so a stuck
benchmark cannot prevent later cases from receiving an explicit result.
"""
from __future__ import annotations

import argparse
import ast
import itertools
import json
import multiprocessing as mp
import os
from pathlib import Path
import tempfile
import time
import traceback
from urllib.request import urlretrieve


ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT.parent / "hail" / "hail" / "python" / "benchmark" / "hail"
SOURCES = ("benchmark_table.py", "benchmark_matrix_table.py")


def _parameters(node: ast.FunctionDef) -> list[tuple]:
    dimensions = []
    for decorator in node.decorator_list:
        if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "parametrize" and len(decorator.args) >= 2):
            values = ast.literal_eval(decorator.args[1])
            dimensions.append(values)
    return list(itertools.product(*dimensions)) if dimensions else [()]


def _fixture_parameters() -> dict[str, list]:
    tree = ast.parse((UPSTREAM / "fixtures.py").read_text())
    result = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute) and decorator.func.attr == "fixture":
                for keyword in decorator.keywords:
                    if keyword.arg == "params":
                        result[node.name] = ast.literal_eval(keyword.value)
    # The benchmark module aliases this fixture under two argument names.
    result["many_partitions_ht1"] = result["many_partitions_ht"]
    result["many_partitions_ht2"] = result["many_partitions_ht"]
    return result


def inventory() -> list[dict]:
    cases = []
    fixture_params = _fixture_parameters()
    for filename in SOURCES:
        source = UPSTREAM / filename
        tree = ast.parse(source.read_text())
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            fixture_args = [arg.arg for arg in node.args.args if arg.arg not in {"tmp_path", "n", "n_partitions", "m"}]
            marks = []
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
                    if decorator.func.attr in {"skip", "xtimeout"}:
                        marks.append({
                            "name": decorator.func.attr,
                            "reason": next((ast.literal_eval(k.value) for k in decorator.keywords if k.arg == "reason"), None),
                            "args": [ast.literal_eval(arg) for arg in decorator.args],
                        })
                elif isinstance(decorator, ast.Attribute) and decorator.attr in {"skip", "xtimeout"}:
                    marks.append({"name": decorator.attr, "reason": None, "args": []})
            explicit_names = set()
            explicit_order = []
            for decorator in node.decorator_list:
                if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                        and decorator.func.attr == "parametrize" and decorator.args):
                    raw_names = ast.literal_eval(decorator.args[0])
                    names = [x.strip() for x in raw_names.split(",")]
                    explicit_names.update(names)
                    explicit_order.extend(names)
            fixture_dimensions = [fixture_params[name] for name in fixture_args
                                  if name in fixture_params and name not in explicit_names]
            case_values = [tuple(a) + tuple(b) for a, b in itertools.product(_parameters(node), itertools.product(*fixture_dimensions))] if fixture_dimensions else _parameters(node)
            for values in case_values:
                test_parameter_count = len(explicit_order)
                fixture_parameter_values = {
                    name: values[explicit_order.index(name)] for name in fixture_args
                    if name in fixture_params and name in explicit_order
                }
                fixture_parameter_values.update({
                    name: value for name, value in zip(
                        [arg for arg in fixture_args if arg in fixture_params and arg not in explicit_names],
                        values[test_parameter_count:],
                    )
                })
                cases.append({
                    "module": filename,
                    "name": node.name,
                    "id": f"{filename.removesuffix('.py')}::{node.name}" + (f"[{','.join(map(str, values))}]" if values else ""),
                    "args": [arg.arg for arg in node.args.args],
                    "fixtures": fixture_args,
                    "parameters": values,
                    "fixture_parameters": fixture_parameter_values,
                    "upstream_marks": marks,
                })
    return cases


class UnsupportedBenchmarkInput(Exception):
    pass


def _make_smoke_fixtures(hl, names: list[str], directory: Path, fixture_parameters: dict) -> dict:
    """Create reduced fixtures for a smoke check, never for timing claims.

    These are execution-coverage fixtures, not substitutes for the upstream
    benchmark's published data sizes. BGEN requires real external input.
    """
    paths = {}

    def write_table(name, table):
        path = directory / f"{name}.ht"
        table.write(str(path), overwrite=True)
        paths[name] = path

    def write_matrix(name, matrix):
        path = directory / f"{name}.mt"
        matrix.write(str(path), overwrite=True)
        paths[name] = path

    if any(n in names for n in ("many_ints_ht", "many_strings_ht", "many_partitions_ht", "many_partitions_ht1", "many_partitions_ht2")):
        table = hl.utils.range_table(10_000, 16)
        table = table.annotate(**{f"i{i}": table.idx + i for i in range(5)},
                               **{f"array{i}": hl.array([table.idx, table.idx + 1]) for i in range(2)},
                               **{f"f{i}": hl.str(table.idx % 1000) for i in range(19)})
        if "many_ints_ht" in names:
            write_table("many_ints_ht", table)
        if "many_strings_ht" in names:
            write_table("many_strings_ht", table)
        for name in ("many_partitions_ht", "many_partitions_ht1", "many_partitions_ht2"):
            if name in names:
                partition_count = fixture_parameters.get(name, fixture_parameters.get("many_partitions_ht", 100))
                write_table(name, hl.utils.range_table(100_000, partition_count))

    if "profile25_mt" in names:
        mt = hl.utils.range_matrix_table(2_000, 100, 16)
        mt = mt.annotate_rows(locus=hl.locus("1", mt.row_idx + 1), alleles=["A", "C"],
                              info=hl.struct(AF=[0.25]))
        mt = mt.annotate_cols(s=hl.str(mt.col_idx))
        mt = mt.annotate_entries(GT=hl.call(0, 1), GQ=mt.row_idx % 40,
                                 DP=mt.col_idx % 50 + 1, AD=[4, 6], PL=[0, 10, 20])
        write_matrix("profile25_mt", mt)
    if "gnomad_dp_sim" in names:
        mt = hl.utils.range_matrix_table(5_000, 100, 32)
        mt = mt.annotate_entries(x=hl.int32(mt.row_idx % 101))
        write_matrix("gnomad_dp_sim", mt)
    if "random_doubles_mt" in names:
        mt = hl.utils.range_matrix_table(2_000, 100, 16)
        mt = mt.annotate_entries(x=hl.float64(mt.row_idx + mt.col_idx + 1) / 10)
        write_matrix("random_doubles_mt", mt)
    if any(n in names for n in ("many_ints_tsv", "many_strings_tsv")):
        ints = directory / "many_ints.tsv"
        strings = directory / "many_strings.tsv"
        int_header = ["idx", *(f"i{i}" for i in range(5)), *(f"array{i}" for i in range(2))]
        string_header = ["idx", *(f"f{i}" for i in range(19))]
        if "many_ints_tsv" in names:
            with ints.open("w") as out:
                out.write("\t".join(int_header) + "\n")
                for i in range(10_000):
                    out.write("\t".join(map(str, [i, *(i+j for j in range(5)), f"[{i}, {i+1}]", f"[{i}, {i+1}]" ])) + "\n")
            paths["many_ints_tsv"] = ints
        if "many_strings_tsv" in names:
            with strings.open("w") as out:
                out.write("\t".join(string_header) + "\n")
                for i in range(10_000):
                    out.write("\t".join([str(i), *(str((i+j) % 1000) for j in range(19))]) + "\n")
            paths["many_strings_tsv"] = strings
    if "sim_ukb_bgen" in names or "sim_ukb_sample" in names:
        bgen = os.environ.get("HAIL_BENCHMARK_SIM_UKB_BGEN")
        sample = os.environ.get("HAIL_BENCHMARK_SIM_UKB_SAMPLE")
        if "sim_ukb_bgen" in names:
            if not bgen or not Path(bgen).is_file():
                raise FileNotFoundError("fixture sim_ukb_bgen requires HAIL_BENCHMARK_SIM_UKB_BGEN")
            paths["sim_ukb_bgen"] = Path(bgen)
        if "sim_ukb_sample" in names:
            if not sample or not Path(sample).is_file():
                raise FileNotFoundError("fixture sim_ukb_sample requires HAIL_BENCHMARK_SIM_UKB_SAMPLE")
            paths["sim_ukb_sample"] = Path(sample)
    return {name: str(paths[name]) for name in names if name in paths}


def _download_fixture(data_dir: Path, relative_path: str) -> Path:
    path = data_dir / relative_path
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".download")
        try:
            # Upstream fixtures.localize passes only path.name to __download.
            urlretrieve(f"https://storage.googleapis.com/hail-common/benchmark/{path.name}", temporary)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    return path


def _make_exact_fixtures(hl, engine: str, names: list[str], data_dir: Path,
                         fixture_parameters: dict) -> dict:
    """Use upstream fixture dimensions and published input files."""
    engine_dir = data_dir / engine
    engine_dir.mkdir(parents=True, exist_ok=True)
    paths = {}

    def table_fixture(name, build, path_name=None):
        path = engine_dir / f"{path_name or name}.ht"
        if not path.exists():
            build().write(str(path), overwrite=True)
        paths[name] = path

    def matrix_fixture(name, build):
        path = engine_dir / f"{name}.mt"
        if not path.exists():
            build().write(str(path), overwrite=True)
        paths[name] = path

    if any(name in names for name in ("many_ints_tsv", "many_ints_ht")):
        ints_tsv = _download_fixture(data_dir, "many_ints_table/many_ints_table.tsv.bgz")
        paths["many_ints_tsv"] = ints_tsv
        if "many_ints_ht" in names:
            table_fixture("many_ints_ht", lambda: hl.import_table(
                str(ints_tsv), types={"idx": "int", **{f"i{i}": "int" for i in range(5)},
                                      **{f"array{i}": "array<int>" for i in range(2)}}))

    if any(name in names for name in ("many_strings_tsv", "many_strings_ht")):
        strings_tsv = _download_fixture(data_dir, "many_strings_table/many_strings_table.tsv.bgz")
        paths["many_strings_tsv"] = strings_tsv
        if "many_strings_ht" in names:
            table_fixture("many_strings_ht", lambda: hl.import_table(str(strings_tsv)))

    for name in ("many_partitions_ht", "many_partitions_ht1", "many_partitions_ht2"):
        if name in names:
            partitions = fixture_parameters.get(name, fixture_parameters.get("many_partitions_ht", 100))
            table_fixture(name, lambda partitions=partitions: hl.utils.range_table(10_000_000, partitions),
                          path_name=f"{name}-p{partitions}")

    if "profile25_mt" in names:
        vcf = _download_fixture(data_dir, "profile25/profile.vcf.bgz")
        matrix_fixture("profile25_mt", lambda: hl.import_vcf(str(vcf), min_partitions=16))

    if "gnomad_dp_sim" in names:
        if engine == "vail" and not hasattr(hl, "rand_unif"):
            raise UnsupportedBenchmarkInput("Vail does not implement upstream fixture expression rand_unif")
        def make_gnomad():
            mt = hl.utils.range_matrix_table(n_rows=250_000, n_cols=1_000, n_partitions=32)
            return mt.annotate_entries(x=hl.int(hl.rand_unif(0, 4.5) ** 3))
        matrix_fixture("gnomad_dp_sim", make_gnomad)

    if "random_doubles_mt" in names:
        if not hasattr(hl, "import_matrix_table"):
            raise UnsupportedBenchmarkInput("Vail has no import_matrix_table API for the upstream wide TSV fixture")
        tsv = _download_fixture(data_dir, "random_doubles/random_doubles_mt.tsv.bgz")
        matrix_fixture("random_doubles_mt", lambda: hl.import_matrix_table(
            str(tsv), row_key="row_idx", row_fields={"row_idx": hl.tint32}, entry_type=hl.tfloat64))

    if "sim_ukb_bgen" in names or "sim_ukb_sample" in names:
        if not hasattr(hl, "import_bgen"):
            raise UnsupportedBenchmarkInput("Vail does not implement BGEN import")
        bgen = _download_fixture(data_dir, "sim_ukb/sim_ukb.bgen")
        sample = _download_fixture(data_dir, "sim_ukb/sim_ukb.sample")
        hl.index_bgen(str(bgen))
        paths["sim_ukb_bgen"] = bgen
        paths["sim_ukb_sample"] = sample

    missing = [name for name in names if name not in paths]
    if missing:
        raise UnsupportedBenchmarkInput("no exact fixture adapter for: " + ", ".join(missing))
    return {name: str(paths[name]) for name in names}


def _worker(case: dict, engine: str, smoke: bool, data_dir: str, queue) -> None:
    fixture_temp = None
    try:
        import importlib
        import sys
        sys.path.insert(0, str(ROOT / "python"))
        if engine == "vail":
            import vail
        if engine == "hail":
            hl = importlib.import_module("hail")
            hl.init(quiet=True)
        else:
            hl = vail
        source = (UPSTREAM / case["module"]).read_text()
        tree = ast.parse(source)
        body = []
        for node in tree.body:
            if isinstance(node, ast.Import):
                names = [n for n in node.names if n.name not in {"pytest", "hail"}]
                if names:
                    node.names = names
                    body.append(node)
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith("benchmark.hail.fixtures"):
                    continue
                body.append(node)
            elif isinstance(node, ast.Assign) and any(
                isinstance(target, (ast.Name, ast.Tuple))
                and "many_partitions_ht1" in ast.unparse(target)
                for target in node.targets
            ):
                continue
            elif isinstance(node, ast.FunctionDef):
                if node.name.startswith("test_"):
                    node.decorator_list = []
                body.append(node)
            else:
                body.append(node)
        # Use the requested implementation under the same `hl` name.
        module = ast.Module(body=[ast.Import(names=[ast.alias(name=engine, asname="hl")])] + body, type_ignores=[])
        ast.fix_missing_locations(module)
        namespace = {"__name__": "upstream_benchmark_case"}
        exec(compile(module, case["module"], "exec"), namespace)
        fn = namespace[case["name"]]
        fixture_temp = tempfile.TemporaryDirectory(prefix="vail-upstream-fixtures-")
        if smoke:
            fixture_values = _make_smoke_fixtures(hl, case["fixtures"], Path(fixture_temp.name), case["fixture_parameters"])
        else:
            fixture_values = _make_exact_fixtures(hl, engine, case["fixtures"], Path(data_dir), case["fixture_parameters"])
        kwargs = {}
        for arg, value in zip((a for a in case["args"] if a not in {"tmp_path"}), case["parameters"]):
            kwargs[arg] = value
        kwargs.update(fixture_values)
        with tempfile.TemporaryDirectory(prefix="vail-upstream-bench-") as temp:
            if "tmp_path" in case["args"]:
                kwargs["tmp_path"] = Path(temp)
            queue.put({"status": "ready"})
            fn(**kwargs)
        queue.put({"status": "passed"})
    except UnsupportedBenchmarkInput as exc:
        queue.put({"status": "unsupported", "reason": str(exc)})
    except BaseException as exc:
        queue.put({"status": "failed", "error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()})
    finally:
        if fixture_temp is not None:
            fixture_temp.cleanup()
        if engine == "hail":
            try:
                hl.stop()
            except Exception:
                pass


def run_one(case: dict, engine: str, timeout: float, fixture_timeout: float,
            smoke: bool, data_dir: str) -> dict:
    upstream_skip = next((mark for mark in case["upstream_marks"] if mark["name"] == "skip"), None)
    if upstream_skip:
        return {"status": "upstream_skipped", "reason": upstream_skip["reason"]}
    queue = mp.get_context("spawn").Queue()
    process = mp.get_context("spawn").Process(target=_worker, args=(case, engine, smoke, data_dir, queue))
    started = time.monotonic()
    process.start()
    try:
        first = queue.get(timeout=fixture_timeout)
    except Exception:
        if process.is_alive():
            process.terminate()
            process.join(5)
        return {"status": "fixture_timeout", "fixture_timeout_seconds": fixture_timeout,
                "elapsed_seconds": round(time.monotonic() - started, 3)}
    if first.get("status") != "ready":
        process.join(1)
        first["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return first
    benchmark_started = time.monotonic()
    process.join(timeout)
    if process.is_alive():
        process.terminate()
        process.join(5)
        return {"status": "timed_out", "timeout_seconds": timeout,
                "fixture_seconds": round(benchmark_started - started, 3),
                "elapsed_seconds": round(time.monotonic() - started, 3)}
    try:
        result = queue.get(timeout=1)
    except Exception:
        result = {"status": "failed", "error": f"worker exited {process.exitcode} without a result"}
    result["fixture_seconds"] = round(benchmark_started - started, 3)
    result["benchmark_seconds"] = round(time.monotonic() - benchmark_started, 3)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("vail", "hail"), default="vail")
    parser.add_argument("--case", action="append", help="Exact case id; repeat to select. Default: every discovered case.")
    parser.add_argument("--timeout", type=float, default=120.0, help="Per-case timeout in seconds.")
    parser.add_argument("--fixture-timeout", type=float, default=1800.0,
                        help="Timeout for public downloads and native fixture materialization.")
    parser.add_argument("--data-dir", type=Path, default=Path(tempfile.gettempdir()) / "hail-upstream-benchmark-data",
                        help="Cache for exact upstream fixture downloads and native materializations.")
    parser.add_argument("--smoke", action="store_true", help="Use reduced synthetic fixtures. Never use for benchmark timing claims.")
    parser.add_argument("--output", type=Path, help="JSON report path; defaults to stdout.")
    parser.add_argument("--list", action="store_true", help="Print discovered cases and exit.")
    args = parser.parse_args()
    cases = inventory()
    if args.case:
        selected = set(args.case)
        cases = [case for case in cases if case["id"] in selected]
        missing = selected - {case["id"] for case in cases}
        if missing:
            parser.error("unknown case id(s): " + ", ".join(sorted(missing)))
    if args.list:
        print("\n".join(case["id"] for case in cases))
        return
    report = {"engine": args.engine, "timeout_seconds": args.timeout,
              "fixture_timeout_seconds": args.fixture_timeout, "data_dir": str(args.data_dir),
              "smoke_fixtures": args.smoke, "discovered": len(cases), "cases": []}
    for i, case in enumerate(cases, 1):
        timeout = max([args.timeout, *(mark["args"][0] for mark in case["upstream_marks"]
                                      if mark["name"] == "xtimeout" and mark["args"]
                                      and isinstance(mark["args"][0], (int, float)))])
        result = run_one(case, args.engine, timeout, args.fixture_timeout, args.smoke, str(args.data_dir))
        report["cases"].append({**case, **result})
        report["totals"] = {status: sum(item["status"] == status for item in report["cases"])
                             for status in sorted({item["status"] for item in report["cases"]})}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(f"[{i}/{len(cases)}] {case['id']}: {result['status']}", flush=True)
    report["totals"] = {status: sum(item["status"] == status for item in report["cases"])
                         for status in sorted({item["status"] for item in report["cases"]})}
    output = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output)


if __name__ == "__main__":
    main()
