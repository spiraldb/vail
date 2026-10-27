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
import hashlib
import json
import multiprocessing as mp
import os
from queue import Empty
import shutil
import signal
from pathlib import Path
import tempfile
import time
import traceback
from urllib.request import urlretrieve


ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = Path(os.environ.get(
    "VAIL_BENCHMARK_UPSTREAM",
    ROOT.parent / "hail" / "hail" / "python" / "benchmark" / "hail",
)).expanduser().resolve()
SOURCES = tuple(sorted(path.name for path in UPSTREAM.glob("benchmark_*.py")))


def _parameter_value(node):
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError):
        if isinstance(node, (ast.List, ast.Tuple)):
            return [_parameter_value(x) for x in node.elts]
        if isinstance(node, ast.Name):
            return {"__benchmark_symbol__": node.id}
        raise UnsupportedBenchmarkInput("Unrecognized parameter syntax: " + ast.unparse(node))


def _parameters(node: ast.FunctionDef) -> list[tuple]:
    dimensions = []
    for decorator in node.decorator_list:
        if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "parametrize" and len(decorator.args) >= 2):
            values = _parameter_value(decorator.args[1])
            raw_names = ast.literal_eval(decorator.args[0])
            names = raw_names.split(",") if isinstance(raw_names, str) else raw_names
            dimensions.append([(value,) if len(names) == 1 else tuple(value) for value in values])
    return [tuple(value for dimension in combination for value in dimension)
            for combination in itertools.product(*dimensions)] if dimensions else [()]


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
            fixture_args = [name for name in fixture_args if name not in explicit_names or name in fixture_params]
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
                    "arguments": dict(zip(explicit_order, values[:test_parameter_count])),
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


def _make_original_exact_fixtures(hl, engine: str, names: list[str], data_dir: Path,
                         fixture_parameters: dict) -> dict:
    """Use upstream fixture dimensions and published input files."""
    engine_dir = data_dir / engine
    engine_dir.mkdir(parents=True, exist_ok=True)
    paths = {}

    def is_complete(path: Path, kind: str) -> bool:
        marker = "_SUCCESS" if engine == "hail" else f"_vail_{kind}.json"
        return path.is_dir() and (path / marker).is_file()

    def remove_incomplete(path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

    def table_fixture(name, build, path_name=None):
        path = engine_dir / f"{path_name or name}.ht"
        if not is_complete(path, "table"):
            remove_incomplete(path)
            build().write(str(path), overwrite=True)
        paths[name] = path

    def matrix_fixture(name, build):
        path = engine_dir / f"{name}.mt"
        if not is_complete(path, "matrix"):
            remove_incomplete(path)
            build().write(str(path), overwrite=True)
        paths[name] = path

    def readable_text(path):
        # Read benchmarks use native table fixtures. Decompress identical input
        # during fixture preparation only; compressed import benchmarks retain
        # the original path and therefore still report unsupported compression.
        if engine != "vail" or not any(name.endswith("_ht") for name in names):
            return path
        import gzip
        output = path.with_suffix("")
        if not output.exists():
            temporary = output.with_name(output.name + ".decompressing")
            try:
                with gzip.open(path, "rb") as source, temporary.open("wb") as target:
                    shutil.copyfileobj(source, target, length=1024*1024)
                temporary.replace(output)
            finally:
                temporary.unlink(missing_ok=True)
        return output

    if any(name in names for name in ("many_ints_tsv", "many_ints_ht")):
        ints_tsv = _download_fixture(data_dir, "many_ints_table/many_ints_table.tsv.bgz")
        paths["many_ints_tsv"] = ints_tsv
        if "many_ints_ht" in names:
            table_fixture("many_ints_ht", lambda: hl.import_table(
                str(readable_text(ints_tsv)), types={"idx": "int", **{f"i{i}": "int" for i in range(5)},
                                      **{f"array{i}": "array<int>" for i in range(2)}}))

    if any(name in names for name in ("many_strings_tsv", "many_strings_ht")):
        strings_tsv = _download_fixture(data_dir, "many_strings_table/many_strings_table.tsv.bgz")
        paths["many_strings_tsv"] = strings_tsv
        if "many_strings_ht" in names:
            table_fixture("many_strings_ht", lambda: hl.import_table(str(readable_text(strings_tsv))))

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

    if "profile25_vcf" in names:
        paths["profile25_vcf"] = _download_fixture(data_dir, "profile25/profile.vcf.bgz")
    if "balding_nichols_5k_5k" in names:
        matrix_fixture("balding_nichols_5k_5k", lambda: hl.balding_nichols_model(
            n_populations=5, n_variants=5000, n_samples=5000, n_partitions=16))
    for name, filename in (("onethreetwo", "0.2.132.jsonl"), ("onethreethree", "0.2.133.jsonl")):
        if name in names:
            paths[name] = _download_fixture(data_dir, filename)
    if "empty_gvcf" in names:
        _download_fixture(data_dir, "empty_gvcf/empty.g.vcf.bgz.tbi")
        paths["empty_gvcf"] = _download_fixture(data_dir, "empty_gvcf/empty.g.vcf.bgz")
    if any(name in names for name in ("single_gvcf", "chr22_gvcfs")):
        import subprocess
        archive = _download_fixture(data_dir, "1kg_chr22.tar")
        extracted = data_dir / "1kg_chr22"
        marker = extracted / ".extracted"
        if not marker.exists():
            extracted.mkdir(exist_ok=True)
            subprocess.check_call(["tar", "-xf", str(archive), "-C", str(extracted), "--strip-components=1"])
            marker.touch()
        if "single_gvcf" in names:
            paths["single_gvcf"] = extracted / "NA20760.hg38.g.vcf.gz"
        if "chr22_gvcfs" in names:
            tree = ast.parse((UPSTREAM / "fixtures.py").read_text())
            fixture = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "chr22_gvcfs")
            paths["chr22_gvcfs"] = [str(extracted / (arg.arg + ".hg38.g.vcf.gz")) for arg in fixture.args.args]
    if "_100_instances_100_iterations" in names:
        def make_instances():
            rows = lambda n, _: [hl.struct(id=0, instances=hl.repeat(hl.repeat(1.0, n), n))]
            table = hl.Table._generate(contexts=[100], partitions=1, rowfn=rows)
            return table._key_by_assert_sorted(table.id)
        table_fixture("_100_instances_100_iterations", make_instances)
    # These fixtures are fresh paths local to each worker, installed below.
    for name in ("local_tmpdir", "tmp_and_output_paths"):
        if name in names:
            paths[name] = "__worker_temporary__"
    missing = [name for name in names if name not in paths]
    if missing:
        raise UnsupportedBenchmarkInput("no exact fixture adapter for: " + ", ".join(missing))
    return {name: paths[name] if isinstance(paths[name], list) else str(paths[name]) for name in names}



def _fixture_manifest(data_dir, names, parameters):
    key = hashlib.sha256(json.dumps([sorted(names), parameters], sort_keys=True).encode()).hexdigest()
    return Path(data_dir) / "shared-manifests" / (key + ".json")


def _make_exact_fixtures(hl, engine, names, data_dir, fixture_parameters):
    # Spawned workers import this file, so the adapter is installed in every worker.
    manifest = _fixture_manifest(data_dir, names, fixture_parameters)
    if engine == "hail":
        paths = _make_original_exact_fixtures(hl, "hail", names, data_dir, fixture_parameters)
        _write_report(manifest, {"paths": paths, "origin": "hail-written", "parameters": fixture_parameters})
        return paths
    if not names:
        return {}
    if not manifest.exists():
        raise UnsupportedBenchmarkInput("Shared Hail fixture preparation did not complete: " + str(manifest))
    paths = json.loads(manifest.read_text())["paths"]
    if os.environ.get("VAIL_SCAN_MODE") == "vail-hail":
        return paths
    converted = {}
    for name, raw in paths.items():
        if isinstance(raw, list):
            converted[name] = raw
            continue
        source = Path(raw)
        kind = "table" if source.suffix == ".ht" else "matrix" if source.suffix == ".mt" else None
        if kind is None:
            converted[name] = raw
            continue
        # Preserve source-relative identity, including partition-count variants.
        target = Path(data_dir) / "vail-from-hail" / source.relative_to(Path(data_dir) / "hail")
        marker = target / f"_vail_{kind}.json"
        if not marker.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            reader = hl.Table.read_legacy if kind == "table" else hl.MatrixTable.read_legacy
            reader(raw).write(str(target), overwrite=True)
        converted[name] = str(target)
    return converted


def _install_legacy_boundary(hl):
    native_table, native_matrix = hl.read_table, hl.read_matrix_table
    def table(path, **kwargs):
        if (Path(path) / "_vail_table.json").is_file():
            return native_table(path, **kwargs)
        if kwargs:
            raise NotImplementedError("Legacy benchmark boundary does not implement read_table options: " + repr(kwargs))
        return hl.Table.read_legacy(path)
    def matrix(path, **kwargs):
        if (Path(path) / "_vail_matrix.json").is_file():
            return native_matrix(path, **kwargs)
        if kwargs:
            raise NotImplementedError("Legacy benchmark boundary does not implement read_matrix_table options: " + repr(kwargs))
        return hl.MatrixTable.read_legacy(path)
    hl.read_table = table
    hl.read_matrix_table = matrix


def _worker(case: dict, engine: str, smoke: bool, data_dir: str, queue, start_event=None) -> None:
    # Hail starts a JVM (and potentially other child processes). Put the whole
    # worker tree in its own process group so a timed-out case cannot leave
    # Spark behind and contaminate later measurements.
    os.setsid()
    fixture_temp = None
    benchmark_started = None
    benchmark_seconds = None
    telemetry = None
    measurement = None
    fixture_measurement = None
    try:
        import importlib
        if engine == "vail":
            import vail
        if engine == "hail":
            hl = importlib.import_module("hail")
            cores = int(os.environ.get("HAIL_BENCHMARK_CORES", os.cpu_count() or 1))
            memory = os.environ.get("HAIL_BENCHMARK_MEMORY", "16g")
            hl.init(quiet=True, master=f"local[{cores}]",
                    spark_conf={"spark.driver.memory": memory, "spark.executor.memory": memory,
                                "spark.ui.enabled": "false"})
        else:
            hl = vail
        if engine == "vail" and os.environ.get("VAIL_SCAN_MODE") == "vail-hail":
            _install_legacy_boundary(hl)
        if engine == "vail" and (case["module"] == "benchmark_analysis.py" or (case["module"] == "benchmark_combiner.py" and case["name"] != "test_import_gvcf_force_count")):
            raise UnsupportedBenchmarkInput("Upstream analysis/combiner helper modules hardcode Hail; no verified Vail adapter")
        source = (UPSTREAM / case["module"]).read_text()
        tree = ast.parse(source)
        body = []
        for node in tree.body:
            if isinstance(node, ast.Import):
                names = [n for n in node.names if n.name not in {"hail"}]
                if names:
                    node.names = names
                    body.append(node)
            elif isinstance(node, ast.ImportFrom):
                if engine == "vail" and case["module"] == "benchmark_combiner.py" and node.module and (node.module.startswith("hail.") or node.module == "test.hail.helpers"):
                    continue
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
        telemetry_root = os.environ.get("VAIL_BENCHMARK_TELEMETRY_DIR")
        if telemetry_root:
            from benchmark_telemetry import Measurement
            label = case["id"].replace("::", "--").replace("/", "_")
            fixture_measurement = Measurement(
                Path(telemetry_root) / engine / label / "fixture"
            ).start()
        if smoke:
            fixture_values = _make_smoke_fixtures(hl, case["fixtures"], Path(fixture_temp.name), case["fixture_parameters"])
        else:
            fixture_values = _make_exact_fixtures(hl, engine, case["fixtures"], Path(data_dir), case["fixture_parameters"])
        if fixture_measurement is not None:
            fixture_measurement.stop()
            fixture_measurement = None
        if os.environ.get("VAIL_SCAN_PREPARE_ONLY") == "1":
            queue.put({"status": "prepared", "fixtures": fixture_values})
            return
        kwargs = {name: namespace[value["__benchmark_symbol__"]] if isinstance(value, dict) and "__benchmark_symbol__" in value else value for name, value in case["arguments"].items()}
        kwargs.update(fixture_values)
        with tempfile.TemporaryDirectory(prefix="vail-upstream-bench-") as temp:
            if "tmp_path" in case["args"]:
                kwargs["tmp_path"] = Path(temp)
            if "local_tmpdir" in case["args"]:
                kwargs["local_tmpdir"] = Path(temp)
            if "tmp_and_output_paths" in case["args"]:
                tmp, output = Path(temp) / "tmp", Path(temp) / "output"
                tmp.mkdir(); output.mkdir()
                kwargs["tmp_and_output_paths"] = (tmp, output)
            if "_100_instances_100_iterations" in kwargs:
                kwargs["_100_instances_100_iterations"] = hl.read_table(kwargs["_100_instances_100_iterations"])
            queue.put({"status": "ready"})
            if start_event is not None:
                start_event.wait()
            telemetry_root = os.environ.get("VAIL_BENCHMARK_TELEMETRY_DIR")
            if telemetry_root:
                from benchmark_telemetry import Measurement
                label = case["id"].replace("::", "--").replace("/", "_")
                measurement = Measurement(Path(telemetry_root) / engine / label).start()
            benchmark_started = time.perf_counter()
            try:
                fn(**kwargs)
            except BaseException:
                benchmark_seconds = time.perf_counter() - benchmark_started
                if measurement is not None:
                    telemetry = measurement.stop()
                    measurement = None
                raise
            benchmark_seconds = time.perf_counter() - benchmark_started
            if measurement is not None:
                telemetry = measurement.stop()
                measurement = None
        queue.put({"status": "passed", "benchmark_seconds": benchmark_seconds, "telemetry": telemetry})
    except UnsupportedBenchmarkInput as exc:
        queue.put({"status": "unsupported", "reason": str(exc)})
    except BaseException as exc:
        result = {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()}
        if benchmark_seconds is not None:
            result["benchmark_seconds"] = benchmark_seconds
        if telemetry is not None:
            result["telemetry"] = telemetry
        queue.put(result)
    finally:
        if fixture_measurement is not None:
            fixture_measurement.stop()
        if fixture_temp is not None:
            fixture_temp.cleanup()
        if engine == "hail":
            try:
                hl.stop()
            except Exception:
                pass


def _stop_worker_tree(process, grace_seconds: float = 5.0) -> None:
    """Terminate and reap the worker and any JVM/Spark descendants."""
    if not process.is_alive():
        # An aborted Python worker can leave JVM descendants in its group.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        process.join()
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        # If the child has not reached setsid yet, kill its process directly.
        process.terminate()
    process.join(grace_seconds)
    if process.is_alive():
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            process.kill()
        process.join(grace_seconds)


class WorkerExitedError(RuntimeError):
    pass


def _worker_message(queue, process, timeout):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise Empty
        try:
            return queue.get(timeout=min(0.2, remaining))
        except Empty:
            if not process.is_alive():
                raise WorkerExitedError(f"worker exited with code {process.exitcode} before reporting a result")


def _signal_fixture_prefetch(signum):
    raw = os.environ.get("VAIL_BENCHMARK_PREFETCH_PID")
    if not raw:
        return
    import psutil
    try:
        process = psutil.Process(int(raw))
        if process.create_time() == float(os.environ["VAIL_BENCHMARK_PREFETCH_CREATED"]):
            process.send_signal(signum)
    except psutil.Error:
        pass


def run_one(case: dict, engine: str, timeout: float, fixture_timeout: float,
            smoke: bool, data_dir: str) -> dict:
    upstream_skip = next((mark for mark in case["upstream_marks"] if mark["name"] == "skip"), None)
    if upstream_skip:
        return {"status": "upstream_skipped", "reason": upstream_skip["reason"]}
    queue = mp.get_context("spawn").Queue()
    start_event = mp.get_context("spawn").Event()
    process = mp.get_context("spawn").Process(target=_worker, args=(case, engine, smoke, data_dir, queue, start_event))
    started = time.monotonic()
    process.start()
    try:
        first = _worker_message(queue, process, fixture_timeout)
    except Exception as exc:
        _stop_worker_tree(process)
        queue.close()
        if isinstance(exc, WorkerExitedError):
            return {"status": "failed", "error": str(exc), "worker_exitcode": process.exitcode,
                    "phase": "awaiting_fixture_or_start", "elapsed_seconds": round(time.monotonic() - started, 3)}
        return {"status": "fixture_timeout", "fixture_timeout_seconds": fixture_timeout,
                "elapsed_seconds": round(time.monotonic() - started, 3)}
    if first.get("status") != "ready":
        process.join(1)
        if process.is_alive():
            _stop_worker_tree(process)
        queue.close()
        first["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return first
    _signal_fixture_prefetch(signal.SIGSTOP)
    start_event.set()
    benchmark_started = time.monotonic()
    try:
        try:
            result = _worker_message(queue, process, timeout)
        finally:
            _signal_fixture_prefetch(signal.SIGCONT)
    except Exception as exc:
        _stop_worker_tree(process)
        queue.close()
        if isinstance(exc, WorkerExitedError):
            return {"status": "failed", "error": str(exc), "worker_exitcode": process.exitcode,
                    "phase": "benchmark", "elapsed_seconds": round(time.monotonic() - started, 3)}
        return {"status": "timed_out", "timeout_seconds": timeout,
                "fixture_seconds": round(benchmark_started - started, 3),
                "elapsed_seconds": round(time.monotonic() - started, 3)}
    process.join(5)
    if process.is_alive():
        _stop_worker_tree(process)
        # The benchmark result is already complete. Spark shutdown can exceed
        # the grace period; record forced cleanup separately from query status.
        result["worker_cleanup_forced"] = True
    queue.close()
    result["fixture_seconds"] = round(benchmark_started - started, 3)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(path)


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
    parser.add_argument("--resume", action="store_true",
                        help="Retain completed case results from --output and run only missing cases.")
    parser.add_argument("--list", action="store_true", help="Print discovered cases and exit.")
    args = parser.parse_args()
    if args.resume and not args.output:
        parser.error("--resume requires --output PATH")
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
    report = {"runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "upstream_sha256": {name: hashlib.sha256((UPSTREAM / name).read_bytes()).hexdigest()
                                  for name in (*SOURCES, "fixtures.py")},
              "engine": args.engine, "timeout_seconds": args.timeout,
              "fixture_timeout_seconds": args.fixture_timeout, "data_dir": str(args.data_dir),
              "smoke_fixtures": args.smoke, "discovered": len(cases), "cases": []}
    completed = {}
    if args.resume and args.output and args.output.exists():
        previous = json.loads(args.output.read_text())
        if (previous.get("engine") != args.engine or previous.get("smoke_fixtures") != args.smoke
                or previous.get("data_dir") != str(args.data_dir)):
            parser.error("--resume report must use the same engine, smoke mode, and data directory")
        completed = {item["id"]: item for item in previous.get("cases", []) if "id" in item and "status" in item}
        report["cases"] = [completed[case["id"]] for case in cases if case["id"] in completed]
    for i, case in enumerate(cases, 1):
        if case["id"] in completed:
            print(f"[{i}/{len(cases)}] {case['id']}: retained {completed[case['id']]['status']}", flush=True)
            continue
        timeout = max([args.timeout, *(mark["args"][0] for mark in case["upstream_marks"]
                                      if mark["name"] == "xtimeout" and mark["args"]
                                      and isinstance(mark["args"][0], (int, float)))])
        if os.environ.get("VAIL_SCAN_MODE"):
            timeout = args.timeout  # Cloud scan imposes an explicit hard cap.
        result = run_one(case, args.engine, timeout, args.fixture_timeout, args.smoke, str(args.data_dir))
        report["cases"].append({**case, **result})
        report["cases"].sort(key=lambda item: next(i for i, original in enumerate(cases) if original["id"] == item["id"]))
        report["totals"] = {status: sum(item["status"] == status for item in report["cases"])
                             for status in sorted({item["status"] for item in report["cases"]})}
        if args.output:
            _write_report(args.output, report)
        print(f"[{i}/{len(cases)}] {case['id']}: {result['status']}", flush=True)
    report["totals"] = {status: sum(item["status"] == status for item in report["cases"])
                         for status in sorted({item["status"] for item in report["cases"]})}
    output = json.dumps(report, indent=2) + "\n"
    if args.output:
        _write_report(args.output, report)
    else:
        print(output)


if __name__ == "__main__":
    main()
