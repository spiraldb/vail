"""Comparable local Hail/Vail workloads; metrics only, no query data in JSON.

Synthetic Table workloads follow Hail's benchmark_table.py patterns. Both engines
construct a fresh lazy query for every invocation and force equivalent output.
See the result report for scales and caveats. This runner does not build Hail.
Hail benchmark patterns: Copyright (c) 2015-2023 Hail Authors, MIT;
see ../licenses/HAIL.txt.
"""
from __future__ import annotations

import argparse
import gc
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import time
import traceback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', choices=['hail', 'vail'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workdir', type=Path, required=True)
    parser.add_argument('--scale', type=float, default=1.0)
    parser.add_argument('--matrix-rows', type=int, default=100_000)
    parser.add_argument('--matrix-columns', type=int, default=10)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--cores', type=int, default=14)
    parser.add_argument('--case', action='append')
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    imported_at = time.perf_counter()
    hl = importlib.import_module(args.engine)
    import_seconds = time.perf_counter() - imported_at
    initialized_at = time.perf_counter()
    settings = {}
    if args.engine == 'hail':
        hl.init(master=f'local[{args.cores}]', spark_conf={
            'spark.driver.memory': '48g', 'spark.executor.memory': '48g',
            'spark.driver.maxResultSize': '8g', 'spark.ui.enabled': 'false',
            'spark.sql.shuffle.partitions': str(args.cores),
        }, log=str(args.workdir / 'hail.log'), quiet=True)
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        settings = {
            'spark_version': sc.version, 'master': sc.master,
            'parallelism': sc.defaultParallelism,
            'max_heap_bytes': sc._jvm.java.lang.Runtime.getRuntime().maxMemory(),
            'spark_conf': dict(sc.getConf().getAll()),
        }
    initialization_seconds = time.perf_counter() - initialized_at
    cores = args.cores
    size = lambda n: max(cores, int(n * args.scale))
    n = size(1_000_000)
    big = size(10_000_000)
    range_n = size(100_000_000)
    matrix_n = size(args.matrix_rows)
    matrix_cols = args.matrix_columns
    cols = 10
    rt = hl.utils.range_table
    rm = hl.utils.range_matrix_table

    def range_force():
        assert rt(range_n, cores)._force_count() == range_n

    def rekey():
        table = rt(n, cores)
        assert table.key_by(x=n - table.idx)._force_count() == n

    def group(sorted_keys):
        table = rt(big, cores)
        key = table.idx // 1000 if sorted_keys else (big - table.idx) // 1000
        grouped = table.group_by(x=key).aggregate(y=hl.agg.count())
        expected = (big + 999) // 1000 if sorted_keys else big // 1000 + 1
        assert grouped._force_count() == expected

    def annotate():
        table = rt(n, cores)
        assert table.annotate(**{f'x{i}': i + table.idx for i in range(100)})._force_count() == n

    def stats():
        table = rt(big, cores)
        result = table.aggregate(hl.agg.stats(hl.float64(table.idx)))
        assert result.n == big
        assert result.min == 0 and result.max == big - 1
        assert abs(result.mean - (big - 1) / 2) < 1e-6

    def join():
        left = rt(n, cores)
        right = rt(n, cores).annotate(payload=7)
        assert left.join(right, 'inner')._force_count() == n

    def lookup():
        table = rt(n, cores)
        target = rt(1000, cores).annotate(payload=7)
        assert table.annotate(x=target[(n - 1 - table.idx) % 1000])._force_count() == n

    def scan():
        table = rt(n, cores)
        assert table.annotate(prefix=hl.scan.sum(table.idx))._force_count() == n

    def matrix_force():
        assert rm(matrix_n, matrix_cols, cores)._force_count_rows() == matrix_n

    def matrix_entries():
        matrix = rm(matrix_n, matrix_cols, cores)
        matrix = matrix.annotate_entries(value=matrix.row_idx + matrix.col_idx)
        expected = matrix_cols * matrix_n * (matrix_n - 1) // 2 + matrix_n * matrix_cols * (matrix_cols - 1) // 2
        assert matrix.aggregate_entries(hl.agg.sum(hl.int64(matrix.value))) == expected

    def matrix_filtered():
        matrix = rm(matrix_n, matrix_cols, cores)
        matrix = matrix.filter_rows(matrix.row_idx % 2 == 0)
        matrix = matrix.filter_cols(matrix.col_idx % 2 == 0)
        assert matrix._force_count_rows() == (matrix_n + 1) // 2
        assert matrix.count_cols() == (matrix_cols + 1) // 2

    vcf_rows = size(50_000)
    vcf = args.workdir / 'synthetic.vcf'
    if args.case is None or 'vcf_import_entry_sum' in args.case:
        with vcf.open('w') as output:
            output.write('##fileformat=VCFv4.2\n##contig=<ID=1,length=249250621>\n')
            output.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n')
            output.write('##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">\n')
            output.write('#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t' + '\t'.join(f's{i}' for i in range(cols)) + '\n')
            entries = '\t'.join(['0/1:20'] * cols)
            for position in range(1, vcf_rows + 1):
                output.write(f'1\t{position}\t.\tA\tC\t50\tPASS\t.\tGT:DP\t{entries}\n')

    def vcf_sum():
        matrix = hl.import_vcf(str(vcf), reference_genome='GRCh37')
        assert matrix.aggregate_entries(hl.agg.sum(matrix.DP)) == vcf_rows * cols * 20

    snapshot = args.workdir / ('table.ht' if args.engine == 'hail' else 'table.viceberg')
    def write():
        table = rt(n, cores)
        table.annotate(x=table.idx * 7).write(str(snapshot), overwrite=True)
    def read():
        assert hl.read_table(str(snapshot))._force_count() == n

    cases = {
        'table_range_force_count': (range_force, {'rows': range_n}),
        'table_key_by_shuffle': (rekey, {'rows': n}),
        'table_group_sorted': (lambda: group(True), {'rows': big, 'group_width': 1000}),
        'table_group_unsorted': (lambda: group(False), {'rows': big, 'group_width': 1000}),
        'table_annotate_100_fields': (annotate, {'rows': n, 'fields': 100}),
        'table_stats': (stats, {'rows': big}),
        'table_inner_join': (join, {'left_rows': n, 'right_rows': n}),
        'table_foreign_key_lookup': (lookup, {'rows': n, 'target_rows': 1000}),
        'table_scan_sum': (scan, {'rows': n}),
        'table_write': (write, {'rows': n, 'int32_columns': 2}),
        'table_read': (read, {'rows': n, 'int32_columns': 2}),
        'matrix_force_count_rows': (matrix_force, {'rows': matrix_n, 'columns': matrix_cols}),
        'matrix_entry_sum': (matrix_entries, {'rows': matrix_n, 'columns': matrix_cols}),
        'matrix_filter_rows_columns': (matrix_filtered, {'rows': matrix_n, 'columns': matrix_cols}),
        'vcf_import_entry_sum': (vcf_sum, {'rows': vcf_rows, 'samples': cols}),
    }
    selected = args.case or list(cases)
    if 'table_read' in selected and 'table_write' not in selected:
        write()
    report = {
        'engine': args.engine, 'package_version': importlib.metadata.version(args.engine),
        'runtime_version': getattr(hl, '__version__', None),
        'python': platform.python_version(), 'architecture': platform.machine(),
        'pid': os.getpid(), 'cores': cores, 'scale': args.scale,
        'warmups': args.warmups, 'iterations': args.iterations,
        'import_seconds': import_seconds, 'initialization_seconds': initialization_seconds,
        'settings': settings, 'cases': [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def save():
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    save()
    for name in selected:
        function, parameters = cases[name]
        record = {'name': name, 'parameters': parameters, 'configured_cores': cores,
                  'source_partitions_requested': None if name == 'vcf_import_entry_sum' else cores,
                  'status': 'running', 'seconds': []}
        report['cases'].append(record)
        save()
        print(f'{args.engine}: {name} {parameters}', flush=True)
        try:
            for _ in range(args.warmups):
                function()
            for _ in range(args.iterations):
                gc.collect()
                record.setdefault('load_average_before', []).append(list(os.getloadavg()))
                started = time.perf_counter()
                function()
                elapsed = time.perf_counter() - started
                record['seconds'].append(elapsed)
                print(f'  {elapsed:.6f}s', flush=True)
                save()
            record['median_seconds'] = statistics.median(record['seconds'])
            record['status'] = 'passed'
        except Exception:
            record['status'] = 'failed'
            record['error'] = traceback.format_exc()
            print(record['error'], flush=True)
        save()
    if args.engine == 'hail':
        hl.stop()


if __name__ == '__main__':
    main()
