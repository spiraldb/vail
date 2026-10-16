"""Capped public VCF actions; generated input and runtime warmup excluded."""
import argparse
import importlib.util
import itertools
import json
import resource
import subprocess
import sys
import tempfile
import time


def worker(mode, rows, samples, width, layouts, depth, baseline):
    import vail as hl
    hl.eval(1)
    with tempfile.TemporaryDirectory() as directory:
        path = directory + '/data.vcf'
        if mode == 'export':
            export = hl.export_vcf
            if baseline:
                spec = importlib.util.spec_from_file_location('vail._vcf_baseline', baseline)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                export = module.export_vcf
            mt = hl.range_matrix_table(rows, samples)
            mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 1), alleles=['A', 'C'])
            mt = mt.key_rows_by('locus', 'alleles')
            mt = mt.annotate_cols(s=hl.str(mt.col_idx)).key_cols_by('s')
            mt = mt.annotate_entries(GT=hl.call(0, 1), **{f'X{i}': 7 for i in range(width - 1)})
            start = time.perf_counter()
            export(mt, path)
            action = time.perf_counter() - start
            with open(path) as stream:
                body = (line for line in stream if not line.startswith('#'))
                count = 0
                for line in body:
                    fields = line.rstrip('\n').split('\t')
                    assert len(fields) == 9 + samples
                    assert all(sample == '0/1' + ':7' * (width - 1) for sample in fields[9:])
                    count += 1
                assert count == rows
            construction = 0
        else:
            names = ['GT', 'DP'] + [f'X{i}' for i in range(width - 2)]
            permutations = list(itertools.islice(itertools.permutations(names), layouts))
            with open(path, 'w') as stream:
                stream.write('##fileformat=VCFv4.2\n')
                for name in names:
                    kind = 'String' if name == 'GT' else 'Integer'
                    stream.write(f'##FORMAT=<ID={name},Number=1,Type={kind},Description="">\n')
                stream.write('#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t' + '\t'.join(f's{i}' for i in range(samples)) + '\n')
                for i in range(rows):
                    fields = permutations[i % len(permutations)]
                    sample = ':'.join('0/1' if name == 'GT' else '7' for name in fields)
                    stream.write(f'1\t{i + 1}\t.\tA\tC\t.\tPASS\t.\t' + ':'.join(fields) + '\t' + '\t'.join([sample] * samples) + '\n')
            start = time.perf_counter()
            mt = hl.import_vcf(path)
            if mode == 'depth':
                table = mt.rows()
                for _ in range(depth):
                    table = table.annotate(value=table.locus.position)
                value = hl.agg.sum(table.value)
            elif mode == 'rows':
                table = mt.rows()
                value = hl.agg.sum(table.locus.position)
            else:
                value = hl.agg.sum(mt.DP)
                if mode == 'all':
                    value += hl.agg.sum(mt.GT.n_alt_alleles())
                    for name in names[2:]:
                        value += hl.agg.sum(mt[name])
            construction = time.perf_counter() - start
            start = time.perf_counter()
            result = table.aggregate(value) if mode in ['rows', 'depth'] else mt.aggregate_entries(value)
            action = time.perf_counter() - start
            expected = rows * (rows + 1) // 2 if mode in ['rows', 'depth'] else rows * samples * (7 if mode == 'DP' else 7 * (width - 1) + 1)
            assert result == expected
        print(json.dumps(dict(mode=mode, rows=rows, samples=samples, width=width, layouts=layouts, depth=depth,
                             construction_seconds=construction, action_seconds=action,
                             max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--mode', choices=['export', 'rows', 'DP', 'all', 'depth'])
    for name, default in [('rows', 512), ('samples', 8), ('width', 2), ('layouts', 1), ('depth', 1)]:
        p.add_argument('--' + name, type=int, default=default)
    p.add_argument('--baseline')
    args = p.parse_args()
    if args.mode:
        worker(args.mode, args.rows, args.samples, args.width, args.layouts, args.depth, args.baseline)
    else:
        cases = [('export', r, s, w, 1, 1) for r, s, w in [(512, 8, 2), (4096, 8, 2), (512, 64, 16), (4096, 64, 16)]]
        if not args.baseline:
            cases += [(mode, r, s, w, layouts, 1) for mode in ['rows', 'DP', 'all'] for r, s, w, layouts in [(512, 8, 2, 1), (4096, 128, 2, 1), (1024, 64, 8, 256), (4096, 64, 16, 1)]]
            cases += [('depth', 512, 8, 2, 1, d) for d in [1, 16, 64, 256]]
        for mode, rows, samples, width, layouts, depth in cases:
            command = [sys.executable, __file__, '--mode', mode, '--rows', str(rows), '--samples', str(samples), '--width', str(width), '--layouts', str(layouts), '--depth', str(depth)]
            if args.baseline:
                command += ['--baseline', args.baseline]
            try:
                result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=20)
                print(result.stdout.strip(), flush=True)
            except subprocess.TimeoutExpired:
                print(json.dumps(dict(mode=mode, rows=rows, samples=samples, width=width, layouts=layouts, depth=depth, timeout_seconds=20)), flush=True)
