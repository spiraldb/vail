"""Paths shared by the upstream test adapters."""
import os
import gzip
import json
from pathlib import Path


class FixtureModule:
    """Conformance-only module view resolving original sample snapshot paths."""
    def __init__(self, module, resources, cache_dir):
        self._module, self._resources, self._cache_dir = module, Path(resources).resolve(), cache_dir
        self._read_matrix_table = module.read_matrix_table

    def __getattr__(self, name):
        return getattr(self._module, name)

    def __dir__(self):
        return dir(self._module)

    def read_matrix_table(self, path, *args, **kwargs):
        path = Path(path).resolve()
        if path.parent == self._resources:
            path = resource_path(self._resources, path.name, self._cache_dir)
        return self._read_matrix_table(str(path), *args, **kwargs)


def resource_path(resources, name, cache_dir):
    """Adapt the sample snapshots only within the test harness.

    Their original fixture bodies remain intact. Both snapshots have the same
    sample.vcf row/entry/column schema and 346 rows; the latter retains 20
    partitions. VCF import is independently checked against the Hail snapshot
    by conformance/vcf_golden.py. No legacy storage reader is added to Vail.
    """
    resources = Path(resources)
    if name == 'split-multi-sample.vcf.mt':
        target = Path(cache_dir) / name
        if not (target / '_vail_matrix.json').exists():
            from plink_fixture import materialize
            materialize(target)
        return str(target)
    if name not in ('sample.vcf.mt', 'sample.vcf-20-partitions.mt'):
        return str(resources / name)
    target = Path(cache_dir) / name
    if not (target / '_vail_matrix.json').exists():
        import vail as hl
        matrix = hl.import_vcf(str(resources / 'sample.vcf'), reference_genome='GRCh37',
                               array_elements_required=False)
        # Preserve the original fixture's contiguous partition boundaries.
        # Native repartition currently distributes whole Arrow batches, which
        # would leave this small VCF entirely in the first partition.
        metadata = resources / name / 'metadata.json.gz'
        if metadata.exists():
            with gzip.open(metadata, 'rt') as file:
                counts = json.load(file)['components']['partition_counts']['counts']
            core = matrix._mir.core
            rows = core.collect()
            if sum(counts) != len(rows):
                raise ValueError('sample VCF fixture row count differs from original snapshot')
            globals_value = hl.eval(core.index_globals())
            globals_exprs = {field: hl.literal(globals_value[field], dtype)
                             for field, dtype in core._global_schema.items()}
            parts, start = [], 0
            for count in counts:
                parts.append(hl.Table.parallelize(rows[start:start + count], schema=core._schema,
                                                  n_partitions=1))
                start += count
            # Keep metadata outside the union and bound the plan depth. A
            # left-associated union duplicates global lowering at every level.
            while len(parts) > 1:
                parts = [parts[i].union(parts[i + 1]) if i + 1 < len(parts) else parts[i]
                         for i in range(0, len(parts), 2)]
            matrix = matrix._copy(rows=parts[0].annotate_globals(**globals_exprs))
        else:
            partitions = 20 if name == 'sample.vcf-20-partitions.mt' else 2
            matrix = matrix.repartition(partitions)
        matrix.write(str(target), overwrite=True)
    return str(target)


def doctest_data_dir(suite):
    """Resolve doctest data independently of the worker's current directory."""
    default = Path(suite).resolve().parents[1] / 'hail' / 'docs' / 'data'
    return str(Path(os.environ.get('HAIL_DOCTEST_DATA_DIR', default)).expanduser().resolve())
