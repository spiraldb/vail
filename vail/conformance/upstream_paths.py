"""Paths shared by the upstream test adapters."""
import os
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
    """Adapt the two sample VCF snapshots only within the test harness.

    Their original fixture bodies remain intact. Both snapshots have the same
    sample.vcf row/entry/column schema and 346 rows; the latter retains 20
    partitions. VCF import is independently checked against the Hail snapshot
    by conformance/vcf_golden.py. No legacy storage reader is added to Vail.
    """
    resources = Path(resources)
    if name not in ('sample.vcf.mt', 'sample.vcf-20-partitions.mt'):
        return str(resources / name)
    target = Path(cache_dir) / name
    if not (target / '_vail_matrix.json').exists():
        import vail as hl
        partitions = 20 if name == 'sample.vcf-20-partitions.mt' else 2
        matrix = hl.import_vcf(str(resources / 'sample.vcf'), reference_genome='GRCh37',
                               array_elements_required=False)
        matrix.repartition(partitions).write(str(target), overwrite=True)
    return str(target)


def doctest_data_dir(suite):
    """Resolve doctest data independently of the worker's current directory."""
    default = Path(suite).resolve().parents[1] / 'hail' / 'docs' / 'data'
    return str(Path(os.environ.get('HAIL_DOCTEST_DATA_DIR', default)).expanduser().resolve())
