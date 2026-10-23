"""Read Hail-written VDS components through the native legacy storage scan."""
import os
import runpy
from pathlib import Path

import pytest
import vail as hl

RESOURCES = Path(os.environ.get(
    'VAIL_HAIL_RESOURCES',
    Path(__file__).resolve().parents[3] / 'hail/hail/hail/test/resources',
))
SOURCE = RESOURCES / 'vds/1kg_chr22_5_samples.vds'


def test_hail_vds_read_and_native_roundtrip(tmp_path):
    vds = hl.vds.read_vds(str(SOURCE))
    assert vds.n_samples() == 5
    assert vds.variant_data.count_rows() == 81
    assert hl.eval(vds.reference_data.index_globals().ref_block_max_length) == 147492
    reference = vds.reference_data
    assert reference.aggregate_entries(hl.agg.all(
        reference.END == reference.locus.position + reference.LEN - 1
    ))
    # Collect whole sparse records, including filtered entries, rather than
    # testing only metadata counts or Python routing internals.
    reference_rows = reference.localize_entries('entries', 'columns').collect()
    variant_rows = vds.variant_data.localize_entries('entries', 'columns').collect()
    assert any(entry is None for row in reference_rows for entry in row.entries)
    oracle = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'conformance/vds_fixture.py'))
    expected = oracle['vds'](SOURCE.name)
    assert reference_rows == expected.reference_data.localize_entries('entries', 'columns').collect()
    assert variant_rows == expected.variant_data.localize_entries('entries', 'columns').collect()
    target = str(tmp_path / 'copy.vds')
    vds.write(target)
    reread = hl.vds.read_vds(target)
    assert reread.reference_data.localize_entries('entries', 'columns').collect() == reference_rows
    assert reread.variant_data.localize_entries('entries', 'columns').collect() == variant_rows
    assert reread.reference_data.cols().collect() == reference.cols().collect()


def test_hail_vds_drop_end():
    vds = hl.vds.read_vds(str(SOURCE), _drop_end=True)
    assert 'END' not in vds.reference_data.entry
    assert 'LEN' in vds.reference_data.entry
    assert vds.reference_data.aggregate_entries(hl.agg.count()) > 0


@pytest.mark.parametrize('options', [{'intervals': []}, {'n_partitions': 2}])
def test_vds_read_unsupported_planning_is_explicit(options):
    with pytest.raises(NotImplementedError):
        hl.vds.read_vds(str(SOURCE), **options)
