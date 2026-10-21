"""Public GEN import contract, from Oxford's GEN/SAMPLE specification."""
import pytest
import vail as hl
from vail.gen import import_gen


def write_input(tmp_path, rows, samples=('s1', 's2')):
    gen = tmp_path / 'input.gen'
    gen.write_text(rows)
    sample = tmp_path / 'input.sample'
    sample.write_text('ID_1 ID_2 missing\n0 0 0\n' + ''.join(f'{s} {s} 0\n' for s in samples))
    return str(gen), str(sample)


def test_normalization_ties_missing_and_sample_order(tmp_path):
    gen, sample = write_input(tmp_path,
        '01 id1 rs1 7 A G .98 0 0 .4 .4 .2\n'
        '01 id2 rs2 8 C T 0 0 0 .1 .2 .7\n')
    mt = import_gen(gen, sample, contig_recoding={'01': '1'}, reference_genome='GRCh37')
    assert mt.count() == (2, 2)
    assert [c.s for c in mt.cols().collect()] == ['s1', 's2']
    rows = mt.entries().collect()
    assert len(rows) == 3
    assert rows[0].GP == [1.0, 0.0, 0.0]
    assert rows[0].GT == hl.Call([0, 0])
    assert rows[1].GT is None
    assert rows[2].GP == pytest.approx([.1, .2, .7])
    assert rows[2].GT == hl.Call([1, 1])


def test_chromosome_override_and_invalid_loci(tmp_path):
    gen, sample = write_input(tmp_path, 'id rs 7 A G 1 0 0 0 1 0\n')
    mt = import_gen(gen, sample, chromosome='X', reference_genome=None)
    assert mt.rows().collect()[0].locus == hl.Struct(contig='X', position=7)
    gen, sample = write_input(tmp_path,
                             '1 id rs 7 A G 1 0 0 0 1 0\n'
                             'bad id rs 7 A G 1 0 0 0 1 0\n')
    assert import_gen(gen, sample, skip_invalid_loci=True).count_rows() == 1
    with pytest.raises(Exception, match='Invalid locus'):
        import_gen(gen, sample).rows().collect()


def test_cardinality_and_lazy_file_read(tmp_path):
    gen, sample = write_input(tmp_path, '1 id rs 7 A G 1 0 0 0 1 0\n')
    mt = import_gen(gen, sample)
    # Mutation after construction must be visible at execution time.
    from pathlib import Path
    Path(gen).write_text('1 changed rs 8 A G 1 0 0 0 1 0\n')
    assert mt.rows().collect()[0].varid == 'changed'
    Path(gen).write_text('1 id rs 7 A G 1 0 0\n')
    with pytest.raises(Exception, match='incorrect number'):
        import_gen(gen, sample).entries().collect()


def test_export_roundtrip_and_overrides(tmp_path):
    from vail.gen import export_gen
    gen, sample = write_input(tmp_path,
        '1 id rs 7 A G .98 0 0 .4 .4 .2\n')
    mt = import_gen(gen, sample).choose_cols([1, 0])
    output = str(tmp_path / 'roundtrip')
    export_gen(mt, output, precision=5)
    restored = import_gen(output + '.gen', output + '.sample')
    assert restored.row.dtype == mt.row.dtype
    assert restored.entry.dtype == mt.entry.dtype
    assert list(restored.row_key) == list(mt.row_key)
    assert list(restored.col_key) == list(mt.col_key)
    assert restored.rows().collect() == mt.rows().collect()
    assert restored.cols().collect() == mt.cols().collect()
    assert restored.localize_entries('entries', 'cols').collect() == mt.localize_entries('entries', 'cols').collect()
    assert (tmp_path / 'roundtrip.gen').read_text().split()[6:] == ['0.40000', '0.40000', '0.20000', '1.00000', '0.00000', '0.00000']
    export_gen(mt, output, gp=[0.0, 1.0, 0.0], id1=mt.s + '_id',
               id2='second', missing=0.5, varid='variant', rsid='rs')
    rows = import_gen(output + '.gen', output + '.sample').entries().collect()
    assert all(row.GP == [0.0, 1.0, 0.0] and row.varid == 'variant' for row in rows)
    assert {row.s for row in rows} == {'s1_id', 's2_id'}
    with pytest.raises(ValueError, match='requires a GP'):
        export_gen(mt.select_entries(), output)


def test_export_rejects_invalid_gen_fields(tmp_path):
    from vail.gen import export_gen
    gen, sample = write_input(tmp_path, '1 id rs 7 A G 1 0 0 0 1 0\n')
    mt = import_gen(gen, sample)
    output = str(tmp_path / 'invalid')
    with pytest.raises(ValueError, match='identifiers'):
        export_gen(mt, output, id1='has space')
    with pytest.raises(ValueError, match='missing rate'):
        export_gen(mt, output, missing=1.1)
    with pytest.raises(Exception, match='length 3'):
        export_gen(mt, output, gp=[1.0, 0.0])
    with pytest.raises(Exception, match='whitespace'):
        export_gen(mt, output, varid='bad variant')
