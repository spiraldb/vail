"""Reader-established key order survives public axis projections and maps."""
import struct
import zlib

import pytest
import vail as hl


def _text(value, size=2):
    data = value.encode()
    return len(data).to_bytes(size, 'little') + data


def _unordered_bgen(path):
    # Physical encounter order is deliberately unrelated to genomic key order;
    # duplicate keys must retain their original relative order across partitions.
    records = [(20, 'A', 'z'), (10, 'C', 'c'), (10, 'A', 'first'),
               (10, 'A', 'second'), (30, 'A', 'end'), (15, 'A', 'mid')]
    sample = struct.pack('<II', 12, 1) + _text('s0')
    flags = 1 | (2 << 2) | (1 << 31)
    header = struct.pack('<IIII', 20 + len(sample), 20, len(records), 1)
    header += b'bgen' + struct.pack('<I', flags)
    data = struct.pack('<IHBB', 1, 2, 2, 2) + bytes([2, 0, 8, 0, 255])
    block = struct.pack('<I', len(data)) + zlib.compress(data)
    variants = b''
    for position, allele, name in records:
        variants += (_text(name) + _text(name) + _text('1')
                     + struct.pack('<IH', position, 2) + _text(allele, 4)
                     + _text('G', 4) + struct.pack('<I', len(block)) + block)
    path.write_bytes(header + sample + variants)
    hl.index_bgen(str(path))


@pytest.mark.parametrize('partitions', [1, 3, 8])
def test_bgen_index_order_survives_rows_and_annotations(tmp_path, partitions):
    path = tmp_path / 'unordered.bgen'
    _unordered_bgen(path)
    mt = hl.import_bgen(str(path), ['GP'], n_partitions=partitions)
    mt = mt.annotate_rows(total=hl.agg.sum(mt.GP[1]))
    expected = [(10, ['A', 'G'], 'first', 1.),
                (10, ['A', 'G'], 'second', 1.),
                (10, ['C', 'G'], 'c', 1.),
                (15, ['A', 'G'], 'mid', 1.),
                (20, ['A', 'G'], 'z', 1.),
                (30, ['A', 'G'], 'end', 1.)]
    assert [(r.locus.position, r.alleles, r.varid, r.total)
            for r in mt.rows().collect()] == expected
    filtered = mt.filter_rows(mt.locus.position <= 15).select_rows('varid', 'total')
    assert [(r.locus.position, r.alleles, r.varid, r.total)
            for r in filtered.rows().collect()] == expected[:4]
    assert filtered.rows().select('varid').varid.collect() == ['first', 'second', 'c', 'mid']
    assert filtered.rows()._force_count() == 4
    empty = mt.filter_rows(mt.locus.position < 0)
    assert empty.rows().collect() == []
    assert list(empty.rows().key.dtype) == ['locus', 'alleles']
    restored_path = tmp_path / 'persisted.mt'
    filtered.write(restored_path)
    assert hl.read_matrix_table(restored_path).rows().collect() == filtered.rows().collect()


@pytest.mark.parametrize('reader', ['vcf', 'plink'])
def test_sorted_imports_preserve_duplicate_order_through_rows(tmp_path, reader):
    records = [(30, 'last'), (10, 'first'), (10, 'second'), (20, 'middle')]
    if reader == 'vcf':
        path = tmp_path / 'input.vcf'
        path.write_text('##fileformat=VCFv4.2\n'
                        '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n'
                        + ''.join(f'1\t{p}\t{name}\tA\tG\t.\tPASS\t.\n'
                                  for p, name in records))
        mt = hl.import_vcf(str(path))
    else:
        prefix = tmp_path / 'input'
        prefix.with_suffix('.fam').write_text('f s 0 0 1 1\n')
        prefix.with_suffix('.bim').write_text(''.join(
            f'1 {name} 0 {p} G A\n' for p, name in records))
        prefix.with_suffix('.bed').write_bytes(bytes([0x6c, 0x1b, 1, 0, 0, 0, 0]))
        mt = hl.import_plink(*(str(prefix.with_suffix(s)) for s in ['.bed', '.bim', '.fam']))
    mt = mt.annotate_rows(label=mt.rsid)
    assert [(r.locus.position, r.label) for r in mt.rows().collect()] == [
        (10, 'first'), (10, 'second'), (20, 'middle'), (30, 'last')]
    rekeyed = mt.key_rows_by('label')
    assert [r.label for r in rekeyed.rows().collect()] == ['first', 'last', 'middle', 'second']
