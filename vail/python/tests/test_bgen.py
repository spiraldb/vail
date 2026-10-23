"""Small spec-generated layout-2 fixtures exercise native BGEN decoding."""
import struct
import zlib
import pytest
import vail as hl


def _first_variant_offset(path):
    # BGEN specification: uint32 offset counts bytes after this first field.
    with path.open('rb') as source:
        return int.from_bytes(source.read(4), 'little') + 4


def _assert_matrix_values_equal(left, right):
    assert left.row.dtype == right.row.dtype
    assert left.col.dtype == right.col.dtype
    assert left.entry.dtype == right.entry.dtype
    assert left.row_key.dtype == right.row_key.dtype
    assert left.col_key.dtype == right.col_key.dtype
    assert left.globals.dtype == right.globals.dtype
    assert hl.eval(left.index_globals()) == hl.eval(right.index_globals())
    assert left.cols().collect() == right.cols().collect()
    left_rows = left.localize_entries('entries', 'cols')
    right_rows = right.localize_entries('entries', 'cols')
    assert hl.eval(left_rows.index_globals()) == hl.eval(right_rows.index_globals())
    assert left_rows.collect() == right_rows.collect()


def _text(value, size=2):
    data = value.encode()
    return len(data).to_bytes(size, 'little') + data


def _bgen(tmp_path, compressed=True, probabilities=None, allele="A"):
    if probabilities is None:
        probabilities = [(255, 0, False), (0, 255, False), (0, 0, True)]
    n = len(probabilities)
    sample_block = struct.pack('<II', 8 + sum(2+len(f's{i}') for i in range(n)), n)
    sample_block += b''.join(_text(f's{i}') for i in range(n))
    flags = (1 if compressed else 0) | (2 << 2) | (1 << 31)
    header = struct.pack('<IIII', 20+len(sample_block), 20, 1, n) + b'bgen' + struct.pack('<I', flags)
    variant = _text('variant') + _text('rs1') + _text('1') + struct.pack('<IH', 123, 2) + _text(allele,4) + _text('G',4)
    data = struct.pack('<IHBB', n, 2, 2, 2) + bytes(130 if p[2] else 2 for p in probabilities) + bytes([0,8])
    data += bytes(v for a,b,_ in probabilities for v in (a,b))
    block = struct.pack('<I',len(data)) + zlib.compress(data) if compressed else data
    filename = tmp_path / 'test.bgen'
    filename.write_bytes(header + sample_block + variant + struct.pack('<I',len(block)) + block)
    hl.index_bgen(str(filename))
    return filename


@pytest.mark.parametrize('compressed', [False,True])
def test_native_bgen_spec_genotypes(tmp_path, compressed):
    path = _bgen(tmp_path, compressed)
    mt = hl.import_bgen(str(path), ['GT','GP','dosage'])
    assert mt.count() == (1,3)
    assert set(mt._mir.core._schema['__vail_matrix_cells'].element_type) == {'value'}
    slots = mt._mir.core['__vail_matrix_cells'].collect()[0]
    assert len(slots) == 3 and all(set(slot) == {'value'} for slot in slots)
    assert mt.s.collect() == ['s0','s1','s2']
    mt = mt.unfilter_entries()
    assert mt.GT.collect() == [hl.Call((0,0),False), hl.Call((0,1),False), None]
    assert mt.GP.collect() == [[1.,0.,0.], [0.,1.,0.], None]
    assert mt.dosage.collect() == [0.,1.,None]


def test_native_bgen_missing_index_and_no_entry_decode(tmp_path):
    path = _bgen(tmp_path)
    mt = hl.import_bgen(str(path), [])
    assert mt.count() == (1,3)
    assert mt.entry.dtype == hl.tstruct()
    assert mt.locus.collect() == [hl.Locus('1',123)]


def test_native_bgen_does_not_revalidate_probability_sums(tmp_path):
    path = _bgen(tmp_path, probabilities=[(255,255,False)])
    mt = hl.import_bgen(str(path), ['GP', 'GT', 'dosage'])
    assert mt.GP.collect() == [[1., 1., -1.]]
    assert mt.dosage.collect() == [-1.]
    assert mt.GT.collect() == [None]


def _corrupt_probability_sample_count(path):
    # Keep the compressed frame structurally sound, but make its sample count
    # disagree with the enclosing header. Decoding must reject the unsafe shape.
    data = bytearray(path.read_bytes())
    offset = _first_variant_offset(path)
    for _ in range(3):
        size = int.from_bytes(data[offset:offset + 2], 'little')
        offset += 2 + size
    offset += 6  # position and allele count
    for _ in range(2):
        size = int.from_bytes(data[offset:offset + 4], 'little')
        offset += 4 + size
    size = int.from_bytes(data[offset:offset + 4], 'little')
    block = bytes(data[offset + 4:offset + 4 + size])
    payload = bytearray(zlib.decompress(block[4:]))
    count = int.from_bytes(payload[:4], 'little')
    payload[:4] = struct.pack('<I', count + 1)
    replacement = block[:4] + zlib.compress(payload)
    data[offset:offset + 4 + size] = struct.pack('<I', len(replacement)) + replacement
    path.write_bytes(data)


def test_missing_bit_filters_empty_entries_like_hail(tmp_path):
    path = _bgen(tmp_path)
    mt = hl.import_bgen(str(path), [])
    assert mt.aggregate_entries(hl.agg.count()) == 2
    assert mt.entries().count() == 2


@pytest.mark.parametrize('codec', ['zlib', 'zstd'])
@pytest.mark.parametrize('parallel', [None, 'header_per_shard'])
def test_bgen_native_writer_roundtrip(tmp_path, codec, parallel):
    path = _bgen(tmp_path)
    original = hl.import_bgen(str(path), ['GT','GP'])
    output = str(tmp_path / 'roundtrip')
    hl.export_bgen(original, output, parallel=parallel, compression_codec=codec)
    hl.index_bgen(output + '.bgen')
    imported = hl.import_bgen(output + '.bgen', ['GT','GP'], sample_file=output + '.sample')
    _assert_matrix_values_equal(original, imported)


def test_indexed_literal_filter_never_decodes_unselected_bad_block(tmp_path):
    path = _bgen(tmp_path)
    valid = path.read_bytes()
    offset = _first_variant_offset(path)
    path = _bgen(tmp_path)
    _corrupt_probability_sample_count(path)
    bad = path.read_bytes()[offset:]
    second = bytearray(valid[offset:])
    # Identifying strings: 2+7-byte varid, 2+3-byte rsid, 2+1-byte contig.
    second[17:21] = struct.pack('<I',124)
    header = bytearray(valid[:offset])
    header[8:12] = struct.pack('<I',2)
    path.write_bytes(header + bad + second)
    hl.index_bgen(str(path))
    selected = hl.import_bgen(str(path), ['GP'], variants=[hl.Locus('1',124)])
    assert selected.locus.collect() == [hl.Locus('1',124)]
    assert selected.GP.collect() == [[1.,0.,0.], [0.,1.,0.]]
    with pytest.raises(Exception, match='sample/allele count mismatch'):
        hl.import_bgen(str(path), ['GP']).GP.collect()


def test_anonymous_samples_use_lazy_column_metadata(tmp_path):
    path = _bgen(tmp_path)
    data = path.read_bytes()
    first_variant = _first_variant_offset(path)
    header = bytearray(data[:24])
    header[:4] = struct.pack('<I',20)
    flags = int.from_bytes(header[20:24],'little') & ~(1<<31)
    header[20:24] = struct.pack('<I',flags)
    path.write_bytes(header + data[first_variant:])
    hl.index_bgen(str(path))
    mt = hl.import_bgen(str(path), [])
    assert mt.s.collect() == ['_0','_1','_2']
    assert mt.count_rows() == 1


def test_separate_header_shards_concatenate_to_valid_bgen(tmp_path):
    path = _bgen(tmp_path)
    original = hl.import_bgen(str(path), ['GP'])
    root = str(tmp_path / 'separate')
    hl.export_bgen(original, root, parallel='separate_header')
    folder = tmp_path / 'separate.bgen'
    merged = tmp_path / 'merged.bgen'
    manifest = (folder/'shard-manifest.txt').read_text().splitlines()
    merged.write_bytes(b''.join((folder/name).read_bytes() for name in manifest))
    hl.index_bgen(str(merged))
    imported = hl.import_bgen(str(merged), ['GP'], sample_file=root+'.sample')
    _assert_matrix_values_equal(original, imported)


def test_dynamic_variant_filter_never_decodes_unselected_bad_block(tmp_path):
    path = _bgen(tmp_path)
    valid = path.read_bytes()
    offset = _first_variant_offset(path)
    path = _bgen(tmp_path)
    _corrupt_probability_sample_count(path)
    bad = path.read_bytes()[offset:]
    second = bytearray(valid[offset:]); second[17:21] = struct.pack('<I',124)
    header = bytearray(valid[:offset]); header[8:12] = struct.pack('<I',2)
    path.write_bytes(header + bad + second)
    hl.index_bgen(str(path))
    selector = hl.Table.parallelize([{'locus':hl.Locus('1',124)}], key='locus')
    selected = hl.import_bgen(str(path), ['GP'], variants=selector)
    assert selected.GP.collect() == [[1.,0.,0.], [0.,1.,0.]]


def test_dynamic_selector_dependency_survives_globals_and_rechecks_source(tmp_path):
    path = _bgen(tmp_path)
    data = path.read_bytes()
    offset = _first_variant_offset(path)
    second = bytearray(data[offset:]); second[17:21] = struct.pack('<I',124)
    header = bytearray(data[:offset]); header[8:12] = struct.pack('<I',2)
    path.write_bytes(header + data[offset:] + second)
    hl.index_bgen(str(path))
    selector_path = tmp_path/'variants.tsv'
    selector_path.write_text('contig\tposition\n1\t123\n')
    selector = hl.import_table(str(selector_path), types={'position':hl.tint32})
    selector = selector.key_by(locus=hl.locus(selector.contig,selector.position)).select()
    selected = hl.import_bgen(str(path), ['GP'], variants=selector, n_partitions=2).select_globals()
    assert selected.locus.collect() == [hl.Locus('1',123)]
    assert selected.aggregate_entries(hl.agg.count()) == 2
    selector_path.write_text('contig\tposition\n1\t124\n')
    assert selected.locus.collect() == [hl.Locus('1',124)]
    assert selected.GP.collect() == [[1.,0.,0.], [0.,1.,0.]]


def test_wide_allele_length_is_limited_by_file_bytes_not_arbitrary_cap(tmp_path):
    size = 16*1024*1024 + 1
    path = _bgen(tmp_path, allele='A'*size)
    mt = hl.import_bgen(str(path), [])
    assert mt.alleles.map(lambda a:hl.len(a)).collect() == [[size,1]]


def test_truncated_wide_allele_length_fails_before_allocation(tmp_path):
    path = _bgen(tmp_path)
    data = bytearray(path.read_bytes())
    offset = _first_variant_offset(path)
    # 17 identifying-string bytes, then uint32 position and uint16 allele count.
    data[offset+23:offset+27] = struct.pack('<I',2**32-1)
    path.write_bytes(data)
    with pytest.raises(ValueError, match='truncated BGEN identifier'):
        hl.index_bgen(str(path))
    # Existing index still supplies a valid variant offset. The native decoder
    # independently enforces the same physical-byte bound before allocating.
    with pytest.raises(Exception, match='truncated BGEN identifier'):
        hl.import_bgen(str(path), []).locus.collect()


def test_unsigned_position_outside_locus_range_is_rejected(tmp_path):
    path = _bgen(tmp_path)
    data = bytearray(path.read_bytes())
    offset = _first_variant_offset(path)
    data[offset+17:offset+21] = struct.pack('<I', 2**31)
    path.write_bytes(data)
    with pytest.raises(Exception, match='signed 32-bit locus range'):
        hl.index_bgen(str(path), reference_genome=None)
    # The existing index must not permit a native narrowing cast either.
    with pytest.raises(Exception, match='signed 32-bit locus range'):
        hl.import_bgen(str(path), []).locus.collect()


@pytest.mark.parametrize('codec', ['zlib', 'zstd'])
@pytest.mark.parametrize('n_samples', [5, 64, 65])
def test_bgen_multiple_blocks_preserve_probabilities_calls_and_missingness(tmp_path, codec, n_samples):
    mt = hl.range_matrix_table(17, n_samples, n_partitions=2)
    mt = mt.annotate_cols(s=hl.str(mt.col_idx)).key_cols_by('s')
    mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 1), alleles=['A', 'G'])
    mt = mt.key_rows_by('locus', 'alleles')
    options = hl.literal([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]])
    mt = mt.annotate_entries(GP=hl.or_missing((mt.row_idx % 5 != 0) & ((mt.row_idx + mt.col_idx) % 4 != 0),
                                             options[(mt.row_idx + mt.col_idx) % 3]))
    output = str(tmp_path / 'multi')
    hl.export_bgen(mt, output, compression_codec=codec)
    hl.index_bgen(output + '.bgen')
    imported = hl.import_bgen(output + '.bgen', ['GP', 'GT', 'dosage'],
                              sample_file=output + '.sample', n_partitions=3)
    indices = [(row + col) % 3 for row in range(17) for col in range(n_samples)
               if row % 5 != 0 and (row + col) % 4 != 0]
    assert imported.count() == (17, n_samples)
    assert imported.GP.collect() == [[float(i == j) for j in range(3)] for i in indices]
    assert imported.GT.collect() == [hl.Call((0, 0) if i == 0 else (0, 1) if i == 1 else (1, 1), False)
                                     for i in indices]
    assert imported.dosage.collect() == [float(i) for i in indices]
    assert imported.aggregate_entries(hl.agg.count()) == len(indices)
    empty = hl.import_bgen(output + '.bgen', [], sample_file=output + '.sample', n_partitions=3)
    assert empty.aggregate_entries(hl.agg.count()) == len(indices)
    assert empty.entries().count() == len(indices)


def test_native_bgen_zero_sample_probability_block(tmp_path):
    path = _bgen(tmp_path, probabilities=[])
    mt = hl.import_bgen(str(path), ['GP', 'GT', 'dosage'])
    assert mt.count() == (1, 0)
    assert mt.aggregate_entries(hl.agg.count()) == 0
    assert mt.entries().count() == 0
    assert mt.localize_entries('entries', 'cols').collect()[0]['entries'] == []
