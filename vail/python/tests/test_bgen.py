"""Small spec-generated layout-2 fixtures exercise native BGEN decoding."""
import struct
import zlib
import pytest
import vail as hl


def _text(value, size=2):
    data = value.encode()
    return len(data).to_bytes(size, 'little') + data


def _bgen(tmp_path, compressed=True, probabilities=None, allele="A"):
    probabilities = probabilities or [(255, 0, False), (0, 255, False), (0, 0, True)]
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


def test_native_bgen_invalid_probability_sum(tmp_path):
    path = _bgen(tmp_path, probabilities=[(255,255,False)])
    mt = hl.import_bgen(str(path), ['GP'])
    assert mt.count_rows() == 1
    with pytest.raises(Exception, match='probability sum exceeds'):
        mt.GP.collect()


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
    assert original._same(imported)


def test_indexed_literal_filter_never_decodes_unselected_bad_block(tmp_path):
    from vail.bgen import _header
    path = _bgen(tmp_path)
    valid = path.read_bytes()
    offset = _header(str(path))[0]
    path = _bgen(tmp_path, probabilities=[(255,255,False), (0,255,False), (0,0,True)])
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
    with pytest.raises(Exception, match='probability sum exceeds'):
        hl.import_bgen(str(path), ['GP']).GP.collect()


def test_anonymous_samples_use_lazy_column_metadata(tmp_path):
    from vail.bgen import _header
    path = _bgen(tmp_path)
    data = path.read_bytes()
    first_variant = _header(str(path))[0]
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
    assert original._same(imported)


def test_dynamic_variant_filter_never_decodes_unselected_bad_block(tmp_path):
    from vail.bgen import _header
    path = _bgen(tmp_path)
    valid = path.read_bytes()
    offset = _header(str(path))[0]
    path = _bgen(tmp_path, probabilities=[(255,255,False), (0,255,False), (0,0,True)])
    bad = path.read_bytes()[offset:]
    second = bytearray(valid[offset:]); second[17:21] = struct.pack('<I',124)
    header = bytearray(valid[:offset]); header[8:12] = struct.pack('<I',2)
    path.write_bytes(header + bad + second)
    hl.index_bgen(str(path))
    selector = hl.Table.parallelize([{'locus':hl.Locus('1',124)}], key='locus')
    selected = hl.import_bgen(str(path), ['GP'], variants=selector)
    assert selected.GP.collect() == [[1.,0.,0.], [0.,1.,0.]]


def test_dynamic_selector_dependency_survives_globals_and_rechecks_source(tmp_path):
    from vail.bgen import _header
    path = _bgen(tmp_path)
    data = path.read_bytes()
    offset = _header(str(path))[0]
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
    from vail.bgen import _header
    path = _bgen(tmp_path)
    data = bytearray(path.read_bytes())
    offset = _header(str(path))[0]
    # 17 identifying-string bytes, then uint32 position and uint16 allele count.
    data[offset+23:offset+27] = struct.pack('<I',2**32-1)
    path.write_bytes(data)
    with pytest.raises(ValueError, match='truncated BGEN identifier'):
        hl.index_bgen(str(path))
    # Existing index still supplies a valid variant offset. The native decoder
    # independently enforces the same physical-byte bound before allocating.
    with pytest.raises(Exception, match='truncated BGEN identifier'):
        hl.import_bgen(str(path), []).locus.collect()
