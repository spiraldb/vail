"""Small spec-generated layout-2 fixtures exercise native BGEN decoding."""
import struct
import zlib
import pytest
import vail as hl


def _text(value, size=2):
    data = value.encode()
    return len(data).to_bytes(size, 'little') + data


def _bgen(tmp_path, compressed=True, probabilities=None):
    probabilities = probabilities or [(255, 0, False), (0, 255, False), (0, 0, True)]
    n = len(probabilities)
    sample_block = struct.pack('<II', 8 + sum(2+len(f's{i}') for i in range(n)), n)
    sample_block += b''.join(_text(f's{i}') for i in range(n))
    flags = (1 if compressed else 0) | (2 << 2) | (1 << 31)
    header = struct.pack('<IIII', 20+len(sample_block), 20, 1, n) + b'bgen' + struct.pack('<I', flags)
    variant = _text('variant') + _text('rs1') + _text('1') + struct.pack('<IH', 123, 2) + _text('A',4) + _text('G',4)
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
