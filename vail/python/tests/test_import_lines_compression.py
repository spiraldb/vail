import gzip
import struct
import zlib

import pytest
import vail as hl


def _bgzf(data):
    payload = zlib.compress(data, wbits=-15)
    size = 18 + len(payload) + 8
    return (b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00"
            + struct.pack('<H', size - 1) + payload
            + struct.pack('<II', zlib.crc32(data), len(data)))


@pytest.mark.parametrize('suffix,kwargs', [('.gz', {'force': True}), ('.bgz', {}), ('.txt', {'force_bgz': True})])
def test_concatenated_compressed_members_preserve_lines(tmp_path, suffix, kwargs):
    path = tmp_path / ('lines' + suffix)
    # Unicode and a CRLF are deliberately split across BGZF member boundaries.
    path.write_bytes(_bgzf(b'a\r') + _bgzf(b'\n\n\xc3') + _bgzf(b'') + _bgzf(b'\xa9\nlast') + _bgzf(b''))
    table = hl.import_lines(path, min_partitions=3, **kwargs)
    assert table.n_partitions() == 3
    assert [(row.file, row.text) for row in table.collect()] == [
        ('file:' + str(path), value) for value in ['a', '', 'é', 'last']]


def test_gzip_is_lazy_and_checks_corruption(tmp_path):
    path = tmp_path / 'lines.gz'
    path.write_bytes(gzip.compress(b'old\n'))
    table = hl.import_lines(path, force=True)
    path.write_bytes(gzip.compress(b'new\n'))
    assert table.collect()[0].text == 'new'
    path.write_bytes(gzip.compress(b'new\n')[:-3])
    with pytest.raises(Exception):
        table.collect()


def test_gzip_requires_explicit_force_and_boolean_flags(tmp_path):
    path = tmp_path / 'lines.gz'
    path.write_bytes(gzip.compress(b'x\n'))
    with pytest.raises(ValueError, match='force'):
        hl.import_lines(path)
    with pytest.raises(TypeError, match='bool'):
        hl.import_lines(path, force=1)


@pytest.mark.parametrize('suffix', ['.gz', '.bgz'])
def test_compressed_table_export_roundtrip_and_member_sizes(tmp_path, suffix):
    import random
    rng = random.Random(0)
    values = [''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=1000)) for _ in range(200)]
    table = hl.Table.parallelize([{'text': value} for value in values])
    path = tmp_path / ('export' + suffix)
    table.export(path)
    assert gzip.decompress(path.read_bytes()).decode().splitlines() == ['text', *values]
    imported = hl.import_table(path, force=True)
    assert [row.text for row in imported.collect()] == values
    if suffix == '.bgz':
        data, offset = path.read_bytes(), 0
        while offset < len(data):
            assert data[offset:offset + 4] == b'\x1f\x8b\x08\x04'
            assert data[offset + 12:offset + 16] == b'BC\x02\x00'
            size = struct.unpack_from('<H', data, offset + 16)[0] + 1
            assert size <= 65536
            member = data[offset:offset + size]
            assert len(gzip.decompress(member)) <= 65536
            offset += size
        assert offset == len(data)
        assert gzip.decompress(data[-28:]) == b''
