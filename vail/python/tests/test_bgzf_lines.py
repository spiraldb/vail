"""Public import behavior across independently decoded BGZF source ranges."""
import gzip
import random
import struct
import zlib

import pytest
import vail as hl


def _block(data):
    compressor = zlib.compressobj(wbits=-15)
    payload = compressor.compress(data) + compressor.flush()
    size = len(payload) + 26
    assert size <= 65536
    return (b'\x1f\x8b\x08\x04\0\0\0\0\0\xff\x06\0BC\x02\0'
            + struct.pack('<H', size - 1) + payload
            + struct.pack('<II', zlib.crc32(data), len(data)))


def _write(path, chunks):
    offsets = []
    output = bytearray()
    for chunk in chunks:
        offsets.append(len(output))
        output.extend(_block(chunk))
    output.extend(_block(b''))
    path.write_bytes(output)
    return offsets


def _tbi(path, offsets):
    # Sparse chunk/linear anchors, deliberately including the pseudo-bin whose
    # second pair is record counts rather than virtual offsets.
    data = b'TBI\x01' + struct.pack('<8I', 1, 0, 1, 2, 0, 35, 0, 5) + b'chr1\0'
    data += struct.pack('<IIIQQ', 2, 0, 1, offsets[2] << 16, offsets[-3] << 16)
    data += struct.pack('<IIQQQQ', 37450, 2, offsets[2] << 16, offsets[-3] << 16, 400000, 0)
    data += struct.pack('<IQQ', 2, offsets[2] << 16, offsets[-3] << 16)
    path.with_name(path.name + '.tbi').write_bytes(gzip.compress(data))


@pytest.mark.parametrize('ending', ['\n', '\r', '\r\n'])
@pytest.mark.parametrize('index', ['absent', 'tabix', 'invalid', 'stale', 'csi'])
def test_bgzf_ranges_preserve_all_physical_lines(tmp_path, ending, index):
    rng = random.Random(917)
    rows = [''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=137)) for _ in range(300)]
    rows[7] = ''
    rows[25] = 'café' * 20000  # UTF-8 and a record spanning many blocks/splits.
    rows[177] = ''
    content = ending.join(rows).encode()
    # Empty members are legal, including before and between data members.
    chunks = [b'']
    for start in range(0, len(content), 997):
        chunks.append(content[start:start + 997])
        if start % 3 == 0:
            chunks.append(b'')
    path = tmp_path / 'input.bgz'
    offsets = _write(path, chunks)
    if index == 'tabix':
        _tbi(path, offsets)
    elif index == 'invalid':
        path.with_name(path.name + '.tbi').write_bytes(b'not an index')
    elif index == 'stale':
        _tbi(path, [12345 + i * 29 for i in range(len(offsets))])
    elif index == 'csi':
        path.with_name(path.name + '.csi').write_bytes(b'unsupported index is safely ignored')
    table = hl.import_lines(str(path), min_partitions=4)
    assert [row.text for row in table.collect()] == rows
    assert [row.text for row in table.collect()] == rows
    assert all(row.file == 'file:' + str(path) for row in table.take(3))
    one = hl.import_lines(str(path), file_per_partition=True)
    assert one.n_partitions() == 1
    assert [row.text for row in one.collect()] == rows


@pytest.mark.parametrize('content,expected', [
    (b'', []), (b'\r', ['']), (b'\r\r\n\n', ['', '', '']),
    (b'a\r\nb\rc\nlast\r', ['a', 'b', 'c', 'last']),
])
def test_bgzf_newlines_and_empty_members(tmp_path, content, expected):
    path = tmp_path / 'newlines.bgz'
    _write(path, [b'', *[bytes([value]) for value in content], b''])
    assert [row.text for row in hl.import_lines(str(path), min_partitions=4).collect()] == expected


def test_regular_concatenated_gzip_keeps_serial_decoder(tmp_path):
    path = tmp_path / 'regular.gz'
    path.write_bytes(gzip.compress(b'one\r') + gzip.compress(b'\ntwo\nlast'))
    assert [row.text for row in hl.import_lines(str(path), force=True).collect()] == ['one', 'two', 'last']


def test_bgzf_corruption_is_never_skipped_by_partition_discovery(tmp_path):
    path = tmp_path / 'corrupt.bgz'
    offsets = _write(path, [b'first\n', b'broken\n', b'last\n'])
    data = bytearray(path.read_bytes())
    data[offsets[2] - 8] ^= 1
    path.write_bytes(data)
    with pytest.raises(Exception, match='checksum|CRC|crc|corrupt'):
        hl.import_lines(str(path), min_partitions=3).collect()


@pytest.mark.parametrize('mutation', ['grow', 'rewrite', 'replace'])
def test_prepared_bgzf_ranges_reject_changed_input(tmp_path, mutation):
    path = tmp_path / 'changed.bgz'
    original = [b'first\n', b'second\n', b'last\n'] * 30
    _write(path, original)
    table = hl.import_lines(str(path), min_partitions=3)
    if mutation == 'grow':
        with path.open('ab') as output:
            output.write(_block(b'added\n'))
    elif mutation == 'rewrite':
        _write(path, [b'other\n', *original])
    else:
        replacement = tmp_path / 'replacement.bgz'
        # Identical content/length still has a different file identity.
        replacement.write_bytes(path.read_bytes())
        replacement.replace(path)
    with pytest.raises(Exception, match='BGZF input changed'):
        table.collect()
    fresh = hl.import_lines(str(path))
    assert fresh.count() == (91 if mutation != 'replace' else 90)


def test_bgzf_split_finds_header_at_end_of_search_window(tmp_path):
    import os
    if (os.cpu_count() or 1) < 2:
        pytest.skip('requires at least two source workers')
    path = tmp_path / 'edge.bgz'
    rows = []
    with path.open('wb') as output:
        # Each stored member has exactly65536 compressed bytes. With126 such
        # blocks plus a28-byte EOF marker, the halfway split is14 bytes into
        # block63. The next header starts65522 bytes after the probe start, so
        # its18-byte header straddles the original64KiB search-window end.
        for index in range(126):
            row = f'{index:03d}:' + 'a' * 65500
            rows.append(row)
            data = (row + '\n').encode()
            assert len(data) == 65505
            payload = b'\x01' + struct.pack('<HH', len(data), len(data) ^ 0xffff) + data
            output.write(b'\x1f\x8b\x08\x04\0\0\0\0\0\xff\x06\0BC\x02\0'
                         + struct.pack('<H', 65535) + payload
                         + struct.pack('<II', zlib.crc32(data), len(data)))
        output.write(_block(b''))
    table = hl.import_lines(str(path))
    assert table.n_partitions() >= 2
    assert [row.text for row in table.collect()] == rows
