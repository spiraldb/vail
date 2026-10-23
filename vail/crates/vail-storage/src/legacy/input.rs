use super::*;
use std::io::{self, BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
type Stream = Box<dyn Read + Send>;
fn invalid(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}
pub fn validate(spec: &Value) -> Result<()> {
    match spec["name"].as_str().unwrap_or("") {
        "LEB128BufferSpec"
        | "BlockingBufferSpec"
        | "LZ4SizeBasedBlockBufferSpec"
        | "LZ4BlockBufferSpec"
        | "LZ4HCBlockBufferSpec"
        | "LZ4FastBlockBufferSpec"
        | "ZstdBlockBufferSpec"
        | "ZstdSizedBasedBlockBufferSpec" => validate(&spec["child"]),
        "StreamBufferSpec" | "StreamBlockBufferSpec" | "StreamBlockBufferSpec2" => Ok(()),
        name => Err(format!("unsupported Hail buffer {name}").into()),
    }
}
struct Blocks {
    source: Stream,
    spec: Value,
    current: Cursor<Vec<u8>>,
}
impl Blocks {
    fn block(&mut self) -> io::Result<Option<Vec<u8>>> {
        let name = self.spec["name"].as_str().unwrap_or("");
        if name == "StreamBlockBufferSpec" || name == "StreamBlockBufferSpec2" {
            let mut byte = [0];
            if self.source.read(&mut byte)? == 0 {
                return Ok(None);
            }
            let size = if name.ends_with('2') {
                let mut value = u64::from(byte[0] & 127);
                let mut b = byte[0];
                let mut shift = 7;
                while b & 128 != 0 {
                    if shift >= 35 {
                        return Err(invalid("invalid Hail block varint"));
                    }
                    self.source.read_exact(&mut byte)?;
                    b = byte[0];
                    value |= u64::from(b & 127) << shift;
                    shift += 7;
                }
                if value > u32::MAX as u64 {
                    return Err(invalid("Hail block length overflow"));
                }
                value as usize
            } else {
                let mut rest = [0; 4];
                rest[0] = byte[0];
                self.source.read_exact(&mut rest[1..])?;
                usize::try_from(i32::from_le_bytes(rest)).map_err(invalid)?
            };
            // Hail's block buffers are bounded. Reject corrupt framing before allocating.
            if size > 256 * 1024 * 1024 {
                return Err(invalid("Hail compressed block exceeds 256 MiB"));
            }
            let mut out = vec![0; size];
            self.source.read_exact(&mut out)?;
            return Ok(Some(out));
        }
        // The child is a framed block stream. Read precisely one child frame,
        // rather than concatenating the complete decompressed file.
        Err(invalid("compression layer must wrap a framed block source"))
    }
}
impl Read for Blocks {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.current.read(out)?;
            if n != 0 {
                return Ok(n);
            }
            match self.block()? {
                Some(b) => self.current = Cursor::new(b),
                None => return Ok(0),
            }
        }
    }
}
trait BlockRead: Send {
    fn next(&mut self) -> io::Result<Option<Vec<u8>>>;
}
impl BlockRead for Blocks {
    fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.block()
    }
}
struct Compressed {
    child: Box<dyn BlockRead>,
    spec: Value,
}
impl BlockRead for Compressed {
    fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        let Some(block) = self.child.next()? else {
            return Ok(None);
        };
        let mut c = Cursor::new(block);
        let mut word = || -> io::Result<i32> {
            let mut w = [0; 4];
            c.read_exact(&mut w)?;
            Ok(i32::from_le_bytes(w))
        };
        let name = self.spec["name"].as_str().unwrap_or("");
        if name == "LZ4SizeBasedBlockBufferSpec" {
            match word()? {
                0 => {
                    let p = c.position() as usize;
                    return Ok(Some(c.into_inner()[p..].to_vec()));
                }
                1 => {}
                _ => return Err(invalid("invalid Hail LZ4 compression flag")),
            }
        }
        let mut w = [0; 4];
        c.read_exact(&mut w)?;
        let mut size = i32::from_le_bytes(w);
        if name == "ZstdSizedBasedBlockBufferSpec" {
            if size & 1 == 0 {
                let p = c.position() as usize;
                return Ok(Some(c.into_inner()[p..].to_vec()));
            }
            size >>= 1;
        }
        let limit = self.spec["blockSize"]
            .as_u64()
            .ok_or_else(|| invalid("missing Hail block size"))?;
        if size < 0 || size as u64 > limit || size > 256 * 1024 * 1024 {
            return Err(invalid("invalid Hail decompressed block length"));
        }
        let payload = &c.get_ref()[c.position() as usize..];
        let out = if name.starts_with("Zstd") {
            zstd::bulk::decompress(payload, size as usize).map_err(invalid)?
        } else {
            lz4_flex::block::decompress(payload, size as usize).map_err(invalid)?
        };
        if out.len() != size as usize {
            return Err(invalid("Hail decompressed block size mismatch"));
        }
        Ok(Some(out))
    }
}
struct BlockStream {
    blocks: Box<dyn BlockRead>,
    current: Cursor<Vec<u8>>,
}
impl Read for BlockStream {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.current.read(out)?;
            if n != 0 {
                return Ok(n);
            }
            match self.blocks.next()? {
                Some(b) => self.current = Cursor::new(b),
                None => return Ok(0),
            }
        }
    }
}
fn blocks(source: Stream, spec: &Value) -> Result<Box<dyn BlockRead>> {
    let name = spec["name"].as_str().ok_or("missing Hail buffer name")?;
    if name.starts_with("StreamBlock") {
        Ok(Box::new(Blocks {
            source,
            spec: spec.clone(),
            current: Cursor::new(vec![]),
        }))
    } else {
        Ok(Box::new(Compressed {
            child: blocks(source, &spec["child"])?,
            spec: spec.clone(),
        }))
    }
}
pub struct Input {
    source: BufReader<Stream>,
    pub leb: bool,
    pub bytes: u64,
}
impl Input {
    // The callback accepts only complete logical records from this borrowed
    // buffer; no owned buffering and no speculative Input counter updates.
    pub(super) fn with_buffer<T>(&mut self, f: impl FnOnce(&[u8]) -> (T, usize)) -> Result<T> {
        let available = self.source.fill_buf()?;
        let (value, consumed) = f(available);
        debug_assert!(consumed <= available.len());
        self.source.consume(consumed);
        self.bytes += consumed as u64;
        Ok(value)
    }
    /// Retain exactly `count` terminator-framed values without expanding them.
    pub(super) fn capture_varints(&mut self, count: usize, out: &mut Vec<u8>) -> Result<()> {
        let mut remaining = count;
        while remaining != 0 {
            let consumed = self.with_buffer(|available| {
                let mut consumed = 0;
                for &byte in available {
                    consumed += 1;
                    if byte & 128 == 0 {
                        remaining -= 1;
                        if remaining == 0 { break; }
                    }
                }
                out.extend_from_slice(&available[..consumed]);
                (consumed, consumed)
            })?;
            if consumed == 0 { return Err("truncated Hail data".into()); }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn from_memory(bytes: std::sync::Arc<[u8]>, leb: bool) -> Self {
        Self {
            source: BufReader::with_capacity(65536, Box::new(Cursor::new(bytes)) as Stream),
            leb,
            bytes: 0,
        }
    }
    pub fn open(path: &Path, spec: &Value) -> Result<Self> {
        Self::open_raw(path, spec, 0)
    }
    pub fn open_virtual(path: &Path, spec: &Value, offset: u64) -> Result<Self> {
        let buffer = if spec["name"] == "LEB128BufferSpec" {
            &spec["child"]
        } else {
            spec
        };
        if buffer["name"] == "BlockingBufferSpec" {
            let mut file = std::fs::File::open(path)?;
            if offset >> 16 != 0 {
                file.seek(SeekFrom::Start(offset >> 16))?;
            }
            let source: Stream = Box::new(BufReader::with_capacity(65536, file));
            let mut blocks = blocks(source, &buffer["child"])?;
            let first = blocks
                .next()?
                .ok_or("Hail virtual offset points past end of file")?;
            let within = offset & 0xffff;
            if within > first.len() as u64 {
                return Err("Hail virtual offset exceeds decompressed block".into());
            }
            let mut current = Cursor::new(first);
            current.set_position(within);
            Ok(Self {
                source: BufReader::with_capacity(
                    65536,
                    Box::new(BlockStream { blocks, current }) as Stream,
                ),
                leb: spec["name"] == "LEB128BufferSpec",
                bytes: 0,
            })
        } else {
            Self::open_raw(path, spec, offset)
        }
    }
    pub fn open_raw(path: &Path, spec: &Value, offset: u64) -> Result<Self> {
        let mut file = std::fs::File::open(path)?;
        if offset != 0 {
            file.seek(SeekFrom::Start(offset))?;
        }
        let source: Stream = Box::new(BufReader::with_capacity(65536, file));
        let leb = spec["name"] == "LEB128BufferSpec";
        let spec = if leb { &spec["child"] } else { spec };
        let source = if spec["name"] == "BlockingBufferSpec" {
            Box::new(BlockStream {
                blocks: blocks(source, &spec["child"])?,
                current: Cursor::new(vec![]),
            }) as Stream
        } else {
            source
        };
        Ok(Self {
            source: BufReader::with_capacity(65536, source),
            leb,
            bytes: 0,
        })
    }
    #[inline]
    pub fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0; N];
        self.source.read_exact(&mut out)?;
        self.bytes += N as u64;
        Ok(out)
    }
    #[inline(always)]
    pub fn byte(&mut self) -> Result<u8> {
        let available = self.source.fill_buf()?;
        let value = *available.first().ok_or("truncated Hail data")?;
        self.source.consume(1);
        self.bytes += 1;
        Ok(value)
    }
    #[inline(always)]
    pub fn varint(&mut self, bits: u32) -> Result<u64> {
        if bits == 0 {
            return Err("invalid Hail variable length integer".into());
        }
        // Small integers dominate genotype arrays. Consume the common single
        // byte encoding directly and keep continuation handling out of line.
        let available = self.source.fill_buf()?;
        if let Some(&byte) = available.first()
            && byte < 128
        {
            if bits < 7 && u64::from(byte) >= 1u64 << bits {
                return Err("overflow in Hail variable length integer".into());
            }
            self.source.consume(1);
            self.bytes += 1;
            return Ok(u64::from(byte));
        }
        self.varint_long(bits)
    }
    #[inline(never)]
    fn varint_long(&mut self, bits: u32) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..bits).step_by(7) {
            let b = self.byte()?;
            if shift + 7 > bits && (b & 127) as u64 >= 1u64 << (bits - shift) {
                return Err("overflow in Hail variable length integer".into());
            }
            value |= ((b & 127) as u64) << shift;
            if b & 128 == 0 {
                return Ok(value);
            }
        }
        Err("invalid Hail variable length integer".into())
    }
    /// Borrow a short run of complete one-byte varints without consuming
    /// continuations or a partial buffered run. The caller retains fallback.
    #[inline(always)]
    pub fn try_small_varints(&mut self, count: usize) -> Result<Option<[u8; 8]>> {
        if count == 0 {
            return Ok(Some([0; 8]));
        }
        if count > 8 {
            return Ok(None);
        }
        let available = self.source.fill_buf()?;
        if available.len() < count {
            return Ok(None);
        }
        let values = if available.len() >= 8 {
            let word = u64::from_le_bytes(available[..8].try_into().unwrap());
            let prefix_mask = u64::MAX >> ((8 - count) * 8);
            if word & prefix_mask & 0x8080_8080_8080_8080 != 0 {
                return Ok(None);
            }
            (word & prefix_mask).to_le_bytes()
        } else {
            if available[..count].iter().any(|byte| byte & 128 != 0) {
                return Ok(None);
            }
            let mut values = [0; 8];
            values[..count].copy_from_slice(&available[..count]);
            values
        };
        self.source.consume(count);
        self.bytes += count as u64;
        Ok(Some(values))
    }

    /// Skip encoded values by their terminators. Projection does not need
    /// numeric width validation or intermediate integer values.
    pub fn skip_varints(&mut self, count: usize, _bits: u32) -> Result<()> {
        let mut remaining = count;
        while remaining != 0 {
            let available = self.source.fill_buf()?;
            if available.is_empty() {
                return Err("truncated Hail data".into());
            }
            let mut consumed = 0;
            while available.len() - consumed >= 8 {
                let word =
                    u64::from_le_bytes(available[consumed..consumed + 8].try_into().unwrap());
                // Small arrays commonly consist entirely of one-byte values.
                // Consume their prefix without popcount/Nth-bit selection.
                let high_bits = word & 0x8080_8080_8080_8080;
                if remaining <= 8 {
                    let prefix_mask = u64::MAX >> ((8 - remaining) * 8);
                    if high_bits & prefix_mask == 0 {
                        consumed += remaining;
                        remaining = 0;
                        break;
                    }
                }
                let mut terminators = high_bits ^ 0x8080_8080_8080_8080;
                let count = terminators.count_ones() as usize;
                if count < remaining {
                    remaining -= count;
                    consumed += 8;
                } else {
                    // The final word can include bytes from the next field.
                    // Select the Nth terminator and consume its exact prefix.
                    for _ in 1..remaining {
                        terminators &= terminators - 1;
                    }
                    consumed += terminators.trailing_zeros() as usize / 8 + 1;
                    remaining = 0;
                    break;
                }
            }
            if remaining != 0 {
                for &byte in &available[consumed..] {
                    consumed += 1;
                    if byte & 128 == 0 {
                        remaining -= 1;
                        if remaining == 0 {
                            break;
                        }
                    }
                }
            }
            self.source.consume(consumed);
            self.bytes += consumed as u64;
        }
        Ok(())
    }
    #[inline]
    pub fn int(&mut self, bits: u32) -> Result<i64> {
        Ok(if self.leb {
            let v = self.varint(bits)?;
            if bits == 32 {
                v as u32 as i32 as i64
            } else {
                v as i64
            }
        } else if bits == 32 {
            i32::from_le_bytes(self.fixed()?) as i64
        } else {
            i64::from_le_bytes(self.fixed()?)
        })
    }
    #[inline]
    pub fn length(&mut self, e: &Encoding) -> Result<usize> {
        let v = if e.kind.ends_with('2') {
            self.varint(32)? as i64
        } else {
            self.int(32)?
        };
        Ok(usize::try_from(v).map_err(|_| "negative Hail collection length")?)
    }
    #[inline]
    pub fn bytes(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.source.by_ref().take(n as u64).read_to_end(&mut out)?;
        self.bytes += out.len() as u64;
        if out.len() != n {
            return Err("truncated Hail data".into());
        }
        Ok(out)
    }
    pub fn skip(&mut self, n: usize) -> Result<()> {
        // Consume buffered bytes directly. Projected-out variable length values
        // need framing validation, but no copy into a throwaway scratch buffer.
        let mut remaining = n;
        while remaining > 0 {
            let available = self.source.fill_buf()?;
            if available.is_empty() {
                return Err("truncated Hail data".into());
            }
            let count = remaining.min(available.len());
            self.source.consume(count);
            self.bytes += count as u64;
            remaining -= count;
        }
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        let mut b = [0];
        if self.source.read(&mut b)? != 0 {
            return Err("trailing bytes after Hail partition terminator".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffered(bytes: Vec<u8>) -> Input {
        Input {
            source: BufReader::with_capacity(2, Box::new(Cursor::new(bytes)) as Stream),
            leb: true,
            bytes: 0,
        }
    }

    #[test]
    fn projected_skip_crosses_buffer_boundaries() -> Result<()> {
        let mut input = buffered(vec![7, 8, 9, 10, 11, 12, 13]);
        assert_eq!(input.byte()?, 7);
        input.skip(5)?;
        assert_eq!(input.byte()?, 13);
        assert_eq!(input.bytes, 7);
        assert!(input.skip(1).is_err());
        Ok(())
    }

    #[test]
    fn short_varint_word_probe_masks_unused_bytes() -> Result<()> {
        for count in 1..=8 {
            let mut input = buffered(vec![]);
            input.source = BufReader::with_capacity(
                32,
                Box::new(Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 128, 1])) as Stream,
            );
            let values = input.try_small_varints(count)?.unwrap();
            assert_eq!(&values[..count], &(1..=count as u8).collect::<Vec<_>>());
            assert!(values[count..].iter().all(|v| *v == 0));
            assert_eq!(input.bytes, count as u64);
        }
        let mut input = buffered(vec![]);
        input.source = BufReader::with_capacity(
            32,
            Box::new(Cursor::new(vec![1u8, 2, 128, 1, 5, 6, 7, 8])) as Stream,
        );
        assert_eq!(
            input.try_small_varints(2)?.unwrap(),
            [1, 2, 0, 0, 0, 0, 0, 0]
        );
        assert!(input.try_small_varints(1)?.is_none());
        assert_eq!(input.varint(32)?, 128);
        Ok(())
    }

    #[test]
    fn short_varint_probe_preserves_fallback_position() -> Result<()> {
        let mut input = buffered(vec![1, 2, 128, 1, 42]);
        assert!(input.try_small_varints(3)?.is_none());
        assert_eq!(input.bytes, 0);
        assert_eq!(input.try_small_varints(2)?.unwrap()[..2], [1, 2]);
        assert_eq!(input.bytes, 2);
        assert!(input.try_small_varints(1)?.is_none());
        assert_eq!(input.varint(32)?, 128);
        assert_eq!(input.byte()?, 42);
        let mut input = buffered(vec![1, 2, 3, 4]);
        // The tiny test buffer cannot hold the complete requested run.
        assert!(input.try_small_varints(4)?.is_none());
        assert_eq!(input.bytes, 0);
        assert_eq!(input.varint(32)?, 1);
        assert!(input.try_small_varints(9)?.is_none());
        let mut empty = buffered(vec![]);
        assert_eq!(empty.try_small_varints(0)?, Some([0; 8]));
        Ok(())
    }

    #[test]
    fn bulk_varint_skip_preserves_width_and_position() -> Result<()> {
        let mut input = buffered(vec![1, 0xac, 0x82, 0x01, 9, 42]);
        input.skip_varints(3, 32)?;
        assert_eq!(input.bytes, 5);
        assert_eq!(input.byte()?, 42);
        let mut wider_than_int32 = buffered(vec![0xff, 0xff, 0xff, 0xff, 0x1f, 42]);
        wider_than_int32.skip_varints(1, 32)?;
        assert_eq!(wider_than_int32.bytes, 5);
        assert_eq!(wider_than_int32.byte()?, 42);
        assert!(buffered(vec![0x80]).skip_varints(1, 32).is_err());
        assert!(buffered(vec![0x80; 10]).skip_varints(1, 64).is_err());
        Ok(())
    }

    #[test]
    fn projected_varint_prefix_and_mixed_word_paths_match_scalar_cursor() -> Result<()> {
        let mut values = vec![vec![1u8], vec![2], vec![3], vec![4], vec![128, 0]];
        let mut long = vec![255; 12];
        long.push(1);
        values.extend([long, vec![5], vec![6], vec![7], vec![8]]);
        let mut bytes = Vec::new();
        let mut offsets = vec![0];
        for value in values {
            bytes.extend(value);
            offsets.push(bytes.len());
        }
        bytes.push(42);
        for capacity in [1usize, 2, 7, 8, 32, 65536] {
            for (count, &expected) in offsets.iter().enumerate() {
                let mut input = Input {
                    source: BufReader::with_capacity(
                        capacity,
                        Box::new(Cursor::new(bytes.clone())) as Stream,
                    ),
                    leb: true,
                    bytes: 0,
                };
                input.skip_varints(count, 32)?;
                assert_eq!(
                    input.bytes, expected as u64,
                    "capacity={capacity} count={count}"
                );
                assert_eq!(input.byte()?, bytes[expected]);
            }
        }
        Ok(())
    }

    #[test]
    fn projected_varint_skip_accepts_long_values_and_stops_at_exact_terminator() -> Result<()> {
        for capacity in [1usize, 2, 7, 8, 32, 65536] {
            let mut bytes = vec![128u8; 37];
            bytes.extend([255, 127, 1, 128, 0, 42, 43, 44, 45, 46, 47, 48, 49]);
            let mut input = Input {
                source: BufReader::with_capacity(capacity, Box::new(Cursor::new(bytes)) as Stream),
                leb: true,
                bytes: 0,
            };
            input.skip_varints(3, 32)?;
            assert_eq!(input.bytes, 42);
            assert_eq!(input.byte()?, 42);
        }
        let mut input = buffered(vec![128; 19]);
        assert!(input.skip_varints(1, 32).is_err());
        assert_eq!(input.bytes, 19);
        Ok(())
    }

    #[test]
    fn bulk_varint_word_skip_stops_at_requested_value() -> Result<()> {
        for count in 1..=16 {
            let mut input = buffered(vec![]);
            input.source =
                BufReader::with_capacity(32, Box::new(Cursor::new(vec![7u8; 24])) as Stream);
            input.skip_varints(count, 32)?;
            assert_eq!(input.bytes, count as u64);
            assert_eq!(input.byte()?, 7);
        }
        let mut input = buffered(vec![]);
        input.source = BufReader::with_capacity(
            32,
            Box::new(Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 0xac, 2, 42])) as Stream,
        );
        input.skip_varints(9, 32)?;
        assert_eq!(input.bytes, 10);
        assert_eq!(input.byte()?, 42);
        Ok(())
    }

    #[test]
    fn varint_crosses_buffer_boundary_and_checks_overflow() -> Result<()> {
        let mut input = buffered(vec![0xac, 0x82, 0x01]);
        assert_eq!(input.varint(32)?, 16684);
        assert_eq!(input.bytes, 3);
        assert!(
            buffered(vec![0xff, 0xff, 0xff, 0xff, 0x1f])
                .varint(32)
                .is_err()
        );
        Ok(())
    }
    #[test]
    #[ignore = "manual real-file read/decompression benchmark"]
    fn benchmark_real_entry_part_read_and_decompression() -> Result<()> {
        use std::time::Instant;
        let matrix = std::env::var("VAIL_LEGACY_PROFILE_MATRIX").unwrap_or_else(|_| {
            "/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt".into()
        });
        let dataset = Dataset::open(&matrix, "MatrixTableSpec")?;
        let entries = dataset.component("entries")?;
        let part = std::env::var("VAIL_LEGACY_PROFILE_PART")
            .ok()
            .map(|value| value.parse::<usize>())
            .transpose()?
            .unwrap_or(0);
        let path = entries
            .paths
            .get(part)
            .ok_or("profile partition out of bounds")?;
        // Keep compressed bytes owned outside the timers. This stage isolates
        // framing/decompression from filesystem reads and numeric decoding.
        let compressed: Arc<[u8]> = std::fs::read(path)?.into();
        let mut read_seconds = Vec::new();
        let mut decompress_seconds = Vec::new();
        let mut memory_decompress_seconds = Vec::new();
        let mut uncompressed = None;
        for iteration in 0..6 {
            let mut file = std::fs::File::open(path)?;
            let start = Instant::now();
            let count = io::copy(&mut file, &mut io::sink())?;
            let elapsed = start.elapsed().as_secs_f64();
            assert_eq!(count, compressed.len() as u64);
            if iteration != 0 {
                read_seconds.push(elapsed);
            }

            let mut input = Input::open(path, &entries.buffer)?;
            let start = Instant::now();
            let count = io::copy(&mut input.source, &mut io::sink())?;
            let elapsed = start.elapsed().as_secs_f64();
            if let Some(expected) = uncompressed {
                assert_eq!(count, expected);
            } else {
                uncompressed = Some(count);
            }
            if iteration != 0 {
                decompress_seconds.push(elapsed);
            }

            let spec = if entries.buffer["name"] == "LEB128BufferSpec" {
                &entries.buffer["child"]
            } else {
                &entries.buffer
            };
            let source: Stream = Box::new(Cursor::new(compressed.clone()));
            let source: Stream = if spec["name"] == "BlockingBufferSpec" {
                Box::new(BlockStream {
                    blocks: blocks(source, &spec["child"])?,
                    current: Cursor::new(vec![]),
                })
            } else {
                source
            };
            let mut source = BufReader::with_capacity(65536, source);
            let start = Instant::now();
            let count = io::copy(&mut source, &mut io::sink())?;
            let elapsed = start.elapsed().as_secs_f64();
            assert_eq!(Some(count), uncompressed);
            if iteration != 0 {
                memory_decompress_seconds.push(elapsed);
            }
        }
        fn stage(mut samples: Vec<f64>, bytes: u64) -> Value {
            let original = samples.clone();
            samples.sort_by(f64::total_cmp);
            let median = samples[samples.len() / 2];
            serde_json::json!({"seconds":original, "median_seconds":median,
                "median_mib_per_second":bytes as f64 / (1024.0 * 1024.0) / median})
        }
        let uncompressed = uncompressed.unwrap();
        println!(
            "{}",
            serde_json::json!({"benchmark":"legacy_entry_part_stages", "path":path,
            "compressed_bytes":compressed.len(), "uncompressed_bytes":uncompressed,
            "cached_file_read":stage(read_seconds, compressed.len() as u64),
            "file_read_and_decompress":stage(decompress_seconds, uncompressed),
            "memory_decompress":stage(memory_decompress_seconds, uncompressed)})
        );
        Ok(())
    }
}
