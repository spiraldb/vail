use super::*;
use std::io::{self, BufRead, Cursor, Read, Seek, SeekFrom};
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
trait BlockRead: Send {
    fn read_block(&mut self, out: &mut Vec<u8>) -> io::Result<bool>;
}
struct Blocks {
    source: Stream,
    spec: Value,
}
impl BlockRead for Blocks {
    fn read_block(&mut self, out: &mut Vec<u8>) -> io::Result<bool> {
        let name = self.spec["name"].as_str().unwrap_or("");
        if name != "StreamBlockBufferSpec" && name != "StreamBlockBufferSpec2" {
            return Err(invalid("compression layer must wrap a framed block source"));
        }
        let mut byte = [0];
        if self.source.read(&mut byte)? == 0 {
            return Ok(false);
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
        if size > 256 * 1024 * 1024 {
            return Err(invalid("Hail compressed block exceeds 256 MiB"));
        }
        out.resize(size, 0);
        self.source.read_exact(out)?;
        Ok(true)
    }
}
struct Compressed {
    child: Box<dyn BlockRead>,
    spec: Value,
    zstd_decoder: Option<zstd::bulk::Decompressor<'static>>,
    compressed: Vec<u8>,
}
impl BlockRead for Compressed {
    fn read_block(&mut self, out: &mut Vec<u8>) -> io::Result<bool> {
        if !self.child.read_block(&mut self.compressed)? {
            return Ok(false);
        }
        let mut c = Cursor::new(self.compressed.as_slice());
        let name = self.spec["name"].as_str().unwrap_or("");
        if name == "LZ4SizeBasedBlockBufferSpec" {
            let mut flag = [0; 4];
            c.read_exact(&mut flag)?;
            match i32::from_le_bytes(flag) {
                0 => {
                    out.clear();
                    out.extend_from_slice(&self.compressed[c.position() as usize..]);
                    return Ok(true);
                }
                1 => {}
                _ => return Err(invalid("invalid Hail LZ4 compression flag")),
            }
        }
        let mut word = [0; 4];
        c.read_exact(&mut word)?;
        let mut size = i32::from_le_bytes(word);
        if name == "ZstdSizedBasedBlockBufferSpec" {
            if size & 1 == 0 {
                out.clear();
                out.extend_from_slice(&self.compressed[c.position() as usize..]);
                return Ok(true);
            }
            size >>= 1;
        }
        let limit = self.spec["blockSize"]
            .as_u64()
            .ok_or_else(|| invalid("missing Hail block size"))?;
        if size < 0 || size as u64 > limit || size > 256 * 1024 * 1024 {
            return Err(invalid("invalid Hail decompressed block length"));
        }
        let payload = &self.compressed[c.position() as usize..];
        let written = if name.starts_with("Zstd") {
            out.resize(size as usize, 0);
            self.zstd_decoder
                .as_mut()
                .ok_or_else(|| invalid("missing Zstd decoder"))?
                .decompress_to_buffer(payload, out.as_mut_slice())
                .map_err(invalid)?
        } else {
            out.resize(size as usize, 0);
            lz4_flex::block::decompress_into(payload, out.as_mut_slice()).map_err(invalid)?
        };
        if written != size as usize {
            return Err(invalid("Hail decompressed block size mismatch"));
        }
        out.truncate(written);
        Ok(true)
    }
}
struct RawBlocks {
    source: Stream,
    capacity: usize,
}
impl BlockRead for RawBlocks {
    fn read_block(&mut self, out: &mut Vec<u8>) -> io::Result<bool> {
        out.resize(self.capacity, 0);
        let n = self.source.read(out)?;
        out.truncate(n);
        Ok(n != 0)
    }
}
// Concrete BufRead keeps virtual dispatch at block refills, never per value.
struct BlockStream {
    blocks: Box<dyn BlockRead>,
    decoded: Vec<u8>,
    position: usize,
    eof: bool,
}
impl BlockStream {
    fn new(blocks: Box<dyn BlockRead>) -> Self {
        Self {
            blocks,
            decoded: Vec::new(),
            position: 0,
            eof: false,
        }
    }
    #[cfg(test)]
    fn with_capacity(capacity: usize, source: Stream) -> Self {
        Self::raw(source, capacity)
    }
    fn raw(source: Stream, capacity: usize) -> Self {
        Self::new(Box::new(RawBlocks {
            source,
            capacity: capacity.max(1),
        }))
    }
    fn load_first(&mut self, within: usize) -> Result<()> {
        if !self.blocks.read_block(&mut self.decoded)? {
            return Err("Hail virtual offset points past end of file".into());
        }
        if within > self.decoded.len() {
            return Err("Hail virtual offset exceeds decompressed block".into());
        }
        self.position = within;
        Ok(())
    }
}
impl BufRead for BlockStream {
    #[inline]
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.position == self.decoded.len() && !self.eof {
            match self.blocks.read_block(&mut self.decoded) {
                Ok(false) => {
                    self.decoded.clear();
                    self.eof = true;
                }
                Ok(true) => {}
                Err(error) => {
                    // Failed fills never publish a partially overwritten block.
                    self.position = self.decoded.len();
                    return Err(error);
                }
            }
            self.position = 0;
        }
        Ok(&self.decoded[self.position..])
    }
    #[inline]
    fn consume(&mut self, amount: usize) {
        debug_assert!(amount <= self.decoded.len() - self.position);
        self.position += amount;
    }
}
impl Read for BlockStream {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let available = self.fill_buf()?;
        let n = out.len().min(available.len());
        out[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}
fn blocks(source: Stream, spec: &Value) -> Result<Box<dyn BlockRead>> {
    let name = spec["name"].as_str().ok_or("missing Hail buffer name")?;
    if name.starts_with("StreamBlock") {
        Ok(Box::new(Blocks {
            source,
            spec: spec.clone(),
        }))
    } else {
        Ok(Box::new(Compressed {
            child: blocks(source, &spec["child"])?,
            spec: spec.clone(),
            compressed: Vec::new(),
            zstd_decoder: if name.starts_with("Zstd") {
                Some(zstd::bulk::Decompressor::new()?)
            } else {
                None
            },
        }))
    }
}
pub struct Input {
    source: BlockStream,
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
    #[cfg(test)]
    pub(super) fn from_memory(bytes: std::sync::Arc<[u8]>, leb: bool) -> Self {
        Self {
            source: BlockStream::with_capacity(65536, Box::new(Cursor::new(bytes)) as Stream),
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
            let mut source = BlockStream::new(blocks(Box::new(file), &buffer["child"])?);
            source.load_first((offset & 0xffff) as usize)?;
            Ok(Self {
                source,
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
        let source: Stream = Box::new(file);
        let leb = spec["name"] == "LEB128BufferSpec";
        let spec = if leb { &spec["child"] } else { spec };
        let source = if spec["name"] == "BlockingBufferSpec" {
            BlockStream::new(blocks(source, &spec["child"])?)
        } else {
            BlockStream::raw(source, 65536)
        };
        Ok(Self {
            source,
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
