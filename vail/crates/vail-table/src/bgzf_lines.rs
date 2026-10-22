//! Parallel local BGZF text scans. Each range begins/ends immediately after a
//! physical newline, expressed as a BGZF virtual position. Planning probes only
//! split boundaries; it never inflates the whole input or trusts tabix coverage.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, builder::StringBuilder};
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use flate2::{Crc, Decompress, FlushDecompress, Status};
use futures::stream::BoxStream;
use memchr::memchr2;
use serde::{Deserialize, Serialize};

const MAX_BLOCK: usize = 65_536;
const MIN_SPLIT_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Position {
    block: u64,
    offset: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Partition {
    start: Position,
    end: Position,
}

fn invalid(message: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Parse the mandatory BGZF extra field, including additional legal subfields.
fn header(bytes: &[u8]) -> Option<(usize, usize)> {
    if bytes.len() < 12 || bytes[..4] != [31, 139, 8, 4] {
        return None;
    }
    let header_len = 12 + u16::from_le_bytes([bytes[10], bytes[11]]) as usize;
    if header_len > bytes.len() || header_len > MAX_BLOCK - 8 {
        return None;
    }
    let mut index = 12;
    let mut size = None;
    while index < header_len {
        if index + 4 > header_len {
            return None;
        }
        let len = u16::from_le_bytes([bytes[index + 2], bytes[index + 3]]) as usize;
        if index + 4 + len > header_len {
            return None;
        }
        if bytes[index..index + 2] == *b"BC" {
            if len != 2 || size.is_some() {
                return None;
            }
            size = Some(u16::from_le_bytes([bytes[index + 4], bytes[index + 5]]) as usize + 1);
        }
        index += 4 + len;
    }
    let size = size?;
    (size >= header_len + 8 && size <= MAX_BLOCK).then_some((header_len, size))
}

/// One inflater and fixed buffers per worker/probe, reused for every block.
struct Blocks {
    file: File,
    size: u64,
    compressed: Vec<u8>,
    decoded: Vec<u8>,
    inflater: Decompress,
    crc: Crc,
}
impl Blocks {
    fn open(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            file,
            size,
            compressed: vec![0; MAX_BLOCK],
            decoded: vec![0; MAX_BLOCK + 1],
            inflater: Decompress::new(false),
            crc: Crc::new(),
        })
    }

    fn read(&mut self, offset: u64) -> io::Result<(u64, usize)> {
        if offset >= self.size {
            return Err(invalid("BGZF block past end of file"));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut self.compressed[..12])?;
        let extra = u16::from_le_bytes([self.compressed[10], self.compressed[11]]) as usize;
        if extra > MAX_BLOCK - 20 {
            return Err(invalid("invalid BGZF extra field length"));
        }
        self.file.read_exact(&mut self.compressed[12..12 + extra])?;
        let (head, size) = header(&self.compressed[..12 + extra])
            .ok_or_else(|| invalid("invalid BGZF block header"))?;
        if offset + size as u64 > self.size {
            return Err(invalid("truncated BGZF block"));
        }
        self.file.read_exact(&mut self.compressed[head..size])?;
        let expected_crc =
            u32::from_le_bytes(self.compressed[size - 8..size - 4].try_into().unwrap());
        let expected_size =
            u32::from_le_bytes(self.compressed[size - 4..size].try_into().unwrap()) as usize;
        if expected_size > MAX_BLOCK {
            return Err(invalid("oversized BGZF decoded block"));
        }
        self.inflater.reset(false);
        let status = self
            .inflater
            .decompress(
                &self.compressed[head..size - 8],
                &mut self.decoded,
                FlushDecompress::Finish,
            )
            .map_err(|error| invalid(format!("invalid BGZF DEFLATE data: {error}")))?;
        if status != Status::StreamEnd
            || self.inflater.total_out() as usize != expected_size
            || self.inflater.total_in() as usize != size - head - 8
        {
            return Err(invalid("invalid BGZF decoded block size"));
        }
        self.crc.reset();
        self.crc.update(&self.decoded[..expected_size]);
        if self.crc.sum() != expected_crc {
            return Err(invalid("BGZF checksum mismatch"));
        }
        Ok((offset + size as u64, expected_size))
    }

    /// Like Hail's BGzipInputStream.resetState: search forward at a compressed
    /// split. Validate the whole candidate member, including CRC and ISIZE.
    fn find_block(&mut self, split: u64) -> io::Result<u64> {
        let end = self.size.min(split.saturating_add(MAX_BLOCK as u64));
        let mut probe = vec![0; (end - split) as usize];
        self.file.seek(SeekFrom::Start(split))?;
        self.file.read_exact(&mut probe)?;
        for index in 0..probe.len().saturating_sub(11) {
            if probe[index] == 31 && header(&probe[index..]).is_some() {
                let position = split + index as u64;
                if self.read(position).is_ok() {
                    return Ok(position);
                }
            }
        }
        // A split may lie inside the final member and have no following block.
        Ok(self.size)
    }

    fn line_boundary(&mut self, mut block: u64) -> io::Result<Position> {
        let mut pending_cr = false;
        while block < self.size {
            let (next, len) = self.read(block)?;
            if len == 0 {
                block = next;
                continue;
            }
            if pending_cr {
                return Ok(Position {
                    block,
                    offset: usize::from(self.decoded[0] == b'\n'),
                });
            }
            if let Some(index) = memchr2(b'\r', b'\n', &self.decoded[..len]) {
                if self.decoded[index] == b'\r' && index + 1 == len {
                    pending_cr = true;
                } else {
                    let offset = index
                        + 1
                        + usize::from(
                            self.decoded[index] == b'\r' && self.decoded[index + 1] == b'\n',
                        );
                    return Ok(if offset == len {
                        Position {
                            block: next,
                            offset: 0,
                        }
                    } else {
                        Position { block, offset }
                    });
                }
            }
            block = next;
        }
        Ok(Position {
            block: self.size,
            offset: 0,
        })
    }
}

/// Tabix bins/chunks and linear offsets are sparse hints, NOT file coverage.
/// Pseudo-bin 37450 contains counts rather than virtual positions. Bad, stale,
/// or unsupported sidecars fall back to ordinary header discovery.
fn tabix_anchors(path: &str, file_size: u64) -> Option<Vec<u64>> {
    let file = File::open(format!("{path}.tbi")).ok()?;
    let mut bytes = Vec::new();
    flate2::read::MultiGzDecoder::new(file)
        .take(64 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > 64 * 1024 * 1024 || bytes.get(..4)? != b"TBI\x01" {
        return None;
    }
    let mut input = &bytes[4..];
    fn int(input: &mut &[u8]) -> Option<u32> {
        let value = u32::from_le_bytes(input.get(..4)?.try_into().ok()?);
        *input = &input[4..];
        Some(value)
    }
    fn long(input: &mut &[u8]) -> Option<u64> {
        let value = u64::from_le_bytes(input.get(..8)?.try_into().ok()?);
        *input = &input[8..];
        Some(value)
    }
    let refs = int(&mut input)? as usize;
    for _ in 0..6 {
        int(&mut input)?;
    }
    let names = int(&mut input)? as usize;
    input = input.get(names..)?;
    let mut anchors = vec![0];
    for _ in 0..refs {
        let bins = int(&mut input)?;
        for _ in 0..bins {
            let bin = int(&mut input)?;
            let chunks = int(&mut input)?;
            for _ in 0..chunks {
                let start = long(&mut input)? >> 16;
                let end = long(&mut input)? >> 16;
                if bin < 37450 {
                    if start < file_size {
                        anchors.push(start);
                    }
                    if end < file_size {
                        anchors.push(end);
                    }
                }
            }
        }
        for _ in 0..int(&mut input)? {
            let offset = long(&mut input)? >> 16;
            if offset < file_size {
                anchors.push(offset);
            }
        }
    }
    anchors.sort_unstable();
    anchors.dedup();
    Some(anchors)
}

/// None means ordinary gzip: preserve its serial, concatenated-member reader.
pub(crate) fn partitions(
    path: &str,
    requested: Option<usize>,
) -> io::Result<Option<Vec<Partition>>> {
    let mut blocks = match Blocks::open(path) {
        Ok(blocks) => blocks,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if blocks.size < 18 {
        return Ok(None);
    }
    blocks.file.read_exact(&mut blocks.compressed[..12])?;
    let extra = u16::from_le_bytes([blocks.compressed[10], blocks.compressed[11]]) as usize;
    if extra > MAX_BLOCK - 20 || 12 + extra as u64 > blocks.size {
        return Ok(None);
    }
    blocks
        .file
        .read_exact(&mut blocks.compressed[12..12 + extra])?;
    if header(&blocks.compressed[..12 + extra]).is_none() {
        return Ok(None);
    }
    // A valid-looking first BGZF member must not silently fall back on CRC failure.
    blocks.read(0)?;
    let threads = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(crate::EXECUTION_CONCURRENCY);
    let count = requested
        .unwrap_or_else(|| threads.min(blocks.size.div_ceil(MIN_SPLIT_BYTES) as usize))
        .max(1);
    // At most one useful boundary per compressed byte. This also bounds absurd
    // min_partitions requests without introducing empty source ranges.
    let count = count
        .min(blocks.size as usize)
        .min(crate::EXECUTION_CONCURRENCY);
    let anchors = tabix_anchors(path, blocks.size).unwrap_or_default();
    let mut positions = vec![Position {
        block: 0,
        offset: 0,
    }];
    for index in 1..count {
        let target = (blocks.size as u128 * index as u128 / count as u128) as u64;
        // Reuse a previously found boundary when several proposed splits fall
        // inside the same long record rather than repeatedly scanning its suffix.
        if target <= positions.last().unwrap().block {
            continue;
        }
        let anchor = anchors
            .partition_point(|&position| position <= target)
            .checked_sub(1)
            .map(|i| anchors[i]);
        let block = if let Some(anchor) = anchor.filter(|&anchor| anchor > 0) {
            // Only use a sparse hint after validation against this input. Header
            // traversal from it reaches the desired byte split without inflating
            // intervening blocks or relying on chromosome/chunk completeness.
            if blocks.read(anchor).is_ok() {
                let mut current = anchor;
                while current < target && current < blocks.size {
                    blocks.file.seek(SeekFrom::Start(current))?;
                    blocks.file.read_exact(&mut blocks.compressed[..12])?;
                    let extra =
                        u16::from_le_bytes([blocks.compressed[10], blocks.compressed[11]]) as usize;
                    if extra > MAX_BLOCK - 20 {
                        return Err(invalid("invalid BGZF extra length"));
                    }
                    blocks
                        .file
                        .read_exact(&mut blocks.compressed[12..12 + extra])?;
                    let (_, size) = header(&blocks.compressed[..12 + extra])
                        .ok_or_else(|| invalid("invalid BGZF header"))?;
                    current += size as u64;
                }
                current
            } else {
                blocks.find_block(target)?
            }
        } else {
            blocks.find_block(target)?
        };
        positions.push(blocks.line_boundary(block)?);
    }
    positions.push(Position {
        block: blocks.size,
        offset: 0,
    });
    positions.sort_unstable();
    positions.dedup();
    Ok(Some(
        positions
            .windows(2)
            .map(|pair| Partition {
                start: pair[0],
                end: pair[1],
            })
            .collect(),
    ))
}

pub(crate) fn batches(
    path: String,
    range: Partition,
    schema: SchemaRef,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    // Blocking file I/O and inflate run in independent workers with bounded
    // backpressure. Dropping the receiver cancels the worker at its next batch.
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let worker = tokio::task::spawn_blocking(move || {
        let result = scan(&path, &range, schema, batch_size, &sender);
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(DataFusionError::Execution(format!(
                "could not read BGZF lines: {error}"
            ))));
        }
    });
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = receiver.recv().await { yield batch?; }
        worker.await.map_err(|error| DataFusionError::Execution(format!("BGZF scan worker failed: {error}")))?;
    })
}

fn scan(
    path: &str,
    range: &Partition,
    schema: SchemaRef,
    batch_size: usize,
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> io::Result<()> {
    let mut blocks = Blocks::open(path)?;
    let mut block = range.start.block;
    let mut pending = Vec::new();
    let mut builder = StringBuilder::with_capacity(batch_size, batch_size * 32);
    let mut rows = 0;
    let mut after_cr = false;
    while block < blocks.size && block <= range.end.block {
        let (next, len) = blocks.read(block)?;
        let start = if block == range.start.block {
            range.start.offset
        } else {
            0
        };
        let end = if block == range.end.block {
            range.end.offset
        } else {
            len
        };
        if start > end || end > len {
            return Err(invalid("BGZF virtual offset exceeds block"));
        }
        let mut data = &blocks.decoded[start..end];
        if after_cr && !data.is_empty() {
            if data[0] == b'\n' {
                data = &data[1..];
            }
            after_cr = false;
        }
        while let Some(index) = memchr2(b'\r', b'\n', data) {
            let text = if pending.is_empty() {
                &data[..index]
            } else {
                pending.extend_from_slice(&data[..index]);
                &pending
            };
            builder.append_value(std::str::from_utf8(text).map_err(invalid)?);
            pending.clear();
            rows += 1;
            let cr = data[index] == b'\r';
            data = &data[index + 1..];
            if cr {
                if data.first() == Some(&b'\n') {
                    data = &data[1..];
                } else if data.is_empty() {
                    after_cr = true;
                }
            }
            if rows == batch_size {
                let array: ArrayRef = Arc::new(builder.finish());
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), vec![array]).map_err(invalid)?;
                if sender.blocking_send(Ok(batch)).is_err() {
                    return Ok(());
                }
                rows = 0;
            }
        }
        pending.extend_from_slice(data);
        if block == range.end.block {
            break;
        }
        block = next;
    }
    if !pending.is_empty() {
        builder.append_value(std::str::from_utf8(&pending).map_err(invalid)?);
        rows += 1;
    }
    if rows > 0 {
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema, vec![array]).map_err(invalid)?;
        let _ = sender.blocking_send(Ok(batch));
    }
    Ok(())
}
