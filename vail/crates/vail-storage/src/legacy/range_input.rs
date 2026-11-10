//! Acquire compressed bytes before executing the synchronous Hail decoder.
use super::{Component, Error, Result, decode::PartReader, index::IndexReader, input::Input};
use std::{
    ops::Range,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::OnceCell;
use vortex::{
    buffer::{Alignment, ByteBuffer},
    io::{
        VortexReadAt, filesystem::FileSystem, session::RuntimeSessionExt, std_file::StdFileSystem,
    },
    session::VortexSession,
};

// Large rows retain the existing incremental reader rather than collecting an
// arbitrarily large compressed range in memory.
const MAX_COMPRESSED_RANGE: u64 = 8 * 1024 * 1024;

#[derive(Clone)]
struct CompressedWindow {
    start: u64,
    bytes: ByteBuffer,
}
struct OpenedSource {
    // Hail dataset parts are immutable during a scan. Pin the handle and its
    // size together; a truncated file still fails the actual positional read.
    reader: Arc<dyn VortexReadAt>,
    size: u64,
}
impl CompressedWindow {
    fn contains(&self, start: u64, end: u64) -> bool {
        start >= self.start && end <= self.start + self.bytes.len() as u64
    }
    fn slice(&self, start: u64, end: u64) -> ByteBuffer {
        self.bytes
            .slice((start - self.start) as usize..(end - self.start) as usize)
    }
}

pub(super) struct IndexedRanges {
    component: Arc<Component>,
    part: usize,
    index: Arc<Mutex<Option<IndexReader>>>,
    source: OnceCell<OpenedSource>,
    varint_frames: Option<bool>,
    window: Mutex<Option<CompressedWindow>>,
}

/// Newly required compressed frames and their row domain. Continuations retain
/// the decoder's current frame; seeks use `within` in the acquired first frame.
/// A range ending at the partition's row count includes the actual file tail.
pub(super) struct PreparedRange {
    bytes: ByteBuffer,
    within: usize,
    path: PathBuf,
    rows: Range<u64>,
    physical_start: u64,
    continuation: bool,
}

impl PreparedRange {
    pub fn reset_reader(self, reader: &mut PartReader) -> Result<()> {
        if self.continuation {
            assert_eq!(
                reader.compressed_position(self.rows.start),
                Some(self.physical_start)
            );
        }
        reader.reset_compressed(
            self.bytes,
            self.within,
            self.physical_start,
            self.rows.start,
        )
    }
    pub fn into_reader(
        self,
        component: &Component,
        part: usize,
        projection: &[usize],
    ) -> Result<PartReader> {
        assert!(
            !self.continuation,
            "continuations require the original decoder"
        );
        let input = Input::from_compressed(
            self.bytes,
            &component.buffer,
            self.within,
            self.physical_start,
        )
        .map_err(|e| -> Error {
            format!("cannot read Hail partition {}: {e}", self.path.display()).into()
        })?;
        let mut reader = PartReader::with_input(component, part, projection, input)?;
        reader.rows = self.rows.start;
        Ok(reader)
    }
}

impl IndexedRanges {
    pub fn new(component: Arc<Component>, part: usize) -> Self {
        let buffer = if component.buffer["name"] == "LEB128BufferSpec" {
            &component.buffer["child"]
        } else {
            &component.buffer
        };
        let varint_frames = if buffer["name"] == "BlockingBufferSpec" {
            let mut frame = &buffer["child"];
            while let Some(child) = frame.get("child") {
                frame = child;
            }
            match frame["name"].as_str() {
                Some("StreamBlockBufferSpec") => Some(false),
                Some("StreamBlockBufferSpec2") => Some(true),
                _ => None,
            }
        } else {
            None
        };
        Self {
            component,
            part,
            index: Arc::new(Mutex::new(None)),
            source: OnceCell::new(),
            varint_frames,
            window: Mutex::new(None),
        }
    }

    /// The caller serializes requests for this file before entering here. Only
    /// index access and raw I/O use blocking jobs; data decompression stays with
    /// the scan. Unsupported dialects and oversized ranges use the old stream.
    pub async fn prepare(
        &self,
        rows: Range<u64>,
        row_count: u64,
        session: &VortexSession,
        continuation: Option<u64>,
    ) -> Result<Option<PreparedRange>> {
        self.prepare_inner(rows, row_count, session, continuation)
            .await
            .map_err(|e| {
                format!(
                    "cannot read Hail partition {}: {e}",
                    self.component.paths[self.part].display()
                )
                .into()
            })
    }
    async fn prepare_inner(
        &self,
        rows: Range<u64>,
        row_count: u64,
        session: &VortexSession,
        continuation: Option<u64>,
    ) -> Result<Option<PreparedRange>> {
        let Some(spec) = self.component.index.clone() else {
            return Ok(None);
        };
        let Some(varint) = self.varint_frames else {
            return Ok(None);
        };
        let state = self.index.clone();
        let path = self.component.paths[self.part].clone();
        let domain = rows.clone();
        let end_row = rows.end;
        let handle = session.handle();
        let cached = if let Ok(mut state) = self.index.try_lock()
            && let Some(index) = state.as_mut()
        {
            let start = index.row_offset_if_cached(rows.start)?;
            let end = if rows.end == row_count {
                Some(None)
            } else {
                index.row_offset_if_cached(rows.end)?.map(Some)
            };
            start.zip(end)
        } else {
            None
        };
        let (start, end) = if let Some(cached) = cached {
            cached
        } else {
            handle
                .spawn_blocking(move || -> Result<_> {
                    let mut state = state.lock().map_err(|_| "Hail index cursor poisoned")?;
                    if state.is_none() {
                        *state = Some(IndexReader::open(spec, &path)?);
                    }
                    let index = state.as_mut().unwrap();
                    if index.rows != row_count {
                        return Err("Hail index row count differs from partition metadata".into());
                    }
                    let start = index.row_offset(rows.start)?;
                    let end = if rows.end == row_count {
                        None
                    } else {
                        Some(index.row_offset(rows.end)?)
                    };
                    Ok((start, end))
                })
                .await?
        };
        if end.is_some_and(|end| end < start) {
            return Err("Hail index row offsets are reversed".into());
        }
        let opened = self
            .source
            .get_or_try_init(|| async {
                let reader = StdFileSystem::new(handle.clone())
                    .open_read(
                        self.component.paths[self.part]
                            .to_str()
                            .ok_or("non-UTF8 Hail part path")?,
                    )
                    .await
                    .map_err(Error::from)?;
                let size = reader.size().await?;
                Ok::<_, Error>(OpenedSource { reader, size })
            })
            .await?;
        let source = &opened.reader;
        let size = opened.size;
        let begin = continuation.unwrap_or(start >> 16);
        let mut stop = end.map_or(size, |offset| offset >> 16);
        if (start >> 16) > stop || begin > size || stop > size {
            return Err("Hail indexed range exceeds file size".into());
        }
        if stop.saturating_sub(begin) > MAX_COMPRESSED_RANGE {
            return Ok(None);
        }
        let window = self
            .window
            .lock()
            .map_err(|_| "Hail compressed window poisoned")?
            .clone();
        if end.is_some_and(|offset| offset & 0xffff != 0) {
            let header_len = usize::try_from((size - stop).min(if varint { 5 } else { 4 }))?;
            let header = if let Some(window) = &window
                && window.contains(stop, stop + header_len as u64)
            {
                window.slice(stop, stop + header_len as u64)
            } else {
                source
                    .read_at(stop, header_len, Alignment::none())
                    .await?
                    .into_host()
                    .await
            };
            let (prefix, length) = frame_length(header.as_ref(), varint)?;
            stop = stop
                .checked_add(prefix as u64)
                .and_then(|n| n.checked_add(length as u64))
                .filter(|&n| n <= size)
                .ok_or("Hail compressed frame exceeds file size")?;
        }
        if begin > stop {
            return Err("Hail indexed range ends before the decoder position".into());
        }
        let length = stop - begin;
        if length > MAX_COMPRESSED_RANGE {
            return Ok(None);
        }
        let bytes = if let Some(window) = &window
            && window.contains(begin, stop)
        {
            window.slice(begin, stop)
        } else if length == 0 {
            ByteBuffer::empty()
        } else {
            source
                .read_at(begin, usize::try_from(length)?, Alignment::none())
                .await?
                .into_host()
                .await
        };
        if length != 0 {
            *self
                .window
                .lock()
                .map_err(|_| "Hail compressed window poisoned")? = Some(CompressedWindow {
                start: begin,
                bytes: bytes.clone(),
            });
        }
        // End-of-partition requests include the file's actual tail, never a
        // synthetic EOF at an interior range boundary.
        debug_assert!(end_row != row_count || stop == size);
        Ok(Some(PreparedRange {
            bytes,
            within: (start & 0xffff) as usize,
            path: self.component.paths[self.part].clone(),
            rows: domain,
            physical_start: begin,
            continuation: continuation.is_some(),
        }))
    }
}

fn frame_length(header: &[u8], varint: bool) -> Result<(usize, usize)> {
    let (prefix, size) = if varint {
        let mut size = 0u64;
        let mut prefix = None;
        for (i, &byte) in header.iter().enumerate() {
            size |= u64::from(byte & 127) << (i * 7);
            if byte & 128 == 0 {
                prefix = Some(i + 1);
                break;
            }
        }
        (
            prefix.ok_or("truncated Hail block varint")?,
            usize::try_from(size)?,
        )
    } else {
        (4, usize::try_from(i32::from_le_bytes(header.try_into()?))?)
    };
    if size > 256 * 1024 * 1024 {
        return Err("Hail compressed block exceeds 256 MiB".into());
    }
    Ok((prefix, size))
}
