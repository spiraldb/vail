//! Bounded local gzip/BGZF output for Arrow's streaming CSV writer.
use std::io::{self, Write};

/// At most 65,280 uncompressed bytes per BGZF member. Even stored DEFLATE
/// blocks then fit inside BGZF's 65,536-byte compressed block limit.
const BLOCK_SIZE: usize = 65_280;

pub(crate) struct BgzfWriter<W: Write> {
    output: W,
    pending: Vec<u8>,
}

impl<W: Write> BgzfWriter<W> {
    pub(crate) fn new(output: W) -> Self {
        Self { output, pending: Vec::with_capacity(BLOCK_SIZE) }
    }
    fn block(&mut self, data: &[u8]) -> io::Result<()> {
        let mut encoder =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data)?;
        let payload = encoder.finish()?;
        let block_size = payload.len() + 26;
        let size = u16::try_from(block_size - 1)
            .map_err(|_| io::Error::other("BGZF compressed member exceeds 65536 bytes"))?;
        self.output
            .write_all(b"\x1f\x8b\x08\x04\0\0\0\0\0\xff\x06\0BC\x02\0")?;
        self.output.write_all(&size.to_le_bytes())?;
        self.output.write_all(&payload)?;
        let mut crc = flate2::Crc::new();
        crc.update(data);
        self.output.write_all(&crc.sum().to_le_bytes())?;
        self.output.write_all(&(data.len() as u32).to_le_bytes())
    }

    fn drain(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            let data = std::mem::take(&mut self.pending);
            self.block(&data)?;
            self.pending = data;
            self.pending.clear();
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> io::Result<()> {
        self.drain()?;
        self.block(&[])?; // Standard BGZF end-of-file marker.
        self.output.flush()
    }
}

impl<W: Write> Write for BgzfWriter<W> {
    fn write(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let total = data.len();
        while !data.is_empty() {
            let count = (BLOCK_SIZE - self.pending.len()).min(data.len());
            self.pending.extend_from_slice(&data[..count]);
            data = &data[count..];
            if self.pending.len() == BLOCK_SIZE {
                self.drain()?;
            }
        }
        Ok(total)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.drain()?;
        self.output.flush()
    }
}

enum CompressedWriter {
    Gzip(flate2::write::GzEncoder<std::io::BufWriter<std::fs::File>>),
    Bgzf(BgzfWriter<std::io::BufWriter<std::fs::File>>),
}

impl CompressedWriter {
    fn finish(self) -> io::Result<()> {
        match self {
            Self::Gzip(writer) => writer.finish()?.flush(),
            Self::Bgzf(writer) => writer.finish(),
        }
    }
}

impl Write for CompressedWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match self {
            Self::Gzip(writer) => writer.write(data),
            Self::Bgzf(writer) => writer.write(data),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Gzip(writer) => writer.flush(),
            Self::Bgzf(writer) => writer.flush(),
        }
    }
}

/// The async producer retains at most two queued Arrow batches; the blocking
/// worker owns serialization/compression and is joined on every completion path.
pub(crate) async fn export_csv(
    plan: std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    path: &str,
    builder: arrow_csv::WriterBuilder,
    context: std::sync::Arc<datafusion_execution::TaskContext>,
) -> crate::Result<()> {
    use futures::TryStreamExt;
    let path = path.to_owned();
    let schema = plan.schema();
    let mut stream = plan.execute(0, context)?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<arrow_array::RecordBatch>(2);
    let worker = tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
        let file =
            std::io::BufWriter::new(std::fs::File::create(&path).map_err(|e| e.to_string())?);
        let output = if path.ends_with(".bgz") {
            CompressedWriter::Bgzf(BgzfWriter {
                output: file,
                pending: Vec::with_capacity(BLOCK_SIZE),
            })
        } else {
            CompressedWriter::Gzip(flate2::write::GzEncoder::new(
                file,
                flate2::Compression::default(),
            ))
        };
        let mut writer = builder.build(output);
        let mut wrote_batch = false;
        while let Some(batch) = receiver.blocking_recv() {
            writer.write(&batch).map_err(|e| e.to_string())?;
            wrote_batch = true;
        }
        if !wrote_batch {
            writer
                .write(&arrow_array::RecordBatch::new_empty(schema))
                .map_err(|e| e.to_string())?;
        }
        writer.into_inner().finish().map_err(|e| e.to_string())
    });
    let mut scan_error = None;
    loop {
        match stream.try_next().await {
            Ok(Some(batch)) => {
                if sender.send(batch).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => {
                scan_error = Some(error);
                break;
            }
        }
    }
    drop(sender);
    let output = worker
        .await
        .map_err(|e| crate::TableError::Invalid(e.to_string()))?;
    if let Some(error) = scan_error {
        return Err(error.into());
    }
    output.map_err(crate::TableError::Invalid)
}
