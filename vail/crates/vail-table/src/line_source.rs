//! DataFusion file source for reading files as one UTF-8 string per physical line.

use std::fmt;
use std::io;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::BytesMut;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use memchr::memchr2;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::io::StreamReader;

/// Reads a file as physical lines, preserving empty lines and line contents.
/// LF is removed as the separator; CR immediately before LF is removed too.
#[derive(Debug, Clone)]
pub struct LineSource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
}

impl Default for LineSource {
    fn default() -> Self {
        Self::new()
    }
}

impl LineSource {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        Self {
            table_schema: TableSchema::from(schema),
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(self.table_schema.file_schema())
    }
}

impl FileSource for LineSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(LineOpener {
            object_store,
            schema: self.schema(),
            batch_size: self.batch_size.unwrap_or(8192).max(1),
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut next = self.clone();
        next.batch_size = Some(batch_size.max(1));
        Arc::new(next)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "lines"
    }

    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        datafusion_physical_plan::apply_expression_roots(
            std::iter::empty::<Arc<dyn datafusion_physical_plan::PhysicalExpr>>(),
            f,
        )
    }

    fn fmt_extra(
        &self,
        _t: datafusion_physical_plan::display::DisplayFormatType,
        _f: &mut fmt::Formatter,
    ) -> fmt::Result {
        Ok(())
    }
}

struct LineOpener {
    object_store: Arc<dyn ObjectStore>,
    schema: SchemaRef,
    batch_size: usize,
}

/// Recognizes the universal line endings accepted by Hail: LF, CRLF, and CR.
/// It delays a CR at the end of a buffer until the next chunk determines
/// whether it is followed by LF.
#[derive(Default)]
struct UniversalLinesCodec {
    next_index: usize,
}

impl Decoder for UniversalLinesCodec {
    type Item = String;
    type Error = io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        let Some(offset) = memchr2(b'\r', b'\n', &buffer[self.next_index..]) else {
            self.next_index = buffer.len();
            return Ok(None);
        };
        let index = self.next_index + offset;
        let terminator = buffer[index];
        if terminator == b'\r' && index + 1 == buffer.len() {
            self.next_index = index;
            return Ok(None);
        }
        let terminator_len = if terminator == b'\r' && buffer[index + 1] == b'\n' {
            2
        } else {
            1
        };
        let mut line = buffer.split_to(index + terminator_len);
        line.truncate(index);
        self.next_index = 0;
        String::from_utf8(line.to_vec())
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn decode_eof(&mut self, buffer: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if let Some(line) = self.decode(buffer)? {
            return Ok(Some(line));
        }
        if buffer.is_empty() {
            return Ok(None);
        }
        // At EOF, a lone CR is a delimiter and should not be included in the
        // last line. A trailing line delimiter does not create another row.
        if buffer.last() == Some(&b'\r') {
            if buffer.len() == 1 {
                buffer.clear();
                self.next_index = 0;
                return Ok(Some(String::new()));
            }
            buffer.truncate(buffer.len() - 1);
        }
        if buffer.is_empty() {
            self.next_index = 0;
            return Ok(None);
        }
        let line = buffer.split().freeze();
        self.next_index = 0;
        String::from_utf8(line.to_vec())
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

impl FileOpener for LineOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "raw line scans do not support byte-range partitions".into(),
            ));
        }
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.schema);
        let batch_size = self.batch_size;
        let location = file.object_meta.location;

        Ok(Box::pin(async move {
            let result = store.get(&location).await?;
            let input = result.into_stream().map_err(DataFusionError::from).boxed();
            Ok(line_batches(input, schema, batch_size))
        }))
    }
}

fn line_batches(
    input: BoxStream<'static, Result<bytes::Bytes>>,
    schema: SchemaRef,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    let byte_reader = StreamReader::new(input.map_err(|error| io::Error::other(error.to_string())));
    let mut lines = FramedRead::new(byte_reader, UniversalLinesCodec::default());
    Box::pin(async_stream::try_stream! {
        let mut builder = StringBuilder::new();
        let mut rows = 0usize;

        while let Some(line) = lines.next().await {
            let line = line.map_err(|error| {
                DataFusionError::Execution(format!("could not decode UTF-8 line: {error}"))
            })?;
            builder.append_value(line);
            rows += 1;
            if rows == batch_size {
                let array: ArrayRef = Arc::new(builder.finish());
                yield RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
                rows = 0;
            }
        }

        // The codec emits every terminated physical line, including blanks,
        // and emits an unterminated final line at EOF without a phantom row.
        if rows > 0 {
            let array: ArrayRef = Arc::new(builder.finish());
            yield RecordBatch::try_new(schema, vec![array])?;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use futures::TryStreamExt;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    async fn read_chunks(chunks: Vec<&'static [u8]>, batch_size: usize) -> Vec<String> {
        let schema = LineSource::new().schema();
        let input = futures::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<_, DataFusionError>(bytes::Bytes::from_static(chunk))),
        )
        .boxed();
        let batches = line_batches(input, schema, batch_size)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        batches
            .into_iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn preserves_quotes_blank_lines_crlf_and_unicode_across_chunks() {
        let rows = read_chunks(vec![b"\"quoted\"\n\n", b"caf\xc3", b"\xa9\r\nlast"], 2).await;
        assert_eq!(rows, ["\"quoted\"", "", "café", "last"]);
    }

    #[tokio::test]
    async fn trailing_lf_does_not_create_an_extra_row() {
        assert_eq!(read_chunks(vec![b"one\n"], 8192).await, ["one"]);
        assert_eq!(read_chunks(vec![b"one\r"], 8192).await, ["one"]);
    }

    #[tokio::test]
    async fn splits_hail_universal_newlines_even_across_chunks() {
        assert_eq!(
            read_chunks(vec![b"a\r", b"\nb\rc\r", b"last\r"], 8192).await,
            ["a", "b", "c", "last"]
        );
        assert_eq!(read_chunks(vec![b"\r"], 8192).await, [""]);
        assert_eq!(read_chunks(vec![b"\r\r\n\n"], 8192).await, ["", "", ""]);
    }

    #[tokio::test]
    async fn opener_streams_object_store_file_and_rejects_invalid_utf8() {
        let store = Arc::new(InMemory::new());
        let good_path = Path::from("good.txt");
        let good_bytes = b"first\n\nlast";
        store
            .put(&good_path, PutPayload::from_static(good_bytes))
            .await
            .unwrap();
        let opener = LineOpener {
            object_store: store.clone(),
            schema: LineSource::new().schema(),
            batch_size: 8192,
        };
        let stream = opener
            .open(PartitionedFile::new(
                good_path.to_string(),
                good_bytes.len() as u64,
            ))
            .unwrap()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            [Some("first"), Some(""), Some("last")]
        );

        let bad_path = Path::from("bad.txt");
        store
            .put(&bad_path, PutPayload::from_static(b"valid\n\xff"))
            .await
            .unwrap();
        let error = opener
            .open(PartitionedFile::new(bad_path.to_string(), 7))
            .unwrap()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("UTF-8"));
    }
}
