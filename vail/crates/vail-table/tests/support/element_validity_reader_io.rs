//! Public IO receipt. This counts real FileReadAt traffic, not reader internals.
use futures::{FutureExt, TryStreamExt, future::BoxFuture};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vortex::array::arrays::{ListArray, PrimitiveArray, StructArray};
use vortex::array::memory::MemorySessionExt;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray, VortexSessionExecute, buffer::BufferHandle};
use vortex::buffer::Alignment;
use vortex::dtype::FieldNames;
use vortex::error::VortexResult;
use vortex::expr::{list_sum, root};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::{CoalesceConfig, VortexReadAt, session::RuntimeSessionExt, std_file::FileReadAt};
use vortex::{VortexSessionDefault, session::VortexSession};

#[derive(Default)]
struct Traffic {
    requested: AtomicU64,
    completed: AtomicU64,
    ranges: Mutex<Vec<(u64, u64)>>,
}
impl Traffic {
    fn snapshot(&self) -> (u64, u64, u64) {
        let mut ranges = self.ranges.lock().unwrap().clone();
        ranges.sort_unstable();
        let (mut unique, mut end) = (0, 0);
        for (start, next) in ranges {
            unique += next.saturating_sub(end.max(start));
            end = end.max(next);
        }
        (
            self.requested.load(Ordering::Relaxed),
            self.completed.load(Ordering::Relaxed),
            unique,
        )
    }
}
struct ObservedFile {
    inner: FileReadAt,
    traffic: Arc<Traffic>,
}
impl VortexReadAt for ObservedFile {
    fn uri(&self) -> Option<&Arc<str>> {
        self.inner.uri()
    }
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.inner.coalesce_config()
    }
    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.inner.size()
    }
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        self.traffic
            .requested
            .fetch_add(length as u64, Ordering::Relaxed);
        self.traffic
            .ranges
            .lock()
            .unwrap()
            .push((offset, offset.checked_add(length as u64).unwrap()));
        let future = self.inner.read_at(offset, length, alignment);
        let traffic = self.traffic.clone();
        async move {
            let buffer = future.await?;
            traffic
                .completed
                .fetch_add(buffer.len() as u64, Ordering::Relaxed);
            Ok(buffer)
        }
        .boxed()
    }
}

fn fixture(rows: usize, width: usize) -> VortexResult<ArrayRef> {
    let len = rows * width;
    let fields = (0..8)
        .map(|column| {
            // Stable nonsequential values keep unrelated payload materially larger
            // than validity. This generation is outside the measured read window.
            let mut state = 0x9e3779b97f4a7c15u64 ^ column;
            PrimitiveArray::from_iter((0..len).map(move |_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            }))
            .into_array()
        })
        .collect::<Vec<_>>();
    let selected = StructArray::try_new(
        FieldNames::from(["a", "b", "c", "d"]),
        fields[..4].to_vec(),
        len,
        Validity::from_iter((0..len).map(|i| i % 7 != 0)),
    )?
    .into_array();
    let entry = StructArray::try_new(
        FieldNames::from(["selected", "e", "f", "g", "h"]),
        std::iter::once(selected)
            .chain(fields[4..].iter().cloned())
            .collect(),
        len,
        Validity::from_iter((0..len).map(|i| i % 5 != 0)),
    )?
    .into_array();
    Ok(ListArray::try_new(
        entry,
        PrimitiveArray::from_iter((0..=rows).map(|i| (i * width) as u64)).into_array(),
        Validity::AllValid,
    )?
    .into_array())
}

#[tokio::test]
async fn element_validity_public_file_io_receipt() -> VortexResult<()> {
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    let (rows, width) = (64usize, 1024usize);
    let input = fixture(rows, width)?;
    let session = VortexSession::default();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("presence-resource.vortex");
    session
        .write_options()
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(
                input.dtype().clone(),
                futures::stream::iter([Ok(input.clone())]),
            ),
        )
        .await?;
    let file_bytes = std::fs::metadata(&path).unwrap().len();
    let presence = list_element_validity(root(), input.dtype(), &["selected".into()])?;
    let expected = (0..rows)
        .map(|row| {
            Some(
                ((row * width)..((row + 1) * width))
                    .filter(|i| i % 5 != 0 && i % 7 != 0)
                    .count() as u64,
            )
        })
        .collect::<Vec<_>>();
    for raw in [true, false] {
        let traffic = Arc::new(Traffic::default());
        let reader = Arc::new(ObservedFile {
            inner: FileReadAt::open_with_allocator(&path, session.handle(), session.allocator())?,
            traffic: traffic.clone(),
        });
        let start = std::time::Instant::now();
        let file = session.open_options().open(reader).await?;
        let open_seconds = start.elapsed().as_secs_f64();
        let open = traffic.snapshot();
        let projection = if raw {
            root()
        } else {
            list_sum(presence.clone())
        };
        let start = std::time::Instant::now();
        let chunks = file
            .scan()?
            .with_projection(projection.bind(file.dtype())?)
            .into_array_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        let mut actual = Vec::new();
        let mut ctx = session.create_execution_ctx();
        for chunk in chunks {
            let output = if raw {
                chunk.apply_bound(&list_sum(presence.clone()).bind(input.dtype())?)?
            } else {
                chunk
            };
            for i in 0..output.len() {
                actual.push(
                    output
                        .execute_scalar(i, &mut ctx)?
                        .as_primitive()
                        .as_::<u64>(),
                );
            }
        }
        assert_eq!(actual, expected);
        let scan_seconds = start.elapsed().as_secs_f64();
        let total = traffic.snapshot();
        eprintln!(
            "ELEMENT_VALIDITY_PUBLIC_IO raw={raw} rows={rows} width={width} file_bytes={file_bytes} open_requested={} open_completed={} open_unique={} total_requested={} total_completed={} total_unique={} open_seconds={open_seconds} scan_and_value_check_seconds={scan_seconds} layout={:?}",
            open.0,
            open.1,
            open.2,
            total.0,
            total.1,
            total.2,
            file.footer().layout()
        );
    }
    Ok(())
}
