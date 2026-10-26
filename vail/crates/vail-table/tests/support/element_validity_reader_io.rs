//! Public IO receipt. This counts real FileReadAt traffic, not reader internals.
//! Snapshots can overlap asynchronous prefetch: they are application-reader
//! requests/completions, not phase-isolated physical-device IO or decode counts.
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
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

/// Mirror the public edition opt-in already used by Vail's Iceberg writer;
/// enable only structural List, retaining all other edition enforcement.
pub(super) fn structural_session() -> VortexResult<VortexSession> {
    use vortex::editions::{
        Edition, EditionDeclaration, EditionId, EditionMember, EditionSessionExt,
    };
    static STRUCTURAL_LIST_EDITION: EditionDeclaration = EditionDeclaration {
        edition: Edition {
            id: EditionId::new("iceberg-structural-list", 2026, 10, 0),
            min_library_version: None,
        },
        added: &[EditionMember::layout(&"vortex.list")],
    };
    let session = VortexSession::default();
    session.register_edition(&STRUCTURAL_LIST_EDITION)?;
    session.enable_edition(STRUCTURAL_LIST_EDITION.edition.id)?;
    Ok(session)
}

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

fn layout_outline(layout: &vortex::layout::LayoutRef) -> VortexResult<Vec<String>> {
    fn visit(
        layout: &vortex::layout::LayoutRef,
        path: &str,
        out: &mut Vec<String>,
    ) -> VortexResult<()> {
        out.push(format!(
            "{path}:{} rows={} segments={:?}",
            layout.encoding_id(),
            layout.row_count(),
            layout.segment_ids()
        ));
        for slot in 0..layout.nslots() {
            if let Some(child) = layout.slot(slot)? {
                visit(&child, &format!("{path}/{slot}"), out)?;
            }
        }
        Ok(())
    }
    let mut result = Vec::new();
    visit(layout, "root", &mut result)?;
    Ok(result)
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
        std::iter::once(selected).chain(fields[4..].iter().cloned()),
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
    let session = structural_session()?;
    let directory = tempfile::tempdir().unwrap();
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
    let reduction = list_sum(presence.clone()).bind(input.dtype())?;
    let result_field = session.arrow().to_arrow_field("", reduction.dtype())?;
    for structural in [false, true] {
        let path = directory
            .path()
            .join(format!("presence-resource-{structural}.vortex"));
        let options = session.write_options();
        let options = if structural {
            options.with_strategy(
                vortex::file::WriteStrategyBuilder::default()
                    .with_list_layout()
                    .build(),
            )
        } else {
            options
        };
        options
            .write(
                tokio::fs::File::create(&path).await.unwrap(),
                ArrayStreamAdapter::new(
                    input.dtype().clone(),
                    futures::stream::iter([Ok(input.clone())]),
                ),
            )
            .await?;
        let file_bytes = std::fs::metadata(&path).unwrap().len();
        for raw in [true, false] {
            let traffic = Arc::new(Traffic::default());
            let reader = Arc::new(ObservedFile {
                inner: FileReadAt::open_with_allocator(
                    &path,
                    session.handle(),
                    session.allocator(),
                )?,
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
                    chunk.apply_bound(&reduction)?
                } else {
                    chunk
                };
                // Execute the tiny row result exactly once. Per-value scalar access
                // to a lazy sum could repeatedly execute the entire expression.
                let materialized =
                    session
                        .arrow()
                        .execute_arrow(output, Some(&result_field), &mut ctx)?;
                let materialized = materialized
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .expect("prepared UInt64 list sum result");
                actual.extend(materialized.iter());
            }
            assert_eq!(actual, expected);
            let scan_seconds = start.elapsed().as_secs_f64();
            let total = traffic.snapshot();
            eprintln!(
                "ELEMENT_VALIDITY_PUBLIC_IO structural={structural} raw={raw} rows={rows} width={width} file_bytes={file_bytes} open_observed_requested={} open_observed_completed={} open_observed_unique={} total_requested={} total_completed={} total_unique={} open_seconds={open_seconds} scan_and_value_check_seconds={scan_seconds} layout_outline={:?}",
                open.0,
                open.1,
                open.2,
                total.0,
                total.1,
                total.2,
                layout_outline(file.footer().layout())?
            );
        }
    }
    Ok(())
}

/// Small fixed workload controls. These record real reader traffic and elapsed
/// time, without byte/latency assertions or claims of independent process trials.
#[tokio::test]
async fn element_validity_tiny_many_fresh_reused_reader_receipt() -> VortexResult<()> {
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    let session = structural_session()?;
    let (rows, width) = (2usize, 3usize);
    let input = fixture(rows, width)?;
    let presence = list_element_validity(root(), input.dtype(), &["selected".into()])?;
    let reduction = list_sum(presence.clone()).bind(input.dtype())?;
    let length = vortex::expr::list_length(root()).bind(input.dtype())?;
    let raw = root().bind(input.dtype())?;
    let expected = (0..rows)
        .map(|row| {
            Some(
                ((row * width)..((row + 1) * width))
                    .filter(|i| i % 5 != 0 && i % 7 != 0)
                    .count() as u64,
            )
        })
        .collect::<Vec<_>>();
    let sum_field = session.arrow().to_arrow_field("", reduction.dtype())?;
    let length_field = session.arrow().to_arrow_field("", length.dtype())?;
    let directory = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    for i in 0..16 {
        let path = directory.path().join(format!("tiny-{i}.vortex"));
        session
            .write_options()
            .with_strategy(
                vortex::file::WriteStrategyBuilder::default()
                    .with_list_layout()
                    .build(),
            )
            .write(
                tokio::fs::File::create(&path).await.unwrap(),
                ArrayStreamAdapter::new(
                    input.dtype().clone(),
                    futures::stream::iter([Ok(input.clone())]),
                ),
            )
            .await?;
        paths.push(path);
    }
    for count in [1usize, 16] {
        for reuse in [false, true] {
            let mut cached = Vec::new();
            if reuse {
                for path in &paths[..count] {
                    let traffic = Arc::new(Traffic::default());
                    let observed = Arc::new(ObservedFile {
                        inner: FileReadAt::open_with_allocator(
                            path,
                            session.handle(),
                            session.allocator(),
                        )?,
                        traffic: traffic.clone(),
                    });
                    cached.push((
                        session.open_options().open(observed).await?.with_caching(),
                        traffic,
                    ));
                }
            }
            for window in 0..4 {
                let order = if window % 2 == 0 {
                    ["raw", "length", "sum"]
                } else {
                    ["sum", "length", "raw"]
                };
                for kind in order {
                    let before = cached
                        .iter()
                        .map(|(_, traffic)| traffic.snapshot().0)
                        .sum::<u64>();
                    let mut fresh_requested = 0;
                    let mut checksum = 0u64;
                    let start = std::time::Instant::now();
                    for i in 0..count {
                        let fresh = if reuse {
                            None
                        } else {
                            let traffic = Arc::new(Traffic::default());
                            let observed = Arc::new(ObservedFile {
                                inner: FileReadAt::open_with_allocator(
                                    &paths[i],
                                    session.handle(),
                                    session.allocator(),
                                )?,
                                traffic: traffic.clone(),
                            });
                            Some((session.open_options().open(observed).await?, traffic))
                        };
                        let file = match &fresh {
                            Some((file, _)) => file,
                            None => &cached[i].0,
                        };
                        let expression = match kind {
                            "raw" => &raw,
                            "length" => &length,
                            _ => &reduction,
                        };
                        let chunks = file
                            .scan()?
                            .with_projection(expression.clone())
                            .into_array_stream()?
                            .try_collect::<Vec<_>>()
                            .await?;
                        let mut actual = Vec::new();
                        let mut ctx = session.create_execution_ctx();
                        for chunk in chunks {
                            let output = if kind == "raw" {
                                chunk.apply_bound(&reduction)?
                            } else {
                                chunk
                            };
                            let field = if kind == "length" {
                                &length_field
                            } else {
                                &sum_field
                            };
                            let output =
                                session
                                    .arrow()
                                    .execute_arrow(output, Some(field), &mut ctx)?;
                            actual.extend(
                                output
                                    .as_any()
                                    .downcast_ref::<arrow_array::UInt64Array>()
                                    .unwrap()
                                    .iter(),
                            );
                        }
                        if kind == "length" {
                            assert_eq!(actual, vec![Some(width as u64); rows]);
                        } else {
                            assert_eq!(actual, expected);
                        }
                        checksum += actual.into_iter().flatten().sum::<u64>();
                        if let Some((_, traffic)) = &fresh {
                            fresh_requested += traffic.snapshot().0;
                        }
                    }
                    let seconds = start.elapsed().as_secs_f64();
                    let requested = if reuse {
                        cached
                            .iter()
                            .map(|(_, traffic)| traffic.snapshot().0)
                            .sum::<u64>()
                            - before
                    } else {
                        fresh_requested
                    };
                    eprintln!(
                        "ELEMENT_VALIDITY_SMALL_READER files={count} reused={reuse} window={window} query={kind} requested_bytes={requested} scan_materialize_check_seconds={seconds} checksum={checksum}"
                    );
                }
            }
        }
    }
    Ok(())
}
