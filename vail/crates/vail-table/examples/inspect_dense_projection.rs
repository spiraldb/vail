//! Read-only physical-capability diagnostic; never used by tests or execution.
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use vortex::array::serde::SerializedArray;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::LayoutRef;
use vortex::layout::layouts::flat::Flat;
use vortex::{VortexSessionDefault, session::VortexSession};
fn inspect<'a>(
    layout: LayoutRef,
    segments: &'a [vortex::file::SegmentSpec],
    file: &'a mut tokio::fs::File,
    depth: usize,
) -> futures::future::BoxFuture<'a, Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    Box::pin(async move {
        eprintln!(
            "{}{} rows={} dtype={}",
            " ".repeat(depth),
            layout.encoding_id(),
            layout.row_count(),
            layout.dtype()
        );
        if let Some(flat) = layout.as_opt::<Flat>() {
            let segment = &segments[*flat.segment_id() as usize];
            let bytes = if let Some(tree) = flat.array_tree() {
                tree.clone()
            } else {
                file.seek(std::io::SeekFrom::Start(
                    segment.offset + u64::from(segment.length) - 4,
                ))
                .await?;
                let mut suffix = [0; 4];
                file.read_exact(&mut suffix).await?;
                let length = u32::from_le_bytes(suffix) as usize;
                if length > 64 * 1024 || length + 4 > segment.length as usize {
                    return Err("metadata length outside diagnostic bound".into());
                }
                file.seek(std::io::SeekFrom::Start(
                    segment.offset + u64::from(segment.length) - 4 - length as u64,
                ))
                .await?;
                let mut bytes = vec![0; length];
                file.read_exact(&mut bytes).await?;
                bytes.into()
            };
            let tree = SerializedArray::from_array_tree(bytes)?;
            fn arrays(
                tree: &SerializedArray,
                ctx: &vortex::session::registry::ReadContext,
                depth: usize,
            ) {
                eprintln!(
                    "{}array {:?} metadata={:?} buffers={:?}",
                    " ".repeat(depth),
                    ctx.resolve(tree.encoding_id()),
                    tree.metadata(),
                    tree.metadata_buffer_lengths()
                );
                for i in 0..tree.nchildren() {
                    arrays(&tree.child(i), ctx, depth + 1);
                }
            }
            arrays(&tree, flat.array_ctx(), depth + 1);
        }
        for child in layout.children()? {
            inspect(child, segments, file, depth + 1).await?;
        }
        Ok(())
    })
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::args().nth(1).expect("native table path");
    let source = vail_storage::local_table::open_local(&path).await?;
    let path = PathBuf::from(
        source.descriptor().files[0]
            .path
            .trim_start_matches("file://"),
    );
    let session = VortexSession::default();
    let opened = session.open_options().open_path(&path).await?;
    let mut file = tokio::fs::File::open(path).await?;
    inspect(
        opened.footer().layout().clone(),
        opened.footer().segment_map(),
        &mut file,
        0,
    )
    .await?;
    Ok(())
}
