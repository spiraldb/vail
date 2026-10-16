//! File pruning is a data-access contract, independent of execution partitions.
//!
//! The supported native storage boundary is TableIR::write_with_options plus
//! LocalRead::descriptor and FragmentIndex's certified typed bounds. This test
//! uses the published file identities, not private execution-plan structure.
use arrow_array::Int32Array;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_storage::{
    fragment_index::FragmentIndex,
    local_table::{WriteOptions, open_local},
};
use vail_table::{ScalarIR, TableIR, TableRef};

async fn values(table: &TableRef) -> vail_table::Result<Vec<i32>> {
    Ok(table
        .collect_partitions()
        .await?
        .iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect())
}

#[tokio::test]
async fn sorted_multifile_output_prunes_impossible_files_but_requires_matching_files() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("table");
        let path = path.to_str().unwrap();
        TableIR::range(12, 3)
            .unwrap()
            .write_with_options(
                path,
                false,
                WriteOptions {
                    max_buffer_bytes: usize::MAX,
                    max_rows_per_file: 2,
                },
            )
            .await
            .unwrap();
        let source = open_local(path).await.unwrap();
        let descriptor = source.descriptor();
        assert_eq!(descriptor.files.len(), 6);
        assert!(descriptor.files.iter().all(|file| file.row_count == 2));
        let local = |path: &str| path.strip_prefix("file://").unwrap_or(path).to_owned();
        let index: FragmentIndex = serde_json::from_slice(
            &std::fs::read(local(descriptor.index_path.as_ref().unwrap())).unwrap(),
        )
        .unwrap();
        let bounds = index
            .typed_fragment_bounds()
            .iter()
            .map(|(first, last)| {
                let key = |value: &vail_storage::fragment_index::TypedKey| {
                    value.components[0]
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0)
                };
                (key(first), key(last))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            bounds,
            (0..6).map(|i| (2 * i, 2 * i + 1)).collect::<Vec<_>>()
        );
        assert!(bounds.windows(2).all(|pair| pair[0].1 < pair[1].0));
        let read = TableIR::read(path).await.unwrap();
        assert_eq!(values(&read).await.unwrap(), (0..12).collect::<Vec<_>>());
        let selected = read
            .filter(
                ScalarIR::Binary {
                    operator: BinaryOperator::Lt,
                    left: Arc::new(ScalarIR::Column { name: "idx".into() }),
                    right: Arc::new(ScalarIR::Literal {
                        dtype: Type::Primitive(PrimitiveType::Int32),
                        value: json!(2),
                    }),
                },
                true,
            )
            .unwrap();
        let fragments = index.fragments();
        for fragment in &fragments[1..] {
            std::fs::remove_file(local(&fragment.identity.file_path)).unwrap();
        }
        assert_eq!(values(&selected).await.unwrap(), vec![0, 1]);
        assert_eq!(values(&selected).await.unwrap(), vec![0, 1]);
        std::fs::remove_file(local(&fragments[0].identity.file_path)).unwrap();
        // Public native diagnostics must not attempt to execute a missing source.
        assert!(!selected.physical_plan().unwrap().is_empty());
        assert!(
            values(&selected).await.is_err(),
            "matching file must actually be read"
        );
    })
    .await
    .expect("file pruning action exceeded deadline");
}
