//! Public-API IO/retained-owner experiment; no plan inspection or test hooks.
use arrow_array::{Array, Int32Array, ListArray};
use futures::{StreamExt, TryStreamExt};
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use std::time::Instant;
use vortex::array::arrays::{ListArray as VListArray, PrimitiveArray};
use vortex::array::validity::Validity;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::dtype::{DType, Nullability, PType};
use vortex::expr::{cast, get_item, list_length, pack, root};
use vortex::{VortexSessionDefault, session::VortexSession};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = std::env::args().nth(1) {
        let source = vail_storage::local_table::open_local(&path).await?;
        let table = StaticTable::from_metadata_file(
            source.metadata_location(),
            TableIdent::new(NamespaceIdent::new("probe".into()), "rows".into()),
            FileIO::new_with_fs(),
        )
        .await?
        .into_table();
        let tasks = table
            .scan()
            .select(["items"])
            .build()?
            .plan_files()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        for computed in [false, true] {
            let mut reader = table.reader_builder().with_data_file_concurrency_limit(16);
            if computed {
                reader = reader.with_computed_projection(pack(
                    [(
                        "n",
                        cast(
                            list_length(get_item("items", root())),
                            DType::Primitive(PType::I32, Nullability::Nullable),
                        ),
                    )],
                    Nullability::NonNullable,
                ));
            }
            let stream = reader
                .build()
                .read(futures::stream::iter(tasks.clone().into_iter().map(Ok)).boxed())?;
            let metrics = stream.metrics().clone();
            let mut stream = stream.stream();
            let mut total = 0i64;
            let mut rows = 0usize;
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows();
                if computed {
                    let values = batch
                        .column_by_name("n")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    total += values.iter().flatten().map(i64::from).sum::<i64>();
                } else {
                    let lists = batch
                        .column_by_name("items")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    total += (0..lists.len())
                        .filter(|&i| !lists.is_null(i))
                        .map(|i| i64::from(lists.value_length(i)))
                        .sum::<i64>();
                }
            }
            println!(
                "{}",
                serde_json::json!({"boundary":"public_Iceberg_ArrowReader",
                "computed":computed,"files":tasks.len(),"rows":rows,"total":total,
                "logical_requested_file_bytes":metrics.bytes_read()})
            );
        }
        return Ok(());
    }
    let session = VortexSession::default();
    for owner in [10_000usize, 1_000_000, 10_000_000] {
        let mut ctx = session.create_execution_ctx();
        let values = PrimitiveArray::from_option_iter(
            (0..owner).map(|i| (i % 13 != 0).then_some((i % 101) as i32)),
        );
        let values =
            vortex::encodings::pco::Pco::from_primitive(values.as_view(), 0, 1024, &mut ctx)?
                .into_array();
        let list = VListArray::try_new(
            values,
            PrimitiveArray::from_iter([
                (owner - 192) as u64,
                (owner - 128) as u64,
                (owner - 64) as u64,
                owner as u64,
            ])
            .into_array(),
            Validity::from_iter([true, false, true]),
        )?
        .into_array();
        let expression = list_length(root()).bind(list.dtype())?;
        let mut times = Vec::new();
        for run in 0..45 {
            let start = Instant::now();
            let result = list
                .clone()
                .apply_bound(&expression)?
                .execute::<PrimitiveArray>(&mut ctx)?;
            let elapsed = start.elapsed().as_secs_f64();
            let mask = result.validity()?.execute_mask(3, &mut ctx)?;
            for (row, expected) in [Some(64), None, Some(64)].into_iter().enumerate() {
                assert_eq!(
                    mask.value(row).then_some(result.as_slice::<u64>()[row]),
                    expected
                );
            }
            if run >= 5 {
                times.push(elapsed);
            }
        }
        times.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({"boundary":"public_Vortex_ListLength",
            "owner":owner,"nonnull_visible_elements":128,"rows":3,
            "median_query_wall":times[20]})
        );
    }
    Ok(())
}
