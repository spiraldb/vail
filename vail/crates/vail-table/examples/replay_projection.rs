//! Replay an exported public projection on its real native input for profiling.
use arrow_ipc::reader::FileReader;
use std::{
    fs::File,
    path::PathBuf,
    time::{Duration, Instant},
};
use vail_table::{ScalarIR, TableIR};
#[tokio::main(worker_threads = 32)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args().nth(1).expect("evidence directory"));
    let input = std::env::args().nth(2).expect("native core table path");
    let fields: Vec<(String, ScalarIR)> =
        serde_json::from_reader(File::open(root.join("filter-fields.json"))?)?;
    let table = TableIR::read(&input)
        .await?
        .key_by(vec![])?
        .map_rows(fields)?;
    std::fs::write(
        root.join("replay-physical-plan.txt"),
        table.physical_plan()?,
    )?;
    let expected =
        FileReader::try_new(File::open(root.join("filter-core-reference.arrow"))?, None)?;
    let expected_schema = expected.schema();
    let expected = arrow_select::concat::concat_batches(
        &expected_schema,
        expected.collect::<Result<Vec<_>, _>>()?.iter(),
    )?;
    let mut actual = vec![];
    for partition in table.collect_partitions().await? {
        for batch in partition {
            let indices = expected_schema
                .fields()
                .iter()
                .map(|f| batch.schema().index_of(f.name()))
                .collect::<Result<Vec<_>, _>>()?;
            let selected = batch.project(&indices)?;
            assert_eq!(
                selected.schema(),
                expected_schema,
                "replay output metadata differs"
            );
            actual.push(selected);
        }
    }
    let actual = arrow_select::concat::concat_batches(&expected_schema, actual.iter())?;
    assert_eq!(
        actual, expected,
        "complete actual-data projection output differs"
    );
    let rows = actual.num_rows();
    let action = vail_table::partitioned::prepare_action(&table)?;
    let plan = action.start_action().await?;
    let measured = datafusion_physical_plan::execution_plan::collect_partitioned(
        plan.clone(),
        std::sync::Arc::new(datafusion_execution::TaskContext::default()),
    )
    .await?;
    let measured = measured
        .iter()
        .flatten()
        .map(|batch| {
            let indices = expected_schema
                .fields()
                .iter()
                .map(|field| batch.schema().index_of(field.name()))
                .collect::<Result<Vec<_>, _>>()?;
            let selected = batch.project(&indices)?;
            assert_eq!(selected.schema(), expected_schema);
            Ok::<_, arrow_schema::ArrowError>(selected)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let measured = arrow_select::concat::concat_batches(&expected_schema, measured.iter())?;
    if measured != expected {
        let differing = measured
            .columns()
            .iter()
            .zip(expected.columns())
            .enumerate()
            .filter_map(|(i, (a, b))| (a != b).then_some(expected_schema.field(i).name()))
            .collect::<Vec<_>>();
        return Err(format!("operator-profile output differs in columns {differing:?}").into());
    }
    std::fs::write(
        root.join("replay-operator-metrics.txt"),
        format!(
            "{}",
            datafusion_physical_plan::display::DisplayableExecutionPlan::with_full_metrics(
                plan.as_ref()
            )
            .indent(true)
        ),
    )?;
    drop(actual);
    drop(expected);
    assert_eq!(table.force_count().await?, rows);
    std::fs::write(root.join("replay-ready"), "ready")?;
    let start = Instant::now();
    let mut actions = 0;
    while start.elapsed() < Duration::from_secs(5) {
        assert_eq!(table.force_count().await?, rows);
        actions += 1;
    }
    std::fs::write(
        root.join("replay-result.json"),
        serde_json::to_vec(
            &serde_json::json!({"validated":true,"rows":rows,"actions":actions,"scope":"exported real projection execution; excludes Python query construction"}),
        )?,
    )?;
    Ok(())
}
