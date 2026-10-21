//! Lazy BGEN variant-key selection. A declared selector input is consumed once
//! per action in native execution; all indexed file partitions share its keys.
//! No execution operator plans queries or localizes keys through Python.
use crate::{Result, TableError, TableNode, TableRef, plan::PhysicalPlan};
use arrow_array::{
    Array, Int32Array, LargeStringArray, ListArray, StringArray, StringViewArray, StructArray,
};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::OnceCell;

fn bad(value: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(value.into())
}
fn down<T: Array + 'static>(a: &dyn Array) -> datafusion_common::Result<&T> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| bad("BGEN selector Arrow storage disagrees with key type"))
}
fn text(a: &dyn Array, row: usize) -> datafusion_common::Result<&str> {
    if a.is_null(row) {
        return Err(bad("BGEN selector key strings cannot be missing"));
    }
    if let Some(a) = a.as_any().downcast_ref::<StringArray>() {
        Ok(a.value(row))
    } else if let Some(a) = a.as_any().downcast_ref::<LargeStringArray>() {
        Ok(a.value(row))
    } else if let Some(a) = a.as_any().downcast_ref::<StringViewArray>() {
        Ok(a.value(row))
    } else {
        Err(bad("BGEN selector key must contain strings"))
    }
}
#[derive(Debug, Default)]
struct Keys {
    loci: HashMap<String, HashMap<i32, HashSet<Vec<String>>>>,
}
#[derive(Debug)]
pub(crate) struct Selection {
    alleles: bool,
    keys: OnceLock<Keys>,
    ready: OnceCell<()>,
}
impl Selection {
    fn new(alleles: bool) -> Self {
        Self {
            alleles,
            keys: OnceLock::new(),
            ready: OnceCell::new(),
        }
    }
    pub(crate) fn matches(
        &self,
        contig: &str,
        position: u32,
        alleles: &[String],
    ) -> std::result::Result<bool, String> {
        let position = i32::try_from(position)
            .map_err(|_| "BGEN position exceeds signed 32-bit locus range")?;
        let keys = self
            .keys
            .get()
            .ok_or("BGEN selector must execute before source partitions")?;
        Ok(keys
            .loci
            .get(contig)
            .and_then(|positions| positions.get(&position))
            .is_some_and(|keys| !self.alleles || keys.contains(alleles)))
    }
    async fn initialize(
        &self,
        selector: PhysicalPlan,
        key_names: &[String],
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<()> {
        self.ready
            .get_or_try_init(|| async {
                let schema = selector.schema();
                let locus = schema.index_of(&key_names[0])?;
                let allele = if self.alleles {
                    Some(schema.index_of(&key_names[1])?)
                } else {
                    None
                };
                let mut keys = Keys::default();
                for partition in 0..selector.output_partitioning().partition_count() {
                    let mut stream = selector.execute(partition, context.clone())?;
                    while let Some(batch) = stream.try_next().await? {
                        let loci: &StructArray = down(batch.column(locus).as_ref())?;
                        let tagged = loci.column_by_name("__vail_locus_contig").is_some();
                        let contigs = loci
                            .column_by_name(if tagged {
                                "__vail_locus_contig"
                            } else {
                                "contig"
                            })
                            .ok_or_else(|| bad("BGEN selector locus lacks contig"))?;
                        let positions: &Int32Array = down(
                            loci.column_by_name(if tagged {
                                "__vail_locus_position"
                            } else {
                                "position"
                            })
                            .ok_or_else(|| bad("BGEN selector locus lacks position"))?
                            .as_ref(),
                        )?;
                        let alleles = allele
                            .map(|i| down::<ListArray>(batch.column(i).as_ref()))
                            .transpose()?;
                        for row in 0..batch.num_rows() {
                            if loci.is_null(row) || contigs.is_null(row) || positions.is_null(row) {
                                continue;
                            }
                            let contig = text(contigs.as_ref(), row)?;
                            let position = positions.value(row);
                            let values = if let Some(alleles) = alleles {
                                if alleles.is_null(row) {
                                    continue;
                                }
                                let start = alleles.value_offsets()[row] as usize;
                                let end = alleles.value_offsets()[row + 1] as usize;
                                (start..end)
                                    .map(|i| text(alleles.values().as_ref(), i).map(str::to_owned))
                                    .collect::<datafusion_common::Result<Vec<_>>>()?
                            } else {
                                Vec::new()
                            };
                            keys.loci
                                .entry(contig.to_owned())
                                .or_default()
                                .entry(position)
                                .or_default()
                                .insert(values);
                        }
                    }
                }
                self.keys
                    .set(keys)
                    .map_err(|_| bad("BGEN selector initialized twice"))?;
                Ok::<(), DataFusionError>(())
            })
            .await?;
        Ok(())
    }
}
static NEXT: AtomicU64 = AtomicU64::new(1);
#[derive(Clone, Debug)]
pub(crate) struct SelectNode {
    id: u64,
    source: TableRef,
    variants: TableRef,
    inputs: Vec<LogicalPlan>,
}
impl PartialEq for SelectNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.inputs == other.inputs
    }
}
impl Eq for SelectNode {}
impl PartialOrd for SelectNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
impl Hash for SelectNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.inputs.hash(state);
    }
}
impl UserDefinedLogicalNodeCore for SelectNode {
    fn name(&self) -> &str {
        "VailBgenVariantSelection"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        self.inputs[0].schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailBgenVariantSelection")
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !expressions.is_empty() || inputs.len() != 2 {
            return Err(bad("invalid BGEN selection rewrite"));
        }
        Ok(Self {
            inputs,
            ..self.clone()
        })
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }
}
pub(crate) fn logical(
    source: TableRef,
    variants: TableRef,
    inputs: Vec<LogicalPlan>,
) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(SelectNode {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            source,
            variants,
            inputs,
        }),
    })
}
impl SelectNode {
    pub(crate) fn physical(
        &self,
        inputs: &[PhysicalPlan],
    ) -> datafusion_common::Result<PhysicalPlan> {
        if inputs.len() != 2 {
            return Err(bad("BGEN selection requires source and selector inputs"));
        }
        Ok(Arc::new(SelectExec::new(
            self.source.clone(),
            self.variants.keys().to_vec(),
            inputs[1].clone(),
        )?))
    }
}
fn source_plan(
    source: &TableRef,
    selection: Arc<Selection>,
) -> datafusion_common::Result<PhysicalPlan> {
    let TableNode::TextRead { paths, .. } = source.node() else {
        return Err(bad("BGEN selection source must be an indexed file source"));
    };
    let parts = (0..paths.len())
        .map(|part| crate::plan::bgen_source_partition(source, part, selection.clone()))
        .collect::<Result<Vec<_>>>()
        .map_err(|e| bad(e.to_string()))?;
    let schema = parts
        .first()
        .ok_or_else(|| bad("BGEN source must have a partition"))?
        .schema();
    Ok(crate::partitioned::PreparedTable {
        counts: vec![None; parts.len()],
        parts,
        schema,
    }
    .physical_plan())
}
#[derive(Debug)]
struct SelectExec {
    source_table: TableRef,
    key_names: Vec<String>,
    source: PhysicalPlan,
    selector: PhysicalPlan,
    selection: Arc<Selection>,
    cache: Arc<PlanProperties>,
}
impl SelectExec {
    fn new(
        source_table: TableRef,
        key_names: Vec<String>,
        selector: PhysicalPlan,
    ) -> datafusion_common::Result<Self> {
        let selection = Arc::new(Selection::new(key_names.len() == 2));
        let source = source_plan(&source_table, selection.clone())?;
        let cache = source.properties().clone();
        Ok(Self {
            source_table,
            key_names,
            source,
            selector,
            selection,
            cache,
        })
    }
}
impl DisplayAs for SelectExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailBgenVariantSelectionExec")
    }
}
impl ExecutionPlan for SelectExec {
    fn name(&self) -> &str {
        "VailBgenVariantSelectionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        ) -> datafusion_common::Result<
            datafusion_common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        datafusion_physical_plan::apply_expression_roots(
            std::iter::empty::<Arc<dyn datafusion_physical_plan::PhysicalExpr>>(),
            f,
        )
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.source, &self.selector]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 2 {
            return Err(bad("BGEN selection requires two children"));
        }
        Ok(Arc::new(Self {
            source: children[0].clone(),
            selector: children[1].clone(),
            cache: children[0].properties().clone(),
            source_table: self.source_table.clone(),
            key_names: self.key_names.clone(),
            selection: self.selection.clone(),
        }))
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        // Fresh action, fresh key set, and fresh BGEN source bindings. Selector
        // dependencies cannot be cached across actions or lost with globals.
        Ok(Arc::new(Self::new(
            self.source_table.clone(),
            self.key_names.clone(),
            self.selector.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let selection = self.selection.clone();
        let selector = self.selector.clone();
        let source = self.source.clone();
        let names = self.key_names.clone();
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                selection.initialize(selector,&names,context.clone()).await?;
                let mut stream=source.execute(partition,context)?;
                while let Some(batch)=stream.try_next().await?{yield batch;}
            },
        )))
    }
}

pub(crate) fn validate(source: &TableRef, variants: &TableRef) -> Result<()> {
    if !matches!(source.node(),TableNode::TextRead{format,..}if format=="bgen") {
        return Err(TableError::Invalid(
            "BGEN selector requires an indexed BGEN source".into(),
        ));
    }
    if !(1..=2).contains(&variants.keys().len()) {
        return Err(TableError::Invalid(
            "BGEN variants table must be keyed by locus or locus and alleles".into(),
        ));
    }
    let row = source
        .schema()
        .field_with_name("__vail_matrix_row")
        .map_err(|e| TableError::Invalid(e.to_string()))?;
    let arrow_schema::DataType::Struct(fields) = row.data_type() else {
        return Err(TableError::Invalid("BGEN row schema must be struct".into()));
    };
    for (index, name) in ["locus", "alleles"]
        .iter()
        .enumerate()
        .take(variants.keys().len())
    {
        let source_type = fields
            .iter()
            .find(|f| f.name() == name)
            .ok_or_else(|| TableError::Invalid("BGEN source lacks variant keys".into()))?
            .data_type();
        let selected_type = variants
            .schema()
            .field_with_name(&variants.keys()[index])
            .map_err(|e| TableError::Invalid(e.to_string()))?
            .data_type();
        if source_type != selected_type {
            return Err(TableError::Invalid(
                "BGEN variant selector key types must match the indexed reference and alleles"
                    .into(),
            ));
        }
    }
    Ok(())
}
