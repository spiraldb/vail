//! Recognize an aligned entry update across row expansion before DF lowering.
//!
//! This proof retains the original TableIR owners. It does not lower expressions,
//! evaluate a producer, or infer source identity from names or expression hashes.
use crate::{ScalarIR, TableNode, TableRef, early_native::metadata_preparation_allowed};
use arrow_schema::{DataType, Schema, SchemaRef};
use std::{iter::once, sync::Arc};
use vail_execution::expressions::PreparationOptions;
use vail_execution::logical_expr::{
    can_block_selected_body, can_defer_selected_body, recognize_selected_entry_projection_expr,
    selected_metadata_expr,
};
use vail_execution::native_frame::NativeFrame;

/// Metadata between the row expansion and its aligned entry consumer, in
/// original execution order. Each node retains its own globals and bindings.
#[derive(Clone, Debug)]
pub(crate) enum Step {
    Project {
        table: TableRef,
        marker_in: String,
        marker_out: String,
    },
    Filter {
        table: TableRef,
        marker: String,
    },
}

/// A closed match, not a prepared evaluator. The producer still executes as
/// part of `source`, once for each original input batch, before row expansion.
#[derive(Clone, Debug)]
pub(crate) struct Proof {
    pub output: TableRef,
    pub consumer: TableRef,
    pub source: TableRef,
    pub expanded: TableRef,
    pub steps: Vec<Step>,
    pub output_field: usize,
    pub consumer_field: usize,
    pub source_entry: usize,
    pub source_list: usize,
    pub final_marker: usize,
    pub selection_schema: SchemaRef,
    pub consumer_frame: Arc<NativeFrame>,
    pub selection_frame: Arc<NativeFrame>,
}
impl Proof {
    pub fn selection_expression(&self) -> &ScalarIR {
        &self.selection_frame.expression
    }
    pub fn consumer_expression(&self) -> &ScalarIR {
        &self.consumer_frame.expression
    }
    pub fn consumer_input(&self) -> &TableRef {
        let TableNode::MapRows { child, .. } = self.consumer.node() else {
            unreachable!()
        };
        child
    }
    pub fn consumer_schema(&self) -> &Schema {
        self.consumer_frame.schema.as_ref()
    }
}

/// Match the actual selected-aligned projection, forwarding projections and
/// choice-only filters. Unknown nodes, nested explosion, effects or marker
/// consumers decline before any physical evaluator is constructed.
pub(crate) fn recognize(table: &TableRef, options: &PreparationOptions) -> Option<Proof> {
    let TableNode::MapRows {
        child: consumer,
        fields: outer_fields,
        ..
    } = table.node()
    else {
        return None;
    };
    let TableNode::MapRows {
        child: consumer_input,
        fields: inner_fields,
        ..
    } = consumer.node()
    else {
        return None;
    };
    let enabled = std::env::var_os("VAIL_TRACE_EARLY_ALIGNED_PRODUCT").is_some();
    let trace = |stage: &str| {
        if enabled {
            eprintln!(
                "VAIL_EARLY_REJECT owner={} stage={stage}",
                Arc::as_ptr(table) as usize
            );
        }
    };
    let mut matched = None;
    let mut inner_indices = HashMap::with_capacity(inner_fields.len());
    for (index, (name, _)) in inner_fields.iter().enumerate() {
        inner_indices.entry(name.as_str()).or_insert(index);
    }
    for (output_field, (_, selection)) in outer_fields.iter().enumerate() {
        let ScalarIR::ArrayMap { value, .. } = selection else {
            continue;
        };
        let ScalarIR::Column {
            name: selected_name,
        } = value.as_ref()
        else {
            continue;
        };
        let consumer_field = *inner_indices.get(selected_name.as_str())?;
        let (_, inner) = &inner_fields[consumer_field];
        let ScalarIR::AlignedMatrixMap {
            entries,
            body,
            preserve_cell: true,
            ..
        } = inner
        else {
            continue;
        };
        let ScalarIR::Column { name: marker } = entries.as_ref() else {
            trace("entries_not_column");
            continue;
        };
        if !Arc::ptr_eq(table.globals(), consumer.globals())
            || !Arc::ptr_eq(consumer.globals(), consumer_input.globals())
        {
            trace("global_owner_changed");
            return None;
        }
        if !can_block_selected_body(body) {
            trace("body_effect_policy");
            continue;
        }
        if !metadata_preparation_allowed(
            inner_fields.iter().enumerate().filter(|(i, _)| *i != consumer_field)
                .map(|(_, (_, expression))| expression),
            consumer_input.schema(), consumer,
        ) || !metadata_preparation_allowed(
            outer_fields.iter().enumerate().filter(|(i, _)| *i != output_field)
                .map(|(_, (_, expression))| expression),
            consumer.schema(), table,
        ) {
            trace("metadata_preparation_work");
            continue;
        }
        // Native programs use their exact canonical dependency slots, not the
        // surrounding Table schema. Preparing these frames compiles nothing.
        fn frame(
            expression: &ScalarIR,
            rows: &Schema,
            owner: &TableRef,
        ) -> Option<Arc<NativeFrame>> {
            let external = node_bindings(owner)
                .into_iter()
                .flat_map(|bindings| bindings.iter())
                .map(|(name, globals)| {
                    (
                        name.clone(),
                        DataType::Struct(globals.schema().fields().clone()),
                    )
                })
                .collect();
            NativeFrame::try_new(expression, rows, owner.globals().schema(), &external)
                .ok()
                .map(Arc::new)
        }
        let Some(consumer_frame) = frame(inner, consumer_input.schema(), consumer) else {
            trace("consumer_frame");
            continue;
        };
        let Some(selection_frame) = frame(selection, consumer.schema(), table) else {
            trace("selection_frame");
            continue;
        };
        if selection_frame.arguments.len() != 1
            || !matches!(&selection_frame.arguments[0], ScalarIR::Column { name } if name == selected_name)
        {
            trace("selection_argument_frame");
            continue;
        }
        let selection_schema = selection_frame.schema.clone();
        let Some(native) = recognize_selected_entry_projection_expr(
            &consumer_frame.expression,
            &consumer_frame.schema,
            &selection_frame.expression,
            &selection_schema,
            options,
        ) else {
            trace("native_projection_proof");
            continue;
        };
        if !native.supports_indexed_body() {
            trace("retained_body_not_indexable");
            continue;
        }
        let final_marker = consumer_input.schema().index_of(marker).ok()?;
        if native.output_type() != table.schema().field(output_field).data_type()
            || !matches!(consumer_frame.arguments.get(native.entry_argument()),
                Some(ScalarIR::Column { name }) if name == marker)
            || native.entry_type() != consumer_input.schema().field(final_marker).data_type()
        {
            trace("native_descriptor_or_entry_argument");
            return None;
        }
        let consumer_columns = crate::early_native::Columns::new(consumer_input.schema());
        let output_columns = crate::early_native::Columns::new(consumer.schema());
        if inner_fields.iter().enumerate().any(|(i, (_, expression))| {
            i != consumer_field
                && (!selected_metadata_expr(expression, Some(marker))
                    || !crate::early_native::can_prepare(
                        expression,
                        consumer_input.schema(),
                        consumer,
                        &consumer_columns,
                    ))
        }) || outer_fields.iter().enumerate().any(|(i, (_, expression))| {
            i != output_field
                && (!selected_metadata_expr(expression, Some(selected_name))
                    || !crate::early_native::can_prepare(
                        expression,
                        consumer.schema(),
                        table,
                        &output_columns,
                    ))
        }) {
            trace("other_metadata_field");
            continue;
        }
        if matched.is_some() {
            return None;
        }
        matched = Some((
            output_field,
            consumer_field,
            marker.clone(),
            final_marker,
            selection_schema,
            consumer_frame,
            selection_frame,
        ));
    }
    let (
        output_field,
        consumer_field,
        mut marker,
        final_marker,
        selection_schema,
        consumer_frame,
        selection_frame,
    ) = matched?;
    let mut current = consumer_input.clone();
    let mut steps = Vec::new();
    loop {
        // Every step's bindings and globals stay attached to the exact owner.
        // This also prevents a metadata-only-looking global replacement from
        // silently changing the callback's independent lexical environment.
        match current.node() {
            TableNode::MapRows { child, fields, .. } => {
                if !Arc::ptr_eq(current.globals(), child.globals()) {
                    return None;
                }
                let marker_out = marker.clone();
                let marker_field = fields.iter().position(|(name, _)| name == &marker)?;
                let ScalarIR::Column { name: marker_in } = &fields[marker_field].1 else {
                    return None;
                };
                if !metadata_preparation_allowed(
                    fields.iter().enumerate().filter(|(i, _)| *i != marker_field)
                        .map(|(_, (_, expression))| expression),
                    child.schema(), &current,
                ) {
                    trace("metadata_preparation_work");
                    return None;
                }
                let columns = crate::early_native::Columns::new(child.schema());
                if fields.iter().enumerate().any(|(i, (_, expression))| {
                    i != marker_field
                        && (!selected_metadata_expr(expression, Some(marker_in))
                            || !crate::early_native::can_prepare(
                                expression,
                                child.schema(),
                                &current,
                                &columns,
                            ))
                }) {
                    return None;
                }
                steps.push(Step::Project {
                    table: current.clone(),
                    marker_in: marker_in.clone(),
                    marker_out,
                });
                marker = marker_in.clone();
                current = child.clone();
            }
            TableNode::Filter {
                child, predicate, ..
            } => {
                if !metadata_preparation_allowed(once(predicate), child.schema(), &current) {
                    trace("metadata_preparation_work");
                    return None;
                }
                let columns = crate::early_native::Columns::new(child.schema());
                if !Arc::ptr_eq(current.globals(), child.globals())
                    || !selected_metadata_expr(predicate, Some(&marker))
                    || !crate::early_native::can_prepare(
                        predicate,
                        child.schema(),
                        &current,
                        &columns,
                    )
                {
                    trace("intermediate_filter_metadata");
                    return None;
                }
                steps.push(Step::Filter {
                    table: current.clone(),
                    marker: marker.clone(),
                });
                current = child.clone();
            }
            _ => break,
        }
    }
    let TableNode::Explode {
        child: source,
        path,
    } = current.node()
    else {
        trace("no_explode_tail");
        return None;
    };
    let [list_name] = path.as_slice() else {
        return None;
    };
    if list_name == &marker {
        return None;
    }
    let source_entry = source.schema().index_of(&marker).ok()?;
    let source_list = source.schema().index_of(list_name).ok()?;
    if !matches!(
        source.schema().field(source_list).data_type(),
        DataType::List(_)
    ) || source.schema().field(source_entry).data_type()
        != consumer_input.schema().field(final_marker).data_type()
        || !Arc::ptr_eq(current.globals(), source.globals())
    {
        return None;
    }
    // TableIR's Explode contract drops null/empty arrays and preserves encounter
    // order. Its constructor also rejects exploding a key field.
    if source.keys().iter().any(|key| key == list_name) {
        return None;
    }
    steps.reverse();
    if enabled {
        eprintln!(
            "VAIL_EARLY_PROOF owner={} steps={}",
            Arc::as_ptr(table) as usize,
            steps.len()
        );
    }
    Some(Proof {
        output: table.clone(),
        consumer: consumer.clone(),
        source: source.clone(),
        expanded: current.clone(),
        steps,
        output_field,
        consumer_field,
        source_entry,
        source_list,
        final_marker,
        selection_schema,
        consumer_frame,
        selection_frame,
    })
}

use crate::globals::{GlobalBindings, GlobalsRef};
use crate::plan::PhysicalPlan;
use async_trait::async_trait;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::expr_fn::scalar_subquery;
use datafusion_expr::lit;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    Expr as DfExpr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder,
    UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_session::Session;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_NODE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct RegionInputs {
    owner: TableRef,
    globals: Option<usize>,
    bindings: Vec<(String, usize)>,
}

/// Global operands stay local to the original expression owner. Two owners
/// may use the same reference spelling for different global relations.
#[derive(Clone, Debug)]
pub(crate) struct BoundRegion {
    pub owner: TableRef,
    pub globals: Option<Arc<dyn PhysicalExpr>>,
    pub bindings: BTreeMap<String, Arc<dyn PhysicalExpr>>,
}
impl BoundRegion {
    pub fn for_owner<'a>(regions: &'a [Self], owner: &TableRef) -> Option<&'a Self> {
        regions
            .iter()
            .find(|region| Arc::ptr_eq(&region.owner, owner))
    }
}

#[derive(Clone, Debug)]
struct EarlyAlignedProduct {
    id: u64,
    proof: Arc<Proof>,
    options: PreparationOptions,
    inputs: Vec<LogicalPlan>,
    parameters: Vec<DfExpr>,
    regions: Vec<RegionInputs>,
    schema: DFSchemaRef,
}
impl PartialEq for EarlyAlignedProduct {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for EarlyAlignedProduct {}
impl PartialOrd for EarlyAlignedProduct {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
impl Hash for EarlyAlignedProduct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl UserDefinedLogicalNodeCore for EarlyAlignedProduct {
    fn name(&self) -> &str {
        "EarlyAlignedProduct"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<DfExpr> {
        self.parameters.clone()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EarlyAlignedProduct: metadata_steps={}",
            self.proof.steps.len()
        )
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<DfExpr>,
        inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        let empty = DFSchema::empty();
        if exprs.len() != self.parameters.len()
            || exprs.iter().zip(&self.parameters).any(|(new, old)| {
                !matches!(new, DfExpr::ScalarSubquery(_) | DfExpr::Literal(_, _))
                    || new.get_type(&empty).ok() != old.get_type(&empty).ok()
            })
            || inputs.len() != self.inputs.len()
            || inputs
                .iter()
                .zip(&self.inputs)
                .any(|(new, old)| new.schema().as_arrow() != old.schema().as_arrow())
        {
            return Err(DataFusionError::Plan(
                "early aligned product private input frame changed".into(),
            ));
        }
        Ok(Self {
            inputs,
            parameters: exprs,
            ..self.clone()
        })
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }
}

fn node_bindings(table: &TableRef) -> Option<&GlobalBindings> {
    match table.node() {
        TableNode::MapRows { bindings, .. } | TableNode::Filter { bindings, .. } => Some(bindings),
        _ => None,
    }
}

/// A total row/choice predicate can cross the two maps when both preserve
/// every referenced column by identity. Entry-dependent predicates and changed
/// aliases retain their original position. Bindings stay on the filter node.
fn push_metadata_filter(table: &TableRef, options: &PreparationOptions) -> Option<Proof> {
    let TableNode::Filter {
        child: selection,
        predicate,
        keep,
        bindings,
    } = table.node()
    else {
        return None;
    };
    let mut proof = recognize(selection, options)?;
    let ScalarIR::AlignedMatrixMap { body, .. } = proof.consumer_expression() else {
        unreachable!()
    };
    if !can_defer_selected_body(body) {
        return None;
    }
    let TableNode::MapRows {
        child: consumer,
        fields: outer_fields,
        bindings: outer_bindings,
    } = selection.node()
    else {
        unreachable!()
    };
    let TableNode::MapRows {
        child: source,
        fields: inner_fields,
        bindings: inner_bindings,
    } = consumer.node()
    else {
        unreachable!()
    };
    let marker = source.schema().field(proof.final_marker).name();
    if !selected_metadata_expr(predicate, Some(marker))
        || !Arc::ptr_eq(table.globals(), source.globals())
        || table.keys() != source.keys()
    {
        return None;
    }
    let mut names = HashSet::new();
    predicate.visit_unique(&mut |expr| {
        if let ScalarIR::Column { name } = expr {
            names.insert(name.clone());
        }
    });
    for name in names {
        for fields in [outer_fields, inner_fields] {
            if !fields.iter().any(|(output, value)| {
                output == &name
                    && matches!(value, ScalarIR::Column { name: input } if input == &name)
            }) {
                return None;
            }
        }
    }
    let filtered = source
        .filter_bound(predicate.clone(), *keep, bindings.clone())
        .ok()?;
    if !metadata_preparation_allowed(once(predicate), source.schema(), &filtered) {
        return None;
    }
    let columns = crate::early_native::Columns::new(source.schema());
    if !crate::early_native::can_prepare(predicate, source.schema(), &filtered, &columns) {
        return None;
    }
    let consumer = consumer.replacing_node(TableNode::MapRows {
        child: filtered.clone(),
        fields: inner_fields.clone(),
        bindings: inner_bindings.clone(),
    });
    let output = selection.replacing_node(TableNode::MapRows {
        child: consumer.clone(),
        fields: outer_fields.clone(),
        bindings: outer_bindings.clone(),
    });
    proof.steps.push(Step::Filter {
        table: filtered,
        marker: marker.clone(),
    });
    proof.consumer = consumer;
    proof.output = output;
    Some(proof)
}

/// Lower only the certified source and exact singleton environments. The
/// expanded row projections and aligned consumer are not given to DF's normal
/// expression planner; their physical program is prepared once by the factory.
pub(crate) fn try_lower(
    table: &TableRef,
    context: &mut crate::logical::LoweringContext,
    options: &PreparationOptions,
) -> crate::Result<Option<LogicalPlan>> {
    let Some(proof) = push_metadata_filter(table, options).or_else(|| recognize(table, options))
    else {
        return Ok(None);
    };
    if proof.output.schema() != table.schema()
        || proof.output.keys() != table.keys()
        || !Arc::ptr_eq(proof.output.globals(), table.globals())
    {
        return Ok(None);
    }
    if std::env::var_os("VAIL_TRACE_EARLY_ALIGNED_PRODUCT").is_some() {
        eprintln!(
            "VAIL_EARLY_ALIGNED_PRODUCT_ADMIT steps={} source_fields={}",
            proof.steps.len(),
            proof.source.schema().fields().len()
        );
    }
    let inputs = vec![crate::logical::lower_inner(&proof.source, context)?];
    let mut parameters = Vec::new();
    let mut global_inputs = HashMap::new();
    fn global_input(
        globals: &GlobalsRef,
        fields: Option<&[String]>,
        parameters: &mut Vec<DfExpr>,
        known: &mut HashMap<(usize, Option<Vec<String>>), usize>,
        context: &mut crate::logical::LoweringContext,
    ) -> crate::Result<usize> {
        let identity = (
            Arc::as_ptr(globals) as usize,
            fields.map(<[String]>::to_vec),
        );
        if let Some(index) = known.get(&identity) {
            return Ok(*index);
        }
        let index = parameters.len();
        let bound = crate::logical_globals::lower_in_context(globals, context)?;
        let mut arguments = Vec::new();
        let mut selected_fields = Vec::new();
        let wanted = fields.map(|fields| fields.iter().map(String::as_str).collect::<HashSet<_>>());
        let original_fields = globals
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field))
            .collect::<HashMap<_, _>>();
        for column in bound.schema().columns() {
            if wanted
                .as_ref()
                .is_some_and(|fields| !fields.contains(column.name.as_str()))
            {
                continue;
            }
            selected_fields.push(
                (*original_fields.get(column.name.as_str()).ok_or_else(|| {
                    crate::TableError::Invalid("early global field changed".into())
                })?)
                .clone(),
            );
            arguments.push(lit(column.name.clone()));
            arguments.push(DfExpr::Column(column));
        }
        let value = if arguments.is_empty() {
            lit(ScalarValue::Struct(Arc::new(
                arrow_array::StructArray::new_empty_fields(1, None),
            )))
        } else {
            datafusion_functions::core::named_struct().call(arguments)
        };
        let value = vail_execution::logical_expr::restore_type(
            value,
            DataType::Struct(selected_fields.into()),
        );
        let bound = LogicalPlanBuilder::from(bound)
            .project(vec![value.alias("__vail_early_global")])?
            .build()?;
        parameters.push(scalar_subquery(Arc::new(bound)));
        known.insert(identity, index);
        Ok(index)
    }
    let mut owners = vec![proof.consumer.clone(), proof.output.clone()];
    for step in &proof.steps {
        owners.push(match step {
            Step::Project { table, .. } | Step::Filter { table, .. } => table.clone(),
        });
    }
    let mut seen = HashSet::new();
    let mut regions = Vec::new();
    for owner in owners {
        if !seen.insert(Arc::as_ptr(&owner) as usize) {
            continue;
        }
        let mut global_names = HashSet::new();
        match owner.node() {
            TableNode::MapRows { fields, .. } => {
                for (_, expression) in fields {
                    expression.visit_unique(&mut |node| {
                        if let ScalarIR::Global { name } = node {
                            global_names.insert(name.clone());
                        }
                    });
                }
            }
            TableNode::Filter { predicate, .. } => predicate.visit_unique(&mut |node| {
                if let ScalarIR::Global { name } = node {
                    global_names.insert(name.clone());
                }
            }),
            _ => unreachable!(),
        }
        let names = owner
            .globals()
            .schema()
            .fields()
            .iter()
            .filter(|field| global_names.contains(field.name()))
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let globals = if names.is_empty() {
            None
        } else {
            Some(global_input(
                owner.globals(),
                Some(&names),
                &mut parameters,
                &mut global_inputs,
                context,
            )?)
        };
        let mut bindings = Vec::new();
        if let Some(original) = node_bindings(&owner) {
            for (reference, globals) in original {
                let index =
                    global_input(globals, None, &mut parameters, &mut global_inputs, context)?;
                bindings.push((reference.clone(), index));
            }
        }
        regions.push(RegionInputs {
            owner,
            globals,
            bindings,
        });
    }
    let schema = Arc::new(DFSchema::try_from(
        crate::plan::physical_schema(table).as_ref().clone(),
    )?);
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(EarlyAlignedProduct {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            proof: Arc::new(proof),
            options: options.clone(),
            inputs,
            parameters,
            regions,
            schema,
        }),
    })))
}

/// This node owns closed scalar parameters, not row-seeded constants. Rewrites
/// that add a row seed must leave this private argument frame unchanged.
pub(crate) fn is_node(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Extension(extension) if extension.node.as_any().is::<EarlyAlignedProduct>())
}

pub(crate) struct Planner {
    pub diagnostic: bool,
}
#[async_trait]
impl ExtensionPlanner for Planner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _: &[&LogicalPlan],
        physical_inputs: &[PhysicalPlan],
        session: &dyn Session,
        planning_context: &PhysicalPlanningContext,
    ) -> DfResult<Option<PhysicalPlan>> {
        let Some(node) = node.as_any().downcast_ref::<EarlyAlignedProduct>() else {
            return Ok(None);
        };
        if physical_inputs.len() != node.inputs.len()
            || physical_inputs
                .iter()
                .zip(&node.inputs)
                .any(|(physical, logical)| {
                    physical.schema().as_ref() != logical.schema().as_arrow()
                })
        {
            return Err(DataFusionError::Plan(
                "early aligned product physical input frame changed".into(),
            ));
        }
        if self.diagnostic {
            return Ok(Some(crate::diagnostics::unbound(
                "early aligned product awaits action binding".into(),
                Arc::new(node.schema.as_arrow().clone()),
                physical_inputs.to_vec(),
            )));
        }
        let empty = DFSchema::empty();
        let parameters = node
            .parameters
            .iter()
            .map(|expression| {
                planner.create_physical_expr(expression, &empty, session, planning_context)
            })
            .collect::<DfResult<Vec<_>>>()?;
        let regions = node
            .regions
            .iter()
            .map(|region| BoundRegion {
                owner: region.owner.clone(),
                globals: region.globals.map(|index| parameters[index].clone()),
                bindings: region
                    .bindings
                    .iter()
                    .map(|(name, index)| (name.clone(), parameters[*index].clone()))
                    .collect(),
            })
            .collect::<Vec<_>>();
        let result = crate::indexed_unnest::from_early(
            physical_inputs[0].clone(),
            &node.proof,
            &node.options,
            &regions,
        )?
        .ok_or_else(|| {
            DataFusionError::Plan(
                "certified early aligned product could not prepare its program".into(),
            )
        })?;
        Ok(Some(result))
    }
}
