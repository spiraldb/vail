//! Lower Vail scalar IR into DataFusion's optimizable logical expression tree.
//!
//! Ordinary scalar operations remain visible as columns, literals, casts,
//! binary expressions, CASE expressions, and nested field access. This is the
//! expression boundary used by Vail logical plans; physical-only kernels stay
//! behind explicit errors until they have a logical UDF contract.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, RecordBatchOptions, StructArray, make_array, new_empty_array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Schema};
use datafusion_physical_expr::PhysicalExpr;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use datafusion_common::{DFSchema, DataFusionError, ScalarValue, Result as DfResult};
use datafusion_expr::{ColumnarValue, Expr as LogicalExpr, ExprSchemable, Operator, Signature, Volatility, lit};

use crate::expressions::{BinaryOperator, Expr, ExprError, PrimitiveType, RebuiltExpr, Type, UnaryOperator};

/// Prepared scalar bindings retain their Arrow owners. Pointer equality is an
/// execution-cache shortcut only (not expression identity): immutable nested
/// buffers cannot change or have their addresses recycled while retained here.
fn same_scalar_binding(left: &ScalarValue, right: &ScalarValue) -> bool {
    match (left, right) {
        (ScalarValue::Struct(a), ScalarValue::Struct(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::List(a), ScalarValue::List(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::LargeList(a), ScalarValue::LargeList(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::FixedSizeList(a), ScalarValue::FixedSizeList(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::ListView(a), ScalarValue::ListView(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::LargeListView(a), ScalarValue::LargeListView(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::Map(a), ScalarValue::Map(b)) => Arc::ptr_eq(a, b),
        (ScalarValue::Dictionary(at, a), ScalarValue::Dictionary(bt, b)) => at == bt && same_scalar_binding(a, b),
        _ => left == right,
    }
}

/// Whether a volatile semantic kernel is closed and deterministic apart from
/// value-domain errors, so a successful projection result may be reused.
///
/// This intentionally recognizes the exact typed kernel implementation and
/// never relies on the display name or digest. The original UDF volatility is
/// unchanged: the projection cache is lazy and stores only successful results.
pub fn is_cacheable_closed_semantic_kernel(function: &datafusion_expr::ScalarUDF) -> bool {
    use crate::{
        expression_effects::{Effects, effects},
        expression_identity::Identity,
        kernel_identity::Scalar,
    };
    use datafusion_expr::{TypeSignature, Volatility};

    if function.signature().volatility != Volatility::Volatile
        || function.signature().type_signature != TypeSignature::Nullary
    {
        return false;
    }
    let Some(kernel) = function
        .inner()
        .downcast_ref::<Scalar<(Identity, Arc<Schema>)>>()
    else {
        return false;
    };
    let (identity, schema) = kernel.key();
    schema.fields().is_empty()
        && identity.expression().is_closed_scalar()
        && !contains_global_reference(identity.expression())
        && effects(identity.expression()) != Effects::RandomContext
}

/// Inspect only Vail's exact typed semantic kernel identity. Consumers must
/// still prove any optimization against the returned expression's semantics.
pub fn semantic_kernel(function: &datafusion_expr::ScalarUDF) -> Option<(&Expr, &Schema)> {
    let kernel = function.inner().downcast_ref::<crate::kernel_identity::Scalar<(
        crate::expression_identity::Identity, Arc<Schema>
    )>>()?;
    let (identity, schema) = kernel.key();
    Some((identity.expression(), schema.as_ref()))
}

fn semantic_preparation_options(function: &datafusion_expr::ScalarUDF) -> Option<&crate::expressions::PreparationOptions> {
    function.inner().downcast_ref::<crate::kernel_identity::Scalar<(
        crate::expression_identity::Identity, Arc<Schema>
    )>>()?.preparation_options()
}

/// Splitting a deterministic semantic program retains value/error semantics;
/// random programs require their original shared evaluation context.
pub fn semantic_kernel_can_split(function: &datafusion_expr::ScalarUDF) -> bool {
    semantic_kernel(function).is_some_and(|(expression, _)| {
        crate::expression_effects::effects(expression) != crate::expression_effects::Effects::RandomContext
    })
}

/// An aligned producer preserves every row's entry-list shape. Only pure
/// computations and known input-validation errors can move after filtering.
pub fn deferrable_aligned_entry_argument(function: &datafusion_expr::ScalarUDF) -> Option<usize> {
    let (expression, schema) = semantic_kernel(function)?;
    let Expr::AlignedMatrixMap { entries, preserve_cell: true, .. } = expression else { return None; };
    if !crate::expression_effects::can_defer_filtered_values(expression) { return None; }
    let Expr::Column { name } = entries.as_ref() else { return None; };
    schema.index_of(name).ok()
}

/// Invocation-local owners and parent lanes for a prepared aligned kernel.
/// These values are never stored in the scalar-specialization cache.
#[derive(Debug, Clone)]
pub struct AlignedSelection {
    pub entries: ArrayRef,
    pub parents: Int32Array,
}
/// A capture root uses at most one existing argument owner. DataFusion evaluates
/// scalars against one row and arrays against their own row domain.
#[derive(Clone, Debug)]
pub(crate) struct PreparedCapture {
    argument: Option<usize>,
    schema: Arc<Schema>,
    program: Arc<dyn PhysicalExpr>,
}
impl PreparedCapture {
    pub(crate) fn prepare(root: &Expr, schema: &Schema, options: &crate::expressions::PreparationOptions) -> Option<Self> {
        if !root.is_pure() {
            return None;
        }
        let mut names = HashSet::new();
        let mut supported = true;
        root.visit_unique(&mut |node| match node {
            Expr::Column { name } => {
                names.insert(name.clone());
            }
            Expr::Struct { .. }
            | Expr::GetField { .. }
            | Expr::Literal { .. }
            | Expr::RuntimeLiteral { .. } => {}
            _ => supported = false,
        });
        if !supported || names.len() > 1 {
            return None;
        }
        let argument = names
            .iter()
            .next()
            .map(|name| schema.index_of(name))
            .transpose()
            .ok()?;
        let fields = argument
            .map(|index| vec![schema.fields()[index].clone()])
            .unwrap_or_default();
        let schema = Arc::new(Schema::new(fields));
        let program = crate::expressions::lower_with_options(root, &schema, options).ok()?;
        if std::env::var_os("VAIL_TRACE_FUSED_UNNEST").is_some() {
            eprintln!("VAIL_FUSED_UNNEST_CAPTURE_PREP argument={argument:?}");
        }
        Some(Self {
            argument,
            schema,
            program,
        })
    }
    pub(crate) fn evaluate(&self, arguments: &[ColumnarValue]) -> DfResult<ColumnarValue> {
        let (columns, rows, scalar) = match self.argument {
            Some(index) => match arguments.get(index).ok_or_else(|| {
                DataFusionError::Internal("selected capture argument absent".into())
            })? {
                ColumnarValue::Scalar(value) => (
                    vec![restore_arrow_array_type(
                        &value.to_array()?,
                        self.schema.field(0).data_type(),
                    )?],
                    1,
                    true,
                ),
                ColumnarValue::Array(value) => (
                    vec![restore_arrow_array_type(
                        value,
                        self.schema.field(0).data_type(),
                    )?],
                    value.len(),
                    false,
                ),
            },
            None => (vec![], 1, true),
        };
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let value = self.program.evaluate(&batch)?;
        if scalar {
            let array = value.into_array(1)?;
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&array, 0)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            ))
        } else {
            Ok(value)
        }
    }
}

/// A selected-capable aligned kernel followed by a pure field-selection view.
/// Capture roots and the indexed body are prepared once for the query.
#[derive(Clone)]
pub struct SelectedAlignedProgram {
    pub entry_argument: usize,
    entry_type: DataType,
    program: Arc<dyn PhysicalExpr>,
    pub(crate) captures: [PreparedCapture; 3],
    projection: Option<crate::PreparedArrayProjection>,
    final_type: DataType,
}
impl Debug for SelectedAlignedProgram {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("SelectedAlignedProgram")
            .field("entry_argument", &self.entry_argument)
            .finish()
    }
}
impl SelectedAlignedProgram {
    pub fn entry_type(&self) -> &DataType {
        &self.entry_type
    }
    pub fn output_type(&self) -> &DataType {
        &self.final_type
    }
    pub fn invoke(
        &self,
        arguments: &[ColumnarValue],
        selection: AlignedSelection,
    ) -> DfResult<ColumnarValue> {
        if selection.entries.data_type() != self.entry_type() {
            return Err(DataFusionError::Execution(
                "selected entry owner schema differs".into(),
            ));
        }
        let rows = selection.parents.len();
        if rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.final_type)));
        }
        let captures = self.evaluate_captures(arguments)?;
        self.invoke_captures(selection, captures, None)
    }
    pub(crate) fn evaluate_captures(&self, arguments: &[ColumnarValue]) -> DfResult<[ColumnarValue; 3]> {
        Ok([self.captures[0].evaluate(arguments)?, self.captures[1].evaluate(arguments)?, self.captures[2].evaluate(arguments)?])
    }
    pub(crate) fn invoke_captures(&self, selection: AlignedSelection, captures: [ColumnarValue; 3], bundle: Option<&ArrayRef>) -> DfResult<ColumnarValue> {
        if selection.entries.data_type()!=self.entry_type() {return Err(DataFusionError::Execution("selected entry owner schema differs".into()));}
        let rows = selection.parents.len();
        if rows==0 {return Ok(ColumnarValue::Array(new_empty_array(&self.final_type)));}
        let map = crate::expressions::indexed_aligned_map(&self.program)
            .ok_or_else(|| DataFusionError::Internal("selected indexed program absent".into()))?;
        let output = map
            .evaluate_selected_captures_with_bundle(&selection, captures, bundle)?
            .into_array(rows)?;
        let lists = output.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("selected aligned output is not a list".into())
        })?;
        let cells = lists
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("selected aligned output is not cells".into())
            })?;
        let values = cells.column(0).clone();
        let nulls = NullBuffer::union(values.nulls(), cells.nulls());
        let values = make_array(values.to_data().into_builder().nulls(nulls).build()?);
        let selected = match &self.projection {
            Some(view) => view.bind_validated(&values)?,
            None => values,
        };
        let DataType::List(item) = &self.final_type else {
            unreachable!()
        };
        let DataType::Struct(fields) = item.data_type() else {
            unreachable!()
        };
        let cells =
            Arc::new(StructArray::try_new(fields.clone(), vec![selected], None)?) as ArrayRef;
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            item.clone(),
            lists.offsets().clone(),
            cells,
            lists.nulls().cloned(),
        )?)))
    }
}

/// Recognize only the typed cell-wrapper field-selection produced by ordinary
/// select_entries. All original computations remain in their original order;
/// dropped expressions must be total ordinary entry-field reads.
pub fn recognize_selected_entry_projection(
    inner: &datafusion_expr::ScalarUDF,
    outer: &datafusion_expr::ScalarUDF,
) -> Option<SelectedAlignedProjection> {
    let (original, schema) = semantic_kernel(inner)?;
    let (selection, outer_schema) = semantic_kernel(outer)?;
    let options = semantic_preparation_options(inner)?;
    let proof = recognize_selected_entry_projection_expr(original, schema, selection, outer_schema, options)?;
    Some(SelectedAlignedProjection {
        inner: inner.clone(), outer: outer.clone(),
        entry_argument: proof.entry_argument, entry_type: proof.entry_type,
    })
}

/// Proof-only recognition against native private argument frames. No physical
/// program is constructed until the enclosing table region has been admitted.
pub fn recognize_selected_entry_projection_expr<'a>(
    original: &'a Expr,
    schema: &'a Schema,
    selection: &'a Expr,
    outer_schema: &'a Schema,
    options: &'a crate::expressions::PreparationOptions,
) -> Option<SelectedAlignedExprProof<'a>> {
    fn value_parameter(expr: &Expr, parameter: &str) -> bool {
        matches!(expr, Expr::GetField { value, name } if name == "value"
            && matches!(value.as_ref(), Expr::LambdaVariable { name, .. } if name == parameter))
    }
    fn null(expr: &Expr) -> bool {
        matches!(expr, Expr::Literal { value, .. } if value.is_null())
            || matches!(expr, Expr::RuntimeLiteral { value } if value.is_null())
    }
    let Expr::ArrayMap {
        value,
        parameter,
        body,
    } = selection
    else {
        return None;
    };
    let Expr::Column { name } = value.as_ref() else {
        return None;
    };
    if outer_schema.fields().len() != 1 || outer_schema.index_of(name).ok()? != 0 {
        return None;
    }
    let Expr::Struct { fields } = body.as_ref() else {
        return None;
    };
    let [(value_name, body)] = fields.as_slice() else {
        return None;
    };
    if value_name != "value" {
        return None;
    }
    let Expr::IfElse {
        condition,
        consequent,
        alternate,
    } = body
    else {
        return None;
    };
    let Expr::Unary {
        operator: UnaryOperator::Not,
        value: missing,
    } = condition.as_ref()
    else {
        return None;
    };
    let Expr::IsMissing { value } = missing.as_ref() else {
        return None;
    };
    if !value_parameter(value, parameter) || !null(alternate) {
        return None;
    }
    let Expr::Struct { fields: projected } = consequent.as_ref() else {
        return None;
    };
    let names = projected
        .iter()
        .map(|(name, value)| {
            let Expr::GetField {
                value,
                name: source,
            } = value
            else {
                return None;
            };
            (name == source && value_parameter(value, parameter)).then_some(name.clone())
        })
        .collect::<Option<HashSet<_>>>()?;
    if names.len() != projected.len() {
        return None;
    }
    let Expr::AlignedMatrixMap {
        entries,
        columns,
        rows,
        globals,
        body,
        preserve_cell: true,
        ..
    } = original
    else {
        return None;
    };
    let Expr::Column { name: entry_name } = entries.as_ref() else {
        return None;
    };
    let entry_argument = schema.index_of(entry_name).ok()?;
    for child in [columns.as_ref(), rows.as_ref(), globals.as_ref()] {
        let mut uses_entry = false;
        child.visit_unique(&mut |node| {
            uses_entry |= matches!(node, Expr::Column { name } if name == entry_name);
        });
        if uses_entry {
            return None;
        }
    }
    let Expr::IfElse {
        condition,
        consequent,
        alternate,
    } = body.as_ref()
    else {
        return None;
    };
    if !matches!(condition.as_ref(), Expr::AlignedMapInput { axis, .. } if axis == "entry_present")
        || !null(alternate)
    {
        return None;
    }
    let Expr::Struct { fields } = consequent.as_ref() else {
        return None;
    };
    if !crate::indexed_batch::ordinary_entry_reads(
        fields
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(_, value)| value),
    ) {
        return None;
    }
    let available = fields
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<HashSet<_>>();
    if !names.is_subset(&available) {
        return None;
    }
    let final_type = selection.dtype(outer_schema).ok()?.to_arrow().ok()?;
    Some(SelectedAlignedExprProof {
        original, schema, options, names, entry_argument,
        entry_type: schema.field(entry_argument).data_type().clone(), final_type,
    })
}

/// An immutable semantic match. Physical eligibility is checked before compiling
/// the selected body, so rejected candidates do not construct an evaluator.
pub struct SelectedAlignedProjection {
    inner: datafusion_expr::ScalarUDF,
    outer: datafusion_expr::ScalarUDF,
    entry_argument: usize,
    entry_type: DataType,
}
impl SelectedAlignedProjection {
    pub fn entry_argument(&self) -> usize { self.entry_argument }
    pub fn entry_type(&self) -> &DataType { &self.entry_type }
    pub fn prepare(self) -> Option<SelectedAlignedProgram> {
        let (original, schema) = semantic_kernel(&self.inner)?;
        let (selection, outer_schema) = semantic_kernel(&self.outer)?;
        let options = semantic_preparation_options(&self.inner)?;
        let mut proof = recognize_selected_entry_projection_expr(original, schema, selection, outer_schema, options)?;
        let arguments = outer_schema.fields().iter().map(|field| field.data_type().clone()).collect::<Vec<_>>();
        proof.final_type = self.outer.return_type(&arguments).ok()?;
        proof.prepare()
    }
}

/// Borrowed proof: its caller retains the exact expression and schema owners.
/// A rebuilt child must be recognized again before preparation.
pub struct SelectedAlignedExprProof<'a> {
    original: &'a Expr,
    schema: &'a Schema,
    options: &'a crate::expressions::PreparationOptions,
    names: HashSet<String>,
    entry_argument: usize,
    entry_type: DataType,
    final_type: DataType,
}
impl SelectedAlignedExprProof<'_> {
    pub fn entry_argument(&self) -> usize { self.entry_argument }
    pub fn entry_type(&self) -> &DataType { &self.entry_type }
    pub fn output_type(&self) -> &DataType { &self.final_type }
    /// Selection removes ordinary passthrough fields before indexed lowering.
    /// Testing the unselected body would incorrectly reject an unused Struct
    /// owner which the selected program never reads or gathers.
    pub fn supports_indexed_body(&self) -> bool {
        let Expr::AlignedMatrixMap { body, .. } = self.original else { return false; };
        let Expr::IfElse { consequent, .. } = body.as_ref() else { return false; };
        let Expr::Struct { fields } = consequent.as_ref() else { return false; };
        let selected = Expr::Struct {
            fields: fields.iter().filter(|(name, _)| self.names.contains(name)).cloned().collect(),
        };
        crate::expressions::selected_aligned_body_supported(&selected)
    }
    pub(crate) fn original(&self) -> &Expr { self.original }
    pub(crate) fn schema(&self) -> &Schema { self.schema }
    pub(crate) fn options(&self) -> &crate::expressions::PreparationOptions { self.options }
    pub(crate) fn retains(&self, name: &str) -> bool { self.names.contains(name) }
    pub(crate) fn prepare_rewritten(&self, original: &Expr, schema: &Schema) -> Option<SelectedAlignedProgram> {
        SelectedAlignedExprProof { original, schema, options: self.options, names: self.names.clone(), entry_argument: self.entry_argument, entry_type: schema.field(self.entry_argument).data_type().clone(), final_type: self.final_type.clone() }.prepare()
    }
    pub fn prepare(self) -> Option<SelectedAlignedProgram> {
        let Self { original, schema, options, names, entry_argument, final_type, .. } = self;
    let Expr::AlignedMatrixMap { columns, rows, globals, body, .. } = original else { unreachable!() };
    let Expr::IfElse { condition, consequent, .. } = body.as_ref() else { unreachable!() };
    let Expr::Struct { fields } = consequent.as_ref() else { unreachable!() };
    let retained = fields
        .iter()
        .filter(|(name, _)| names.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    let selected_body = Expr::Struct { fields: retained };
    // Type against the original aligned body context, before changing its output.
    let Type::Array { array: cell } = original.dtype(schema).ok()? else {
        return None;
    };
    let Type::Struct { r#struct: cell } = cell.as_ref() else {
        return None;
    };
    let original_entry = cell.iter().find(|(name, _)| name == "value")?.1.clone();
    let Type::Struct {
        r#struct: entry_fields,
    } = original_entry
    else {
        return None;
    };
    let entry_type = Type::Struct {
        r#struct: entry_fields
            .into_iter()
            .filter(|(name, _)| names.contains(name))
            .collect(),
    };
    let mut rewritten = original.clone();
    let Expr::AlignedMatrixMap { body, dtype, .. } = &mut rewritten else {
        unreachable!()
    };
    *body = Arc::new(Expr::IfElse {
        condition: condition.clone(),
        consequent: Arc::new(selected_body),
        alternate: Arc::new(Expr::Literal {
            dtype: entry_type.clone(),
            value: serde_json::Value::Null,
        }),
    });
    *dtype = Type::Array {
        array: Box::new(Type::Struct {
            r#struct: vec![("value".into(), entry_type.clone())],
        }),
    };
    let program = crate::expressions::lower_mode_with_options(&rewritten, schema, true, options).ok()?;
    crate::expressions::indexed_aligned_map(&program)?;
    let captures = [
        PreparedCapture::prepare(columns, schema, options)?,
        PreparedCapture::prepare(rows, schema, options)?,
        PreparedCapture::prepare(globals, schema, options)?,
    ];
    let DataType::List(item) = &final_type else {
        return None;
    };
    let DataType::Struct(cell) = item.data_type() else {
        return None;
    };
    let [value_field] = cell.as_ref() else {
        return None;
    };
    let projection = crate::PreparedArrayProjection::prepare(
        &entry_type.to_arrow().ok()?,
        value_field.data_type(),
    )
    .ok()?;
    Some(SelectedAlignedProgram {
        entry_argument,
        entry_type: schema.field(entry_argument).data_type().clone(),
        program,
        captures,
        projection,
        final_type,
    })
    }
}

/// Existing closed callback policy, exposed for native table-region proofs.
pub fn can_block_selected_body(expression: &Expr) -> bool {
    crate::expression_effects::can_block_list_callback(expression)
}

/// A filtered-out lane may skip input bounds/Call validation, under Vail's
/// accepted invalid-input policy. Explicit errors, arithmetic failures,
/// parsing, randomness and unknown functions remain evaluation barriers.
/// This is deliberately separate from changing a callback's batch boundaries.
pub fn can_defer_selected_body(expression: &Expr) -> bool {
    crate::expression_effects::can_defer_selected_body(expression)
}

/// Total metadata computations may run over parent/choice coordinates. The
/// entry-owner marker is never a metadata input, even through a nested field.
pub fn selected_metadata_expr(expression: &Expr, marker: Option<&str>) -> bool {
    let mut allowed = true;
    expression.visit_unique(&mut |node| {
        allowed &= match node {
            Expr::Column { name } => marker != Some(name.as_str()),
            Expr::Global { .. } | Expr::ExternalGlobal { .. }
            | Expr::Literal { .. } | Expr::RuntimeLiteral { .. }
            | Expr::Struct { .. } | Expr::GetField { .. }
            | Expr::IsMissing { .. } | Expr::Unary { .. }
            | Expr::ArrayLength { .. } => true,
            Expr::Binary { .. } | Expr::IntegerBinary { .. } => node.is_pure(),
            _ => false,
        };
    });
    allowed
}

pub fn selected_entry_projection(
    inner: &datafusion_expr::ScalarUDF,
    outer: &datafusion_expr::ScalarUDF,
) -> Option<SelectedAlignedProgram> {
    recognize_selected_entry_projection(inner, outer)?.prepare()
}

/// One branch of a deterministic semantic zip, evaluated against bindings
/// already captured once by its caller. Scalar owners remain scalar captures.
#[derive(Debug, Clone)]
pub struct BoundSemanticProgram {
    function: Arc<datafusion_expr::ScalarUDF>,
    inputs: Vec<usize>,
    fields: Vec<arrow_schema::FieldRef>,
    output: arrow_schema::FieldRef,
    take: Option<Box<(BoundSemanticProgram, BoundSemanticProgram)>>,
}
impl BoundSemanticProgram {
    pub fn count_take(&self, bindings: &[datafusion_expr::ColumnarValue], rows: usize, fields: &[String])
        -> Option<datafusion_common::Result<usize>> {
        self.take.as_ref().map(|programs| {
            let source = programs.0.evaluate(bindings, rows)?;
            let positions = programs.1.evaluate(bindings, rows)?;
            crate::matrix_arrays::count_take(source, positions, rows, fields)
        })
    }
    pub fn evaluate(&self, bindings: &[datafusion_expr::ColumnarValue], rows: usize)
        -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        let args = self.inputs.iter().map(|index| bindings.get(*index).cloned().ok_or_else(||
            datafusion_common::DataFusionError::Internal("semantic zip binding missing".into())))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        self.function.invoke_with_args(datafusion_expr::ScalarFunctionArgs {
            args, arg_fields: self.fields.clone(), number_rows: rows, return_field: self.output.clone(),
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        })
    }
}
/// Compile the exact native branches without changing scalar specialization
/// or value-domain checks. Original physical bindings must be evaluated once
/// before invoking these branches, in their original argument order.
pub fn semantic_zip_programs(function: &datafusion_expr::ScalarUDF) -> Option<Vec<BoundSemanticProgram>> {
    if !semantic_kernel_can_split(function) { return None; }
    let (expression, schema) = semantic_kernel(function)?;
    let Expr::NestedFunction { function: name, args, .. } = expression else { return None; };
    if name != "arrays_zip" { return None; }
    let options = semantic_preparation_options(function)?;
    args.iter().map(|argument| compile_bound_program(argument, schema, true, options)).collect()
}
fn compile_bound_program(argument: &Expr, schema: &Schema, with_take: bool, options: &crate::expressions::PreparationOptions) -> Option<BoundSemanticProgram> {
    let df_schema = DFSchema::try_from(schema.clone()).ok()?;
    let compiled = logical_kernel_with_options(argument, &df_schema, &Schema::empty(), &BTreeMap::new(), &[], options).ok()?;
    let LogicalExpr::ScalarFunction(compiled) = compiled else { return None; };
    let inputs = compiled.args.iter().map(|argument| {
        let LogicalExpr::Column(column) = argument else { return None; };
        schema.index_of(&column.name).ok()
    }).collect::<Option<Vec<_>>>()?;
    let fields = inputs.iter().map(|index| schema.fields()[*index].clone()).collect();
    let dtype = argument.dtype_with_globals(schema, &Schema::empty()).ok()?.to_arrow().ok()?;
    let take = if with_take {
        if let Expr::NestedFunction { function, args, .. } = argument {
            if function == "__vail_list_take" && args.len() == 2 {
                Some(Box::new((compile_bound_program(&args[0], schema, false, options)?, compile_bound_program(&args[1], schema, false, options)?)))
            } else { None }
        } else { None }
    } else { None };
    Some(BoundSemanticProgram { function: compiled.func, inputs, fields, take,
        output: Arc::new(arrow_schema::Field::new("value", dtype, true)) })
}

fn contains_global_reference(expression: &Expr) -> bool {
    let mut found = false;
    expression.visit_unique(&mut |node| {
        found |= matches!(node, Expr::Global { .. } | Expr::ExternalGlobal { .. });
    });
    found
}

/// Lower a Vail expression against a logical input schema and global bindings.
///
/// Binding indices for external globals address `schema` (the joined logical
/// input), so generated references are ordinary DataFusion columns and remain
/// visible to optimizer rules.
pub fn lower_logical_with_bindings(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
) -> Result<LogicalExpr, ExprError> {
    let mut seen = HashSet::new();
    let mut shared = HashSet::new();
    expr.visit_unique(&mut |node| {
        node.for_each_child(&mut |child| {
            if !seen.insert(child as *const Expr as usize)
                && !matches!(
                    child,
                    Expr::Column { .. }
                        | Expr::Global { .. }
                        | Expr::ExternalGlobal { .. }
                        | Expr::Literal { .. }
                        | Expr::RuntimeLiteral { .. }
                        | Expr::LambdaVariable { .. }
                )
            {
                shared.insert(child as *const Expr as usize);
            }
        });
    });
    lower(expr, schema, globals, bindings, &[], &shared)
}

/// Convenience form for expressions without globals.
pub fn lower_logical(expr: &Expr, schema: &DFSchema) -> Result<LogicalExpr, ExprError> {
    lower_logical_with_bindings(expr, schema, &Schema::empty(), &BTreeMap::new())
}

/// Change compatible Arrow descriptors without casting or copying value buffers.
pub fn restore_arrow_array_type(
    array: &arrow_array::ArrayRef,
    dtype: &arrow_schema::DataType,
) -> datafusion_common::Result<arrow_array::ArrayRef> {
    if array.data_type() == dtype {
        return Ok(array.clone());
    }
    restore_arrow_array_type_mismatch(array, dtype)
}

// The caller has already checked the actual descriptor. Avoid repeating its
// potentially deep metadata equality test after scalar materialization.
fn restore_arrow_array_type_mismatch(
    array: &arrow_array::ArrayRef,
    dtype: &arrow_schema::DataType,
) -> datafusion_common::Result<arrow_array::ArrayRef> {
    if vail_ordering::same_layout(array.data_type(), dtype) {
        fn retag_data(
            data: arrow_data::ArrayData,
            dtype: &arrow_schema::DataType,
        ) -> datafusion_common::Result<arrow_data::ArrayData> {
            let child_types = match dtype {
                arrow_schema::DataType::List(field)
                | arrow_schema::DataType::LargeList(field)
                | arrow_schema::DataType::FixedSizeList(field, _) => vec![field.data_type()],
                arrow_schema::DataType::Struct(fields) => {
                    fields.iter().map(|field| field.data_type()).collect()
                }
                _ => Vec::new(),
            };
            let children = data
                .child_data()
                .iter()
                .zip(child_types)
                .map(|(child, child_type)| {
                    // A changed parent descriptor need not rebuild children
                    // which already carry the exact requested metadata.
                    if child.data_type() == child_type {
                        Ok(child.clone())
                    } else {
                        retag_data(child.clone(), child_type)
                    }
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(data
                .into_builder()
                .data_type(dtype.clone())
                .child_data(children)
                .build()?)
        }
        return Ok(arrow_array::make_array(retag_data(array.to_data(), dtype)?));
    }
    Ok(arrow_cast::cast(array.as_ref(), dtype)?)
}

/// Restore descriptors while preserving scalar ownership and shared buffers.
pub fn restore_arrow_value_type(
    value: datafusion_expr::ColumnarValue,
    dtype: &arrow_schema::DataType,
) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
    use datafusion_expr::ColumnarValue;
    let scalar = match value {
        ColumnarValue::Array(array) => {
            return restore_arrow_array_type(&array, dtype).map(ColumnarValue::Array);
        }
        ColumnarValue::Scalar(value) => value,
    };
    // Compare the actual scalar descriptor, not an expression's advertised
    // type: library kernels can lose nested metadata despite reporting the
    // requested logical type. Matching scalars need neither materialization
    // nor extraction through a one-element array.
    if scalar.data_type() == *dtype {
        return Ok(ColumnarValue::Scalar(scalar));
    }
    let array: arrow_array::ArrayRef = match scalar {
        ScalarValue::List(array) => array,
        ScalarValue::Struct(array) => array,
        value => value.to_array()?,
    };
    let array = restore_arrow_array_type_mismatch(&array, dtype)?;
    Ok(ColumnarValue::Scalar(
        crate::expressions::scalar_at(&array, 0)
            .map_err(|error| datafusion_common::DataFusionError::Execution(error.to_string()))?,
    ))
}

/// Restore an IR-declared type after a library operator constructs new Arrow
/// fields. The scalar result retains the original nested Arrow fields, so
/// constant folding preserves logical collection tags and reference metadata.
pub fn restore_type(value: LogicalExpr, dtype: arrow_schema::DataType) -> LogicalExpr {
    let output = dtype.clone();
    let udf = crate::kernel_identity::scalar(
        "vail_restore_arrow_type",
        dtype.clone(),
        Signature::any(1, Volatility::Immutable),
        dtype,
        Arc::new(move |args| restore_arrow_value_type(args[0].clone(), &output)),
    );
    udf.call(vec![value])
}

fn boolean_case(
    base: LogicalExpr,
    yes: LogicalExpr,
    no: LogicalExpr,
    missing: LogicalExpr,
) -> LogicalExpr {
    LogicalExpr::Case(datafusion_expr::expr::Case::new(
        Some(Box::new(base)),
        vec![
            (Box::new(lit(true)), Box::new(yes)),
            (Box::new(lit(false)), Box::new(no)),
        ],
        Some(Box::new(missing)),
    ))
}

fn coerce_type(
    value: LogicalExpr,
    dtype: arrow_schema::DataType,
    schema: &DFSchema,
) -> Result<LogicalExpr, ExprError> {
    let actual = value.get_type(schema)?;
    if actual == dtype {
        return Ok(value);
    }
    if vail_ordering::same_layout(&actual, &dtype) {
        return Ok(restore_type(value, dtype));
    }
    Ok(value.cast_to(&dtype, schema)?)
}

fn lower(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
    scope: &[String],
    shared: &HashSet<usize>,
) -> Result<LogicalExpr, ExprError> {
    // DF logical expressions are owned trees. Keep multiply referenced native
    // subgraphs behind a native semantic kernel rather than cloning their
    // expanded logical tree. Operators outside the shared subgraph remain
    // visible to pushdown and normal logical optimization.
    if shared.contains(&(expr as *const Expr as usize)) {
        return logical_kernel(expr, schema, globals, bindings, scope);
    }
    if crate::list_flatmap::has_reduction_region(expr) {
        // Only an admitted joint program needs this opaque boundary. Declined
        // shapes keep the ordinary logical fields visible to DataFusion.
        if let Ok(Some(kernel)) = logical_kernel_admitted(
            expr, schema, globals, bindings, scope,
            &crate::expressions::PreparationOptions::capture(), true,
        ) {
            return Ok(kernel);
        }
    }
    let recurse = |e: &Expr| lower(e, schema, globals, bindings, scope, shared);
    Ok(match expr {
        Expr::MatrixScope { .. }
        | Expr::TableLookup { .. }
        | Expr::MatrixField { .. }
        | Expr::MatrixNumCols { .. }
        | Expr::MatrixColumnPosition { .. }
        | Expr::AlignedMapInput { .. } => {
            return Err(ExprError::Invalid(
                "unresolved frontend expression scope".into(),
            ));
        }
        Expr::RuntimeLiteral { value } => lit(value.clone()),
        Expr::Literal { dtype, value } => {
            if !matches!(dtype, Type::Primitive(_)) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            lit(literal_scalar(value, dtype)?)
        }
        Expr::Column { name } => {
            let start = globals.fields().len();
            let matches = (start..schema.fields().len())
                .filter(|&i| schema.field(i).name() == name)
                .collect::<Vec<_>>();
            let index = if matches.len() == 1 {
                Some(matches[0])
            } else {
                None
            }
            .ok_or_else(|| {
                ExprError::Invalid(format!("source column {name:?} is absent or ambiguous"))
            })?;
            let (qualifier, field) = schema.qualified_field(index);
            LogicalExpr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                field.name().clone(),
            ))
        }
        Expr::Global { name } => {
            let index = globals
                .index_of(name)
                .map_err(|_| ExprError::Invalid(format!("global column {name:?} is absent")))?;
            let field = globals.field(index);
            let logical_index = index;
            if logical_index >= schema.fields().len() {
                return Err(ExprError::Invalid(format!(
                    "global column {name:?} is not bound in input"
                )));
            }
            let (qualifier, actual) = schema.qualified_field(logical_index);
            if actual.data_type() != field.data_type() {
                return Err(ExprError::Invalid(format!(
                    "global column {name:?} type differs from its declaration"
                )));
            }
            LogicalExpr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                actual.name().clone(),
            ))
        }
        Expr::ExternalGlobal { reference, dtype } => {
            let index = *bindings.get(reference).ok_or_else(|| {
                ExprError::Invalid(format!("unbound external global {reference:?}"))
            })?;
            if index >= schema.fields().len() {
                return Err(ExprError::Invalid(format!(
                    "external global column {reference:?} is absent"
                )));
            }
            let (qualifier, field) = schema.qualified_field(index);
            let actual = Type::from_arrow(field.data_type())?;
            if actual != *dtype {
                return Err(ExprError::Invalid(format!(
                    "external global type mismatch: expected {dtype:?}, found {actual:?}"
                )));
            }
            LogicalExpr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                field.name().clone(),
            ))
        }
        Expr::LambdaVariable { name, .. } => {
            if !scope.iter().any(|bound| bound == name) {
                return Err(ExprError::Invalid(format!(
                    "unbound lambda variable {name:?}"
                )));
            }
            return Err(ExprError::Invalid(
                "lambda variables require a DataFusion higher-order expression".into(),
            ));
        }
        Expr::Binary {
            operator,
            left,
            right,
        } => {
            if matches!(
                operator,
                BinaryOperator::Eq
                    | BinaryOperator::Ne
                    | BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge
            ) {
                let left_type = left.dtype_with_globals(&schema.as_arrow(), globals)?;
                let right_type = right.dtype_with_globals(&schema.as_arrow(), globals)?;
                let common = logical_promote(&left_type, &right_type)?;
                if matches!(operator, BinaryOperator::Eq | BinaryOperator::Ne)
                    && common.contains_unordered()
                {
                    return Err(ExprError::Unsupported(
                        "Set/Dictionary equality requires logical collection operators".into(),
                    ));
                }
                if !matches!(operator, BinaryOperator::Eq | BinaryOperator::Ne) {
                    common.ensure_keyable()?;
                    if !common.native_key() {
                        return Err(ExprError::Unsupported(format!(
                            "ordering comparison for {common:?}"
                        )));
                    }
                }
                let common_arrow = common.to_arrow()?;
                let left = recurse(left)?.cast_to(&common_arrow, schema)?;
                let right = recurse(right)?.cast_to(&common_arrow, schema)?;
                datafusion_expr::expr_fn::binary_expr(left, binary_operator(*operator)?, right)
            } else if *operator == BinaryOperator::Pow {
                let dtype = expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?;
                datafusion_functions::math::power().call(vec![
                    recurse(left)?.cast_to(&dtype, schema)?,
                    recurse(right)?.cast_to(&dtype, schema)?,
                ])
            } else if *operator == BinaryOperator::Floordiv
                && matches!(
                    expr.dtype_with_globals(&schema.as_arrow(), globals)?,
                    Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64)
                )
            {
                let dtype = expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?;
                let divided = datafusion_expr::expr_fn::binary_expr(
                    recurse(left)?.cast_to(&arrow_schema::DataType::Float64, schema)?,
                    Operator::Divide,
                    recurse(right)?.cast_to(&arrow_schema::DataType::Float64, schema)?,
                );
                datafusion_functions::math::floor()
                    .call(vec![divided])
                    .cast_to(&dtype, schema)?
            } else if matches!(
                operator,
                BinaryOperator::Floordiv | BinaryOperator::Mod | BinaryOperator::Pow
            ) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            } else {
                let result_type = expr
                    .dtype_with_globals(&schema.as_arrow(), globals)?
                    .to_arrow()?;
                let mut left_expr = recurse(left)?;
                let mut right_expr = recurse(right)?;
                if *operator == BinaryOperator::Div {
                    left_expr = left_expr.cast_to(&result_type, schema)?;
                    right_expr = right_expr.cast_to(&result_type, schema)?;
                } else if matches!(
                    operator,
                    BinaryOperator::Add | BinaryOperator::Sub | BinaryOperator::Mul
                ) {
                    left_expr = left_expr.cast_to(&result_type, schema)?;
                    right_expr = right_expr.cast_to(&result_type, schema)?;
                }
                datafusion_expr::expr_fn::binary_expr(
                    left_expr,
                    binary_operator(*operator)?,
                    right_expr,
                )
            }
        }
        Expr::IntegerBinary {
            operator,
            left,
            right,
        } => datafusion_expr::expr_fn::binary_expr(
            recurse(left)?,
            binary_operator(*operator)?,
            recurse(right)?,
        ),
        Expr::Unary { operator, value } => {
            let value = recurse(value)?;
            match operator {
                UnaryOperator::Neg => match value.get_type(schema)? {
                    arrow_schema::DataType::Int32 => {
                        datafusion_expr::expr_fn::binary_expr(lit(0_i32), Operator::Minus, value)
                    }
                    arrow_schema::DataType::Int64 => {
                        datafusion_expr::expr_fn::binary_expr(lit(0_i64), Operator::Minus, value)
                    }
                    _ => LogicalExpr::Negative(Box::new(value)),
                },
                UnaryOperator::Pos => value,
                UnaryOperator::Not => LogicalExpr::Not(Box::new(value)),
            }
        }
        Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Call),
            ..
        } => return logical_kernel(expr, schema, globals, bindings, scope),
        Expr::Cast { dtype, value } => {
            let source = value.dtype_with_globals(&schema.as_arrow(), globals)?;
            let fallible = matches!(source, Type::Primitive(PrimitiveType::Str))
                && source != *dtype
                || matches!(
                    source,
                    Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64)
                ) && matches!(
                    dtype,
                    Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64)
                )
                || matches!(source, Type::Primitive(PrimitiveType::Int64))
                    && matches!(dtype, Type::Primitive(PrimitiveType::Int32));
            // A checked cast must not move before a preceding row filter:
            // discarded headers and other excluded values need not parse.
            // The semantic kernel still executes DataFusion's native cast.
            if fallible {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            coerce_type(recurse(value)?, dtype.to_arrow()?, schema)?
        }
        Expr::IsMissing { value } => recurse(value)?.is_null(),
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            let dtype = expr
                .dtype_with_globals(&schema.as_arrow(), globals)?
                .to_arrow()?;
            if crate::expressions::masks_borrowed_read(consequent, alternate)
                || (dtype == DataType::Boolean && crate::boolean_fill::candidate(expr).is_some()) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let condition = recurse(condition)?;
            let consequent = coerce_type(recurse(consequent)?, dtype.clone(), schema)?;
            let alternate = coerce_type(recurse(alternate)?, dtype.clone(), schema)?;
            let null_result = lit(ScalarValue::try_from(&dtype)?);
            datafusion_expr::expr_fn::case(condition)
                .when(lit(true), consequent)
                .when(lit(false), alternate)
                .otherwise(null_result)?
        }
        Expr::Struct { fields } => {
            // SQL named_struct rejects empty identifiers, which Hail permits.
            // The native struct kernel preserves those Arrow field names.
            if fields.is_empty() || fields.iter().any(|(name, _)| name.is_empty()) {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let mut args = Vec::with_capacity(fields.len() * 2);
            for (name, value) in fields {
                args.push(lit(ScalarValue::Utf8(Some(name.clone()))));
                args.push(lower(value, schema, globals, bindings, scope, shared)?);
            }
            datafusion_functions::core::named_struct().call(args)
        }
        Expr::Tuple { elements } => {
            let mut args = Vec::with_capacity(elements.len() * 2);
            for (i, value) in elements.iter().enumerate() {
                args.push(lit(ScalarValue::Utf8(Some(format!("__vail_tuple_{i}")))));
                args.push(recurse(value)?);
            }
            if elements.is_empty() {
                args.extend([
                    lit(ScalarValue::Utf8(Some("__vail_tuple_empty".into()))),
                    lit(false),
                ]);
            }
            let dtype = expr
                .dtype_with_globals(schema.as_arrow(), globals)?
                .to_arrow()?;
            coerce_type(
                datafusion_functions::core::named_struct().call(args),
                dtype,
                schema,
            )?
        }
        Expr::GetField { .. } => return logical_kernel(expr, schema, globals, bindings, scope),
        Expr::ArrayAggregateScan {
            function,
            value,
            predicate,
            options,
            ..
        } => {
            let input = value
                .dtype_with_globals(schema.as_arrow(), globals)?
                .to_arrow()?;
            crate::array_aggregate_scan::make_udf(function, &input, options)?
                .call(vec![recurse(value)?, recurse(predicate)?])
        }
        Expr::NestedFunction { function, args, .. } if function == "coalesce" => {
            expr.dtype_with_globals(schema.as_arrow(), globals)?;
            boolean_case(
                recurse(&args[0])?,
                lit(true),
                lit(false),
                recurse(&args[1])?,
            )
        }
        Expr::NestedFunction { function, args, .. }
            if function == "__vail_list_reduce_collect_as_set"
                && args.len() == 1
                && crate::expression_effects::effects(expr) == crate::expression_effects::Effects::Pure
                && crate::list_aggregate::batch_distinct_compatible(
                    &args[0].dtype_with_globals(schema.as_arrow(), globals)?,
                ) =>
        {
            let dtype = expr.dtype_with_globals(schema.as_arrow(), globals)?.to_arrow()?;
            let input_type = args[0].dtype_with_globals(schema.as_arrow(), globals)?.to_arrow()?;
            let compact = crate::list_aggregate::compact_defined_lists_udf(input_type)?
                .call(vec![recurse(&args[0])?]);
            let distinct = datafusion_functions_nested::set_ops::array_distinct_udf()
                .call(vec![compact]);
            restore_type(distinct, dtype)
        }
        Expr::NestedFunction { function, args, .. } if function == "__vail_list_reduce_collect" => {
            // The input already is the ordered collection, including missing
            // elements. Expose this identity to DataFusion rather than hiding
            // a projection behind a per-list aggregate adapter.
            let dtype = expr
                .dtype_with_globals(schema.as_arrow(), globals)?
                .to_arrow()?;
            restore_type(recurse(&args[0])?, dtype)
        }
        Expr::NestedFunction { function, args, .. } => {
            if crate::expression_effects::effects(expr) != crate::expression_effects::Effects::Pure
            {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let udf = crate::expressions::nested_function(function)?;
            let arguments = args
                .iter()
                .map(|arg| {
                    let ty = arg
                        .dtype_with_globals(schema.as_arrow(), globals)?
                        .to_arrow()?;
                    // The indexing UDF derives its tuple descriptor from the
                    // element's logical type, including opaque Call storage.
                    let input_type = if function == "__vail_index_values" {
                        ty
                    } else {
                        vail_ordering::without_type_metadata(&ty)
                    };
                    coerce_type(recurse(arg)?, input_type, schema)
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let dtype = expr
                .dtype_with_globals(schema.as_arrow(), globals)?
                .to_arrow()?;
            restore_type(udf.call(arguments), dtype)
        }
        Expr::StringFunction { function, args } => {
            if crate::expression_effects::effects(expr) != crate::expression_effects::Effects::Pure
            {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            let (udf, _, _) = crate::expressions::string_function(function)?;
            udf.call(args.iter().map(recurse).collect::<Result<Vec<_>, _>>()?)
        }
        Expr::CollectionCast { value, dtype } => restore_type(recurse(value)?, dtype.to_arrow()?),
        Expr::ToString { value } => {
            let ty = value.dtype_with_globals(&schema.as_arrow(), globals)?;
            let value = recurse(value)?;
            if matches!(ty, Type::Primitive(PrimitiveType::Str)) {
                value
            } else if matches!(
                ty,
                Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64 | PrimitiveType::Bool)
            ) {
                value.cast_to(&arrow_schema::DataType::Utf8, schema)?
            } else {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
        }
        Expr::Array { elements, .. } => {
            let args = elements
                .iter()
                .map(|element| {
                    let ty = element
                        .dtype_with_globals(schema.as_arrow(), globals)?
                        .to_arrow()?;
                    coerce_type(
                        recurse(element)?,
                        vail_ordering::without_type_metadata(&ty),
                        schema,
                    )
                })
                .collect::<Result<Vec<_>, ExprError>>()?;
            let value = datafusion_functions_nested::make_array::make_array_udf().call(args);
            restore_type(
                value,
                expr.dtype_with_globals(schema.as_arrow(), globals)?
                    .to_arrow()?,
            )
        }
        Expr::ArrayReduce { .. } if crate::expressions::scalar_integer_extrema(expr).is_some() => {
            let (operation, arguments) = crate::expressions::scalar_integer_extrema(expr).unwrap();
            let function = match operation {
                crate::expressions::ArrayReduction::Min => datafusion_functions::core::least(),
                crate::expressions::ArrayReduction::Max => datafusion_functions::core::greatest(),
                _ => unreachable!(),
            };
            function.call(arguments.iter().map(recurse).collect::<Result<Vec<_>, _>>()?)
        }
        Expr::ArrayLength { value } => crate::array_length::udf().call(vec![recurse(value)?]),
        Expr::ArrayFlatten { value } => {
            if matches!(value.as_ref(), Expr::ArrayMap { .. } | Expr::ArrayFilter { .. } | Expr::ArrayFlatten { .. })
                && matches!(expr.dtype_with_globals(schema.as_arrow(), globals).and_then(|ty| ty.to_arrow()),
                    Ok(DataType::List(field)) if matches!(field.data_type(), DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 | DataType::Boolean | DataType::Struct(_)))
            {
                return logical_kernel(expr, schema, globals, bindings, scope);
            }
            datafusion_functions_nested::flatten::flatten_udf().call(vec![recurse(value)?])
        }
        Expr::StreamRange { .. } => {
            // A zero step is a value-domain error. Keep validation after row
            // filters and inside the selected conditional branch.
            return logical_kernel(expr, schema, globals, bindings, scope);
        }
        Expr::Aggregate { .. }
        | Expr::AggregateFold { .. }
        | Expr::AggGroupBy { .. }
        | Expr::AggArray { .. }
        | Expr::AggFilter { .. }
        | Expr::AggExplode { .. } => {
            return Err(ExprError::Invalid(
                "aggregation expressions require the DataFusion aggregate planner".into(),
            ));
        }
        Expr::Switch { .. }
        | Expr::Die { .. }
        | Expr::ArrayFold { .. }
        | Expr::ArrayMap { .. }
        | Expr::ArrayPredicate { .. }
        | Expr::ArrayFilter { .. }
        | Expr::ArrayGrouped { .. }
        | Expr::Interval { .. }
        | Expr::IntervalContains { .. }
        | Expr::IntervalOverlaps { .. }
        | Expr::ArrayIndex { .. }
        | Expr::ArraySlice { .. }
        | Expr::ArrayReduce { .. }
        | Expr::AlignedMatrixMap { .. }
        | Expr::Locus { .. }
        | Expr::LocusOp { .. }
        | Expr::LocusValid { .. }
        | Expr::LocusFromGlobalPosition { .. }
        | Expr::ParseLocus { .. }
        | Expr::ParseCsvLine { .. } => {
            return logical_kernel(expr, schema, globals, bindings, scope);
        }
    })
}

/// Keep uncommon Hail semantics in an explicit scalar UDF while binding every
/// source value as a UDF argument. Ordinary parent operators therefore remain
/// optimizer-visible, and column dependencies remain visible beneath the call.
fn bind_scalar_captures(
    expression: &Expr,
    scalars: &std::collections::HashMap<String, ScalarValue>,
    memo: &mut std::collections::HashMap<usize, Expr>,
) -> Result<Expr, ExprError> {
    let address = expression as *const Expr as usize;
    if let Some(value) = memo.get(&address) {
        return Ok(value.clone());
    }
    // Capture slots are free Column references in their own namespace;
    // lexical LambdaVariables cannot shadow or capture them accidentally.
    let result = if let Expr::Column { name } = expression
        && let Some(value) = scalars.get(name)
    {
        Expr::RuntimeLiteral {
            value: value.clone(),
        }
    } else {
        let mut children = vec![];
        expression.for_each_child(&mut |child| children.push(child));
        let mut children = children.into_iter();
        expression.try_map_children(|_| {
            bind_scalar_captures(children.next().expect("native child order"), scalars, memo)
        })?
    };
    memo.insert(address, result.clone());
    Ok(result)
}

fn logical_kernel(
    expr: &Expr,
    schema: &DFSchema,
    globals: &Schema,
    bindings: &BTreeMap<String, usize>,
    scope: &[String],
) -> Result<LogicalExpr, ExprError> {
    logical_kernel_with_options(expr, schema, globals, bindings, scope, &crate::expressions::PreparationOptions::capture())
}
fn logical_kernel_with_options(
    expr: &Expr, schema: &DFSchema, globals: &Schema, bindings: &BTreeMap<String, usize>,
    scope: &[String], options: &crate::expressions::PreparationOptions,
) -> Result<LogicalExpr, ExprError> {
    Ok(logical_kernel_admitted(expr, schema, globals, bindings, scope, options, false)?
        .expect("unconditional semantic kernel admission"))
}
fn logical_kernel_admitted(
    expr: &Expr, schema: &DFSchema, globals: &Schema, bindings: &BTreeMap<String, usize>,
    scope: &[String], options: &crate::expressions::PreparationOptions,
    require_multi_reduce: bool,
) -> Result<Option<LogicalExpr>, ExprError> {
    use std::collections::BTreeMap as Map;
    let options = options.clone();
    let arrow_schema = schema.as_arrow();
    let mut refs: Map<String, (Expr, Type, arrow_schema::DataType)> = Map::new();
    expr.visit_unique(&mut |node| {
        let item = match node {
            Expr::Column { name } => {
                let start = globals.fields().len();
                let matches = (start..schema.fields().len())
                    .filter(|&i| schema.field(i).name() == name)
                    .collect::<Vec<_>>();
                let Some(index) = (matches.len() == 1).then(|| matches[0]) else {
                    return;
                };
                let (qualifier, field) = schema.qualified_field(index);
                (
                    format!("column:{qualifier:?}:{}", field.name()),
                    node.clone(),
                    Type::from_arrow(field.data_type()),
                    field.data_type().clone(),
                )
            }
            Expr::Global { name } => {
                let Ok(index) = globals.index_of(name) else {
                    return;
                };
                if index >= schema.fields().len() {
                    return;
                }
                let input_index = index;
                let (qualifier, field) = schema.qualified_field(input_index);
                let Ok(ty) = Type::from_arrow(field.data_type()) else {
                    return;
                };
                (
                    format!("global:{qualifier:?}:{}", field.name()),
                    node.clone(),
                    Ok(ty),
                    field.data_type().clone(),
                )
            }
            Expr::ExternalGlobal { reference, .. } => {
                let Some(index) = bindings.get(reference).copied() else {
                    return;
                };
                if index >= schema.fields().len() {
                    return;
                }
                let (_, field) = schema.qualified_field(index);
                let Ok(ty) = Type::from_arrow(field.data_type()) else {
                    return;
                };
                (
                    format!("external:{reference}"),
                    node.clone(),
                    Ok(ty),
                    field.data_type().clone(),
                )
            }
            _ => return,
        };
        if let Ok(dtype) = item.2 {
            refs.entry(item.0).or_insert((item.1, dtype, item.3));
        }
    });
    let mut fields = Vec::with_capacity(refs.len());
    let mut logical_args = Vec::with_capacity(refs.len());
    let mut slots = Map::new();
    for (i, (key, (reference, _ty, actual_type))) in refs.iter().enumerate() {
        let slot = format!("__vail_arg_{i}");
        slots.insert(key.clone(), slot.clone());
        fields.push(arrow_schema::Field::new(&slot, actual_type.clone(), true));
        logical_args.push(lower(
            reference,
            schema,
            globals,
            bindings,
            scope,
            &HashSet::new(),
        )?);
    }
    fn replace_refs(
        expr: &Expr,
        shared: bool,
        slots: &Map<String, String>,
        schema: &DFSchema,
        globals: &Schema,
        bindings: &BTreeMap<String, usize>,
        memo: &mut std::collections::HashMap<usize, Arc<Expr>>,
    ) -> Result<RebuiltExpr, ExprError> {
        let address = expr as *const Expr as usize;
        if shared {
            if let Some(cached) = memo.get(&address) { return Ok(RebuiltExpr::Shared(cached.clone())); }
        }
        let result = replace_node(expr, slots, schema, globals, bindings, memo)?;
        if shared {
            let result = Arc::new(result);
            memo.insert(address, result.clone());
            Ok(RebuiltExpr::Shared(result))
        } else {
            Ok(RebuiltExpr::Owned(result))
        }
    }
    fn replace_node(
        expr: &Expr,
        slots: &Map<String, String>,
        schema: &DFSchema,
        globals: &Schema,
        bindings: &BTreeMap<String, usize>,
        memo: &mut std::collections::HashMap<usize, Arc<Expr>>,
    ) -> Result<Expr, ExprError> {
        let key = match expr {
            Expr::Column { name } => {
                let start = globals.fields().len();
                let matches = (start..schema.fields().len())
                    .filter(|&i| schema.field(i).name() == name)
                    .collect::<Vec<_>>();
                let index = if matches.len() == 1 {
                    Some(matches[0])
                } else {
                    None
                }
                .ok_or_else(|| {
                    ExprError::Invalid(format!("source column {name:?} is absent or ambiguous"))
                })?;
                let (q, field) = schema.qualified_field(index);
                format!("column:{q:?}:{}", field.name())
            }
            Expr::Global { name } => {
                let index = globals
                    .index_of(name)
                    .map_err(|_| ExprError::Invalid(format!("global {name:?} is absent")))?;
                let input = index;
                if input >= schema.fields().len() {
                    return Err(ExprError::Invalid(format!(
                        "global {name:?} is not bound in input"
                    )));
                }
                let (q, field) = schema.qualified_field(input);
                format!("global:{q:?}:{}", field.name())
            }
            Expr::ExternalGlobal { reference, .. } => {
                let index = *bindings.get(reference).ok_or_else(|| {
                    ExprError::Invalid(format!("unbound external global {reference:?}"))
                })?;
                if index >= schema.fields().len() {
                    return Err(ExprError::Invalid(format!(
                        "external global {reference:?} is absent"
                    )));
                }
                let _ = schema.qualified_field(index);
                format!("external:{reference}")
            }
            _ => {
                return expr.try_map_children_rebuilt(|child, shared| {
                    replace_refs(child, shared, slots, schema, globals, bindings, memo)
                });
            }
        };
        let slot = slots
            .get(&key)
            .ok_or_else(|| ExprError::Invalid(format!("missing logical kernel binding {key}")))?;
        Ok(Expr::Column { name: slot.clone() })
    }

    let rewritten = replace_refs(
        expr,
        false,
        &slots,
        schema,
        globals,
        bindings,
        &mut std::collections::HashMap::new(),
    )?.into_owned();
    let kernel_schema = Schema::new(fields);
    let prepared = if require_multi_reduce {
        crate::expressions::lower_multi_reduce_with_options(&rewritten, &kernel_schema, &options)
    } else {
        crate::expressions::lower_with_options(&rewritten, &kernel_schema, &options).map(Some)
    };
    let Some(kernel) = prepared.map_err(|error| match error {
        ExprError::Invalid(message) => ExprError::Invalid(format!(
            "{} kernel compilation: {message}", kernel_label(expr)
        )),
        other => other,
    })? else { return Ok(None); };
    let captured_expression = Arc::new(rewritten.clone());
    let output_type = expr
        .dtype_with_globals(&arrow_schema, globals)?
        .to_arrow()?;
    let kernel_schema = Arc::new(kernel_schema);
    let identity = (
        crate::expression_identity::Identity::with_bound_names(rewritten),
        kernel_schema.clone(),
    );
    let volatility = crate::expression_effects::effects(expr).volatility();
    let udf_name = format!("vail_semantic_kernel_{}", kernel_label(expr));
    let expected_output_type = output_type.clone();
    let trace_preparation=std::env::var_os("VAIL_TRACE_SEMANTIC_PREP").is_some();
    let preparation_label=udf_name.clone();
    let signature = if refs.is_empty() {
        Signature::nullary(volatility)
    } else {
        Signature::any(refs.len(), volatility)
    };
    // One prepared program per kernel, bounded to the most recent scalar
    // binding combination. Singleton globals normally reuse one immutable
    // Arrow owner for the entire action. New owners/values reprepare safely.
    let prepared = std::sync::Mutex::new(None::<(
        Vec<(usize, ScalarValue)>,
        Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        Arc<Schema>,
    )>);
    let owner_options = Some(options.clone());
    let udf = crate::kernel_identity::scalar_with_options(
        &udf_name,
        identity,
        signature,
        output_type,
        Arc::new(move |invocation| {
            // Excluded/empty selections must not evaluate value-domain errors.
            if invocation.number_rows == 0 {
                return Ok(datafusion_expr::ColumnarValue::Array(
                    arrow_array::new_empty_array(&expected_output_type),
                ));
            }
            let args = &invocation.args;
            let all_scalar = args
                .iter()
                .all(|value| matches!(value, datafusion_expr::ColumnarValue::Scalar(_)));
            let row_count = if all_scalar {
                1
            } else {
                invocation.number_rows
            };
            let scalar_keys = args.iter().enumerate().filter_map(|(index, value)| {
                match value {
                    datafusion_expr::ColumnarValue::Scalar(value) => Some((index, value.clone())),
                    _ => None,
                }
            }).collect::<Vec<_>>();
            let mut prepared = if scalar_keys.is_empty() { None } else {
                Some(prepared.lock().map_err(|_| {
                    datafusion_common::DataFusionError::Internal("scalar kernel preparation poisoned".into())
                })?)
            };
            let cached = prepared.as_ref().and_then(|guard| guard.as_ref()).filter(|(keys, _, _)| {
                keys.len() == scalar_keys.len() && keys.iter().zip(&scalar_keys)
                    .all(|((left_index, left), (right_index, right))|
                        left_index == right_index && same_scalar_binding(left, right))
            }).map(|(_, program, schema)| (program.clone(), schema.clone()));
            let mut arrays = vec![];
            let mut fields = vec![];
            let mut scalars = std::collections::HashMap::new();
            for (value, field) in args.iter().zip(kernel_schema.fields()) {
                match value {
                    datafusion_expr::ColumnarValue::Scalar(_) if cached.is_some() => {},
                    datafusion_expr::ColumnarValue::Scalar(value) => {
                        let scalar = restore_arrow_array_type(
                            &value.to_array_of_size(1)?,
                            field.data_type(),
                        )?;
                        scalars.insert(
                            field.name().clone(),
                            crate::expressions::scalar_at(&scalar, 0).map_err(|error| {
                                datafusion_common::DataFusionError::Execution(error.to_string())
                            })?,
                        );
                    }
                    datafusion_expr::ColumnarValue::Array(array) => {
                        if array.len() != row_count {
                            return Err(datafusion_common::DataFusionError::Execution(
                                "native kernel capture row counts differ".into(),
                            ));
                        }
                        arrays.push(restore_arrow_array_type(array, field.data_type())?);
                        fields.push(field.clone());
                    }
                }
            }
            // Specialize once for unchanged owned scalar bindings, rather than
            // rebuilding evaluators (and formatting nested literals) per batch.
            let (program, schema) = if let Some(cached) = cached {
                cached
            } else if scalars.is_empty() {
                (kernel.clone(), kernel_schema.clone())
            } else {
                let started=trace_preparation.then(std::time::Instant::now);
                let rewritten =
                    bind_scalar_captures(&captured_expression, &scalars, &mut Default::default())
                        .map_err(|error| {
                        datafusion_common::DataFusionError::Execution(error.to_string())
                    })?;
                let schema = Arc::new(Schema::new(fields));
                let program = crate::expressions::lower_with_options(&rewritten, &schema, &options).map_err(|error| {
                    datafusion_common::DataFusionError::Execution(error.to_string())
                })?;
                if let Some(started)=started {
                    eprintln!("VAIL_SEMANTIC_REPREP label={} rows={} slots={:?} micros={}",preparation_label,row_count,scalar_keys.iter().map(|(index,_)|*index).collect::<Vec<_>>(),started.elapsed().as_micros());
                }
                (program, schema)
            };
            if let Some(prepared) = prepared.as_mut() {
                **prepared = Some((scalar_keys, program.clone(), schema.clone()));
            }
            drop(prepared);
            let batch = arrow_array::RecordBatch::try_new_with_options(
                schema,
                arrays,
                &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
            )?;
            let evaluated = program.evaluate(&batch)?;
            let scalar_output =
                all_scalar || matches!(evaluated, datafusion_expr::ColumnarValue::Scalar(_));
            let output = evaluated.into_array(if scalar_output { 1 } else { row_count })?;
            let output = restore_arrow_array_type(&output, &expected_output_type)?;
            if scalar_output {
                Ok(datafusion_expr::ColumnarValue::Scalar(
                    crate::expressions::scalar_at(&output, 0).map_err(|error| {
                        datafusion_common::DataFusionError::Execution(error.to_string())
                    })?,
                ))
            } else {
                Ok(datafusion_expr::ColumnarValue::Array(output))
            }
        }),
        owner_options,
    );
    Ok(Some(udf.call(logical_args)))
}

pub(crate) fn kernel_label(expr: &Expr) -> &'static str {
    match expr {
        Expr::Switch { .. } => "switch",
        Expr::Die { .. } => "die",
        Expr::ArrayFold { scan: true, .. } => "array_scan",
        Expr::ArrayFold { scan: false, .. } => "array_fold",
        Expr::ArrayMap { .. } => "array_transform",
        Expr::ArrayPredicate { first: true, .. } => "array_find",
        Expr::ArrayPredicate { first: false, .. } => "array_any",
        Expr::ArrayFilter { .. } => "array_filter",
        Expr::ArrayGrouped { .. } => "array_grouped",
        Expr::Interval { .. } => "interval",
        Expr::IntervalContains { .. } => "interval_contains",
        Expr::IntervalOverlaps { .. } => "interval_overlaps",
        Expr::ArrayIndex { .. } => "array_index",
        Expr::ArraySlice { .. } => "array_slice",
        Expr::ArrayReduce { .. } => "array_reduce",
        Expr::Locus { .. } => "locus",
        Expr::LocusOp { operation, .. } => match operation.as_str() {
            "contig" => "locus_contig",
            "position" => "locus_position",
            "global_position" => "locus_global_position",
            _ => "locus_operation",
        },
        Expr::LocusValid { .. } => "locus_valid",
        Expr::LocusFromGlobalPosition { .. } => "locus_from_global_position",
        Expr::ParseLocus { .. } => "parse_locus",
        Expr::ParseCsvLine { .. } => "parse_csv_line",
        Expr::ToString { .. } => "hail_to_string",
        Expr::Struct { fields } if fields.is_empty() => "empty_struct",
        Expr::GetField { .. } => "get_field",
        Expr::Binary { operator, .. } => match operator {
            BinaryOperator::Floordiv => "floor_divide",
            BinaryOperator::Mod => "modulo",
            BinaryOperator::Pow => "power",
            BinaryOperator::Add => "integer_add",
            BinaryOperator::Sub => "integer_subtract",
            BinaryOperator::Mul => "integer_multiply",
            _ => "binary_operation",
        },
        _ => "hail_operation",
    }
}

fn binary_operator(op: BinaryOperator) -> Result<Operator, ExprError> {
    Ok(match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Eq => Operator::Eq,
        BinaryOperator::Ne => Operator::NotEq,
        BinaryOperator::Lt => Operator::Lt,
        BinaryOperator::Le => Operator::LtEq,
        BinaryOperator::Gt => Operator::Gt,
        BinaryOperator::Ge => Operator::GtEq,
        BinaryOperator::And => Operator::And,
        BinaryOperator::Or => Operator::Or,
        BinaryOperator::Floordiv | BinaryOperator::Mod | BinaryOperator::Pow => {
            return Err(ExprError::Invalid(format!(
                "operator {op:?} needs a Vail logical kernel"
            )));
        }
    })
}

fn logical_promote(left: &Type, right: &Type) -> Result<Type, ExprError> {
    if left == right {
        return Ok(left.clone());
    }
    let rank = |ty: &Type| match ty {
        Type::Primitive(PrimitiveType::Int32) => Some(0),
        Type::Primitive(PrimitiveType::Int64) => Some(1),
        Type::Primitive(PrimitiveType::Float32) => Some(2),
        Type::Primitive(PrimitiveType::Float64) => Some(3),
        _ => None,
    };
    match (rank(left), rank(right)) {
        (Some(a), Some(b)) => Ok(match a.max(b) {
            0 => Type::Primitive(PrimitiveType::Int32),
            1 => Type::Primitive(PrimitiveType::Int64),
            2 => Type::Primitive(PrimitiveType::Float32),
            _ => Type::Primitive(PrimitiveType::Float64),
        }),
        _ => Err(ExprError::Invalid(format!(
            "incompatible logical comparison types {left:?} and {right:?}"
        ))),
    }
}

fn literal_scalar(value: &serde_json::Value, dtype: &Type) -> Result<ScalarValue, ExprError> {
    let arr = crate::expressions::literal_array(value, dtype, 1)?;
    crate::expressions::scalar_at(&arr, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use serde_json::json;

    fn schema() -> DFSchema {
        DFSchema::try_from(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Int64, true),
        ]))
        .unwrap()
    }

    #[test]
    fn ordinary_binary_expression_stays_visible_to_optimizer() {
        let expr = Expr::Binary {
            operator: BinaryOperator::Add,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Float64),
                value: json!(4.0),
            }),
        };
        let lowered = lower_logical(&expr, &schema()).unwrap();
        let mut saw_column = false;
        let mut saw_literal = false;
        lowered
            .apply(|node| {
                match node {
                    LogicalExpr::Column(_) => saw_column = true,
                    LogicalExpr::Literal(_, _) => saw_literal = true,
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert!(matches!(lowered, LogicalExpr::BinaryExpr(_)));
        assert!(saw_column && saw_literal);
    }

    #[test]
    fn equality_remains_a_native_binary_expression() {
        let schema = schema();
        let expr = Expr::Binary {
            operator: BinaryOperator::Eq,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Float64),
                value: serde_json::json!(1.0),
            }),
        };
        let lowered = lower_logical(&expr, &schema).unwrap();
        assert!(matches!(lowered, LogicalExpr::BinaryExpr(_)));
    }

    #[test]
    fn set_equality_reports_missing_logical_collection_support() {
        let schema = DFSchema::empty();
        let set = Expr::Literal {
            dtype: Type::Set {
                set: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
            value: json!([1]),
        };
        let expr = Expr::Binary {
            operator: BinaryOperator::Eq,
            left: Arc::new(set.clone()),
            right: Arc::new(set),
        };
        assert!(matches!(
            lower_logical(&expr, &schema),
            Err(ExprError::Unsupported(message)) if message.contains("Set/Dictionary equality")
        ));
    }

    #[test]
    fn type_restoration_constant_folds_without_erasing_nested_metadata() {
        use datafusion_expr::simplify::SimplifyContext;
        use datafusion_optimizer::simplify_expressions::ExprSimplifier;
        let element = Type::Primitive(PrimitiveType::Int32);
        let plain = Type::Array {
            array: Box::new(element.clone()),
        };
        let tagged = Type::Set {
            set: Box::new(element),
        }
        .to_arrow()
        .unwrap();
        let input = crate::expressions::literal_array(&json!([1, 2]), &plain, 1).unwrap();
        let value = crate::expressions::scalar_at(&input, 0).unwrap();
        let restored = restore_type(lit(value), tagged.clone());
        let schema = Arc::new(DFSchema::empty());
        let context = SimplifyContext::builder()
            .with_schema(schema.clone())
            .build();
        let simplified = ExprSimplifier::new(context).simplify(restored).unwrap();
        let LogicalExpr::Literal(value, _) = simplified else {
            panic!("pure restoration should constant fold")
        };
        assert_eq!(value.data_type(), tagged);
        assert_eq!(
            Type::from_arrow(&value.data_type()).unwrap(),
            Type::Set {
                set: Box::new(Type::Primitive(PrimitiveType::Int32))
            }
        );
        assert_eq!(value.to_array_of_size(3).unwrap().len(), 3);
    }

    #[test]
    fn array_map_fallback_is_named_for_the_semantic_operation() {
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]))
        .unwrap();
        let expr = Expr::ArrayMap {
            value: Arc::new(Expr::Column { name: "xs".into() }),
            parameter: "x".into(),
            body: Arc::new(Expr::Binary {
                operator: BinaryOperator::Add,
                left: Arc::new(Expr::LambdaVariable {
                    name: "x".into(),
                    dtype: None,
                }),
                right: Arc::new(Expr::Literal {
                    dtype: Type::Primitive(PrimitiveType::Int64),
                    value: json!(1),
                }),
            }),
        };
        let LogicalExpr::ScalarFunction(call) = lower_logical(&expr, &schema).unwrap() else {
            panic!("expected explicit array-map kernel")
        };
        assert!(
            call.func
                .name()
                .starts_with("vail_semantic_kernel_array_transform_")
        );
    }

    #[test]
    fn integer_negation_wraps_identically_for_scalar_array_and_null() {
        let dtype = DataType::Int64;
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", dtype.clone(), true)])).unwrap();
        let input = Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(9), None]));
        let batch =
            RecordBatch::try_from_iter(vec![("x", input as arrow_array::ArrayRef)]).unwrap();
        let column = Expr::Unary {
            operator: UnaryOperator::Neg,
            value: Arc::new(Expr::Column { name: "x".into() }),
        };
        let result = evaluate_logical(lower_logical(&column, &schema).unwrap(), &schema, &batch);
        let values = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), i64::MIN);
        assert_eq!(values.value(1), -9);
        assert!(values.is_null(2));
        let scalar = Expr::Unary {
            operator: UnaryOperator::Neg,
            value: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Int64(Some(i64::MIN)),
            }),
        };
        let empty = DFSchema::empty();
        let one = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let logical = evaluate_logical(lower_logical(&scalar, &empty).unwrap(), &empty, &one);
        let native = crate::expressions::lower(&scalar, &Schema::empty())
            .unwrap()
            .evaluate(&one)
            .unwrap()
            .into_array(1)
            .unwrap();
        assert_eq!(
            logical
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MIN
        );
        assert_eq!(logical.to_data(), native.to_data());
    }

    #[test]
    fn if_else_keeps_null_condition_branch_explicit() {
        let expr = Expr::IfElse {
            condition: Arc::new(Expr::Column {
                name: "flag".into(),
            }),
            consequent: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(1),
            }),
            alternate: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(0),
            }),
        };
        // A separate Boolean source is required by the typed IR.
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]))
        .unwrap();
        let lowered = lower_logical(&expr, &schema).unwrap();
        let LogicalExpr::Case(case) = lowered else {
            panic!("expected a CASE expression")
        };
        assert_eq!(case.when_then_expr.len(), 2);
    }

    #[test]
    fn scalar_capture_udf_reuses_semantics_after_binding_changes_and_parallel_batches() {
        use arrow_array::{ArrayRef, Int64Array, ListArray};
        use arrow_buffer::OffsetBuffer;
        use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
        let int64 = Type::Primitive(PrimitiveType::Int64);
        let array_type = Type::Array { array: Box::new(int64.clone()) };
        let arrow_array_type = array_type.to_arrow().unwrap();
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("items", arrow_array_type.clone(), true),
            Field::new("shift", DataType::Int64, true),
        ])).unwrap();
        let expression = Expr::ArrayMap {
            value: Arc::new(Expr::Column { name: "items".into() }),
            parameter: "element".into(),
            body: Arc::new(Expr::Binary {
                operator: BinaryOperator::Add,
                left: Arc::new(Expr::LambdaVariable { name: "element".into(), dtype: None }),
                right: Arc::new(Expr::Column { name: "shift".into() }),
            }),
        };
        // Use the normal public lowering API to obtain the executable UDF,
        // then exercise its public batch interface with different captures.
        let LogicalExpr::ScalarFunction(function) = lower_logical(&expression, &schema).unwrap()
            else { panic!("array map must be executable as a scalar function"); };
        let scalar = |values: Vec<i64>| ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::from_lengths([values.len()]), Arc::new(Int64Array::from(values)), None,
        )));
        let invoke = |items: ScalarValue, shifts: Vec<i64>| {
            let rows = shifts.len();
            let shifts: ArrayRef = Arc::new(Int64Array::from(shifts));
            let mut args = Vec::new();
            let mut arg_fields = Vec::new();
            for argument in &function.args {
                let LogicalExpr::Column(column) = argument else { panic!("column argument required"); };
                let field = schema.field_with_unqualified_name(&column.name).unwrap().clone();
                arg_fields.push(field);
                args.push(if column.name == "items" {
                    ColumnarValue::Scalar(items.clone())
                } else { ColumnarValue::Array(shifts.clone()) });
            }
            let output = function.func.invoke_with_args(ScalarFunctionArgs {
                args, arg_fields, number_rows: rows,
                return_field: Arc::new(Field::new("result", arrow_array_type.clone(), true)),
                config_options: Arc::new(Default::default()),
            }).unwrap().into_array(rows).unwrap();
            let output = output.as_any().downcast_ref::<ListArray>().unwrap();
            (0..rows).map(|row| {
                let value = output.value(row);
                value.as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
            }).collect::<Vec<_>>()
        };
        let original = scalar(vec![1, 2]);
        assert_eq!(invoke(original.clone(), vec![3, 4]), vec![vec![4, 5], vec![5, 6]]);
        let changed = scalar(vec![10, 20]);
        assert_eq!(invoke(changed.clone(), vec![3]), vec![vec![13, 23]]);
        assert_eq!(invoke(scalar(vec![]), vec![8, 9]), vec![Vec::<i64>::new(), Vec::<i64>::new()]);
        std::thread::scope(|scope| {
            let handles = (0..4).map(|shift| {
                let invoke = &invoke;
                let changed = changed.clone();
                scope.spawn(move || invoke(changed, vec![shift, shift + 1]))
            }).collect::<Vec<_>>();
            for (shift, handle) in handles.into_iter().enumerate() {
                let shift = shift as i64;
                assert_eq!(handle.join().unwrap(),
                    vec![vec![10 + shift, 20 + shift], vec![11 + shift, 21 + shift]]);
            }
        });
        assert_eq!(invoke(original, vec![0]), vec![vec![1, 2]]);
    }

    fn evaluate_logical(
        expr: LogicalExpr,
        schema: &DFSchema,
        batch: &RecordBatch,
    ) -> arrow_array::ArrayRef {
        let physical = datafusion_physical_expr::planner::create_physical_expr(
            &expr,
            schema,
            &datafusion_physical_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap();
        physical
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap()
    }

    #[test]
    fn invalid_locus_coordinate_is_deferred_until_selected() {
        use crate::reference::ReferenceDefinition;
        use datafusion_expr::simplify::SimplifyContext;
        use datafusion_optimizer::simplify_expressions::ExprSimplifier;
        let reference = ReferenceDefinition {
            name: "small".into(),
            contigs: vec!["1".into()],
            lengths: BTreeMap::from([("1".into(), 10)]),
            x_contigs: vec![],
            y_contigs: vec![],
            mt_contigs: vec![],
            par: vec![],
        };
        let DataType::Struct(fields) = (Type::Locus { locus: reference }).to_arrow().unwrap()
        else {
            unreachable!()
        };
        // Valid Arrow layout carrying an invalid coordinate, as may arrive from
        // imported storage. The accessor must retain runtime validation.
        let locus = arrow_array::StructArray::try_new(
            fields,
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![0])),
                Arc::new(arrow_array::StringArray::from(vec!["1"])),
                Arc::new(arrow_array::Int32Array::from(vec![99])),
            ],
            None,
        )
        .unwrap();
        let accessor = Expr::LocusOp {
            value: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Struct(Arc::new(locus)),
            }),
            operation: "position".into(),
        };
        let schema = DFSchema::empty();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let conditional = Expr::IfElse {
            condition: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Boolean(Some(false)),
            }),
            consequent: Arc::new(accessor.clone()),
            alternate: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Int32(Some(9)),
            }),
        };
        let context = SimplifyContext::builder()
            .with_schema(Arc::new(schema.clone()))
            .build();
        let simplified = ExprSimplifier::new(context)
            .simplify(lower_logical(&conditional, &schema).unwrap())
            .unwrap();
        let result = evaluate_logical(simplified, &schema, &batch);
        assert_eq!(result.as_ref(), &arrow_array::Int32Array::from(vec![9]));
        let physical = datafusion_physical_expr::planner::create_physical_expr(
            &lower_logical(&accessor, &schema).unwrap(),
            &schema,
            &datafusion_physical_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap();
        assert!(physical.evaluate(&batch).is_err());
    }

    #[test]
    fn independent_annotations_execute_with_distinct_values() {
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::Int64, false)])).unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1, 2])) as _)])
                .unwrap();
        let annotate = |n| Expr::Binary {
            operator: BinaryOperator::Add,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(n),
            }),
        };
        let first = evaluate_logical(
            lower_logical(&annotate(1), &schema).unwrap(),
            &schema,
            &batch,
        );
        let second = evaluate_logical(
            lower_logical(&annotate(2), &schema).unwrap(),
            &schema,
            &batch,
        );
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 3]
        );
        assert_eq!(
            second
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 4]
        );
    }

    #[test]
    fn integer_division_and_wrapping_keep_vail_semantics() {
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::Int64, false)])).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from(vec![3, i64::MAX])) as _,
        )])
        .unwrap();
        let division = Expr::Binary {
            operator: BinaryOperator::Div,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(2),
            }),
        };
        let divided = evaluate_logical(lower_logical(&division, &schema).unwrap(), &schema, &batch);
        let divided = divided.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(divided.value(0), 1.5);
        assert_eq!(divided.value(1), i64::MAX as f64 / 2.0);

        let addition = Expr::Binary {
            operator: BinaryOperator::Add,
            left: Arc::new(Expr::Column { name: "x".into() }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(1),
            }),
        };
        let added = evaluate_logical(lower_logical(&addition, &schema).unwrap(), &schema, &batch);
        assert_eq!(
            added
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            i64::MIN
        );
    }

    #[test]
    fn missing_if_condition_produces_missing_and_dead_die_branch_is_not_evaluated() {
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]))
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "flag",
            Arc::new(BooleanArray::from(vec![Some(false), None])) as _,
        )])
        .unwrap();
        let die = |message| Expr::Die {
            message: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Str),
                value: json!(message),
            }),
            dtype: Type::Primitive(PrimitiveType::Int64),
        };
        let expr = Expr::IfElse {
            condition: Arc::new(Expr::Column {
                name: "flag".into(),
            }),
            consequent: Arc::new(die("unselected")),
            alternate: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(9),
            }),
        };
        let out = evaluate_logical(lower_logical(&expr, &schema).unwrap(), &schema, &batch);
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 9);
        assert!(out.is_null(1));
    }
}
