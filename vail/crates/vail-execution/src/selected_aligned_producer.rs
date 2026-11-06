//! Fuse a native list producer with typed, row-selected field consumers.
//!
//! Original producer arguments retain their original row/entry/scalar domains.
//! The selected bundle is an explicit invocation-local input to the consumer.
use crate::{
    expressions::{Expr, PreparationOptions, RebuiltExpr, Type},
    logical_expr::{
        AlignedSelection, PreparedCapture, SelectedAlignedExprProof, SelectedAlignedProgram,
    },
    selected_list_producer::{Argument, Request, SourceDomain, capability},
};
use arrow_array::{
    Array, Int32Array, ListArray, RecordBatch, RecordBatchOptions, StructArray, make_array,
};
use arrow_buffer::{NullBuffer, NullBufferBuilder};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDF};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

const SOURCE: &str = "__vail_original_argument";

fn field(value: Expr, name: &str) -> Expr {
    Expr::GetField {
        value: Arc::new(value),
        name: name.into(),
    }
}
fn entry_path(expression: &Expr) -> Option<Vec<String>> {
    let mut expression = expression;
    let mut path = Vec::new();
    while let Expr::GetField { value, name } = expression {
        path.push(name.clone());
        expression = value;
    }
    if !matches!(expression, Expr::AlignedMapInput { axis, .. } if axis == "entry") {
        return None;
    }
    path.reverse();
    Some(path)
}
fn map_shared(expression: &Expr, change: &mut impl FnMut(&Expr) -> Option<Expr>) -> Expr {
    fn walk(
        expression: &Expr,
        shared: bool,
        change: &mut impl FnMut(&Expr) -> Option<Expr>,
        memo: &mut HashMap<usize, Arc<Expr>>,
    ) -> RebuiltExpr {
        let key = expression as *const Expr as usize;
        if shared {
            if let Some(value) = memo.get(&key) {
                return RebuiltExpr::Shared(value.clone());
            }
        }
        let value = change(expression).unwrap_or_else(|| {
            expression
                .try_map_children_rebuilt::<std::convert::Infallible>(|child, shared| {
                    Ok(walk(child, shared, change, memo))
                })
                .unwrap()
        });
        if shared {
            let value = Arc::new(value);
            memo.insert(key, value.clone());
            RebuiltExpr::Shared(value)
        } else {
            RebuiltExpr::Owned(value)
        }
    }
    walk(expression, false, change, &mut HashMap::new()).into_owned()
}

#[derive(Debug)]
struct Reader {
    domain: SourceDomain,
    schema: Arc<Schema>,
    program: Arc<dyn PhysicalExpr>,
    nodes: usize,
}
impl Reader {
    fn prepare(
        expression: &Expr,
        roots: [&DataType; 3],
        options: &PreparationOptions,
        allow_arithmetic: bool,
    ) -> Option<Self> {
        let mut domains = HashSet::new();
        let mut supported = true;
        let mut nodes = 0usize;
        expression.visit_unique(&mut |node| {
            nodes += 1;
            match node {
                Expr::AlignedMapInput { axis, .. } => {
                    if let Some(domain) = match axis.as_str() {
                        "entry" => Some(SourceDomain::OriginalEntry),
                        "row" => Some(SourceDomain::OriginalRow),
                        "global" => Some(SourceDomain::Scalar),
                        _ => None,
                    } {
                        domains.insert(domain);
                    } else {
                        supported = false;
                    }
                }
                Expr::GetField { .. }
                | Expr::ArrayLength { .. }
                | Expr::Literal { .. }
                | Expr::RuntimeLiteral { .. } => {}
                Expr::Binary { operator, .. } | Expr::IntegerBinary { operator, .. }
                    if allow_arithmetic
                        && matches!(
                            operator,
                            crate::expressions::BinaryOperator::Add
                                | crate::expressions::BinaryOperator::Sub
                        ) => {}
                _ => supported = false,
            }
        });
        // Deliberately narrow: no errors, effects, arbitrary callbacks, or
        // conversion of one array domain into another during argument binding.
        if !supported || domains.len() > 1 {
            return None;
        }
        let domain = domains.into_iter().next().unwrap_or(SourceDomain::Scalar);
        let root = roots[match domain {
            SourceDomain::OriginalEntry => 0,
            SourceDomain::OriginalRow => 1,
            SourceDomain::Scalar => 2,
        }];
        let rewritten = map_shared(expression, &mut |node| {
            let Expr::AlignedMapInput { axis, name, .. } = node else {
                return None;
            };
            let value = Expr::Column {
                name: SOURCE.into(),
            };
            Some(match axis.as_str() {
                "entry" => value,
                "row" => field(field(value, "__vail_aligned_row"), name.as_deref()?),
                "global" => field(value, name.as_deref()?),
                _ => return None,
            })
        });
        let has_root = {
            let mut found = false;
            rewritten.visit_unique(&mut |node| found |= matches!(node, Expr::Column { .. }));
            found
        };
        let schema = Arc::new(Schema::new(if has_root {
            vec![Field::new(SOURCE, root.clone(), true)]
        } else {
            vec![]
        }));
        let program = crate::expressions::lower_with_options(&rewritten, &schema, options).ok()?;
        Some(Self {
            domain,
            schema,
            program,
            nodes,
        })
    }
    fn evaluate(&self, root: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        let scalar = matches!(root, ColumnarValue::Scalar(_));
        let count = if scalar { 1 } else { rows };
        let columns = if self.schema.fields().is_empty() {
            vec![]
        } else {
            vec![root.into_array(count)?]
        };
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?;
        let output = self.program.evaluate(&batch)?;
        if scalar {
            let array = output.into_array(1)?;
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&array, 0)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?,
            ))
        } else {
            Ok(output)
        }
    }
}

/// One raw owner's explicit argument definitions and inherited validity.
#[derive(Debug)]
pub struct PreparedArguments {
    values: Vec<Argument>,
    entry_validity: Option<NullBuffer>,
}

/// Query-prepared alternate consumer and original-domain native arguments.
#[derive(Debug)]
pub struct SelectedProducerProgram {
    function: Arc<ScalarUDF>,
    arguments: Vec<Reader>,
    original_captures: [PreparedCapture; 2],
    selector: Reader,
    fields: u64,
    bundle_type: DataType,
    raw_type: DataType,
    value_index: usize,
    consumer: SelectedAlignedProgram,
}
impl SelectedProducerProgram {
    pub fn prepare(
        function: &ScalarUDF,
        proof: &SelectedAlignedExprProof<'_>,
        options: &PreparationOptions,
    ) -> Option<Self> {
        let trace = std::env::var_os("VAIL_TRACE_SELECTED_LIST_PRODUCER").is_some();
        macro_rules! required {
            ($value:expr,$stage:literal) => {
                match $value {
                    Some(value) => value,
                    None => {
                        if trace {
                            eprintln!("VAIL_SELECTED_PRODUCER_REJECT stage={}", $stage);
                        }
                        return None;
                    }
                }
            };
        }
        required!(
            crate::logical_expr::deferrable_aligned_entry_argument(function),
            "producer_certificate"
        );
        let (producer, producer_schema) = crate::logical_expr::semantic_kernel(function)?;
        let Expr::AlignedMatrixMap {
            entries,
            rows,
            globals,
            body,
            preserve_cell: true,
            ..
        } = producer
        else {
            return None;
        };
        let Expr::Column { name } = entries.as_ref() else {
            return None;
        };
        let raw_type = producer_schema
            .field(producer_schema.index_of(name).ok()?)
            .data_type()
            .clone();
        let DataType::List(raw_item) = &raw_type else {
            return None;
        };
        let DataType::Struct(raw_fields) = raw_item.data_type() else {
            return None;
        };
        let value_index = raw_fields
            .iter()
            .position(|field| field.name() == "value")?;
        let Expr::IfElse {
            condition,
            consequent,
            alternate,
        } = body.as_ref()
        else {
            return None;
        };
        if !matches!(alternate.as_ref(),Expr::Literal{value,..} if value.is_null())
            && !matches!(alternate.as_ref(),Expr::RuntimeLiteral{value} if value.is_null())
        {
            return None;
        }
        if !matches!(condition.as_ref(),Expr::AlignedMapInput{axis,..}if axis=="entry_present") {
            return None;
        }
        let Expr::Struct {
            fields: producer_fields,
        } = consequent.as_ref()
        else {
            return None;
        };
        let mut native = None;
        for (name, expression) in producer_fields {
            if let Expr::StringFunction { function, args } = expression {
                let Ok((function, _, _)) = crate::expressions::string_function(function) else {
                    continue;
                };
                if capability(&function).is_some() {
                    if native.is_some() {
                        return None;
                    }
                    native = Some((name, function, args));
                }
            }
        }
        let (native_field, function, native_arguments) = required!(native, "native_field");
        if !crate::indexed_batch::ordinary_entry_reads(
            producer_fields
                .iter()
                .filter(|(name, _)| name != native_field)
                .map(|(_, expression)| expression),
        ) {
            return None;
        }
        let native_type = function
            .return_type(
                &native_arguments
                    .iter()
                    .map(|argument| argument.dtype(producer_schema).ok()?.to_arrow().ok())
                    .collect::<Option<Vec<_>>>()?,
            )
            .ok()?;
        let DataType::Struct(native_fields) = &native_type else {
            return None;
        };
        if native_fields.len() > 64 {
            return None;
        }
        let Expr::AlignedMatrixMap {
            body: consumer_body,
            ..
        } = proof.original()
        else {
            return None;
        };
        let Expr::IfElse { consequent, .. } = consumer_body.as_ref() else {
            return None;
        };
        let Expr::Struct {
            fields: consumer_fields,
        } = consequent.as_ref()
        else {
            return None;
        };
        let mut fields = 0u64;
        let mut selector: Option<Arc<Expr>> = None;
        let mut selector_identity = None;
        let mut identities = HashMap::<usize, crate::expression_identity::Identity>::new();
        let mut supported = true;
        for (_, expression) in consumer_fields
            .iter()
            .filter(|(name, _)| proof.retains(name))
        {
            expression.visit_unique(&mut |node| {
                let Expr::ArrayIndex { value, index } = node else {
                    return;
                };
                let Some(path) = entry_path(value) else {
                    return;
                };
                if path.len() != 3 || path[0] != "value" || path[1] != *native_field {
                    return;
                }
                let Some(ordinal) = native_fields
                    .iter()
                    .position(|field| field.name() == &path[2])
                else {
                    supported = false;
                    return;
                };
                if let Some(previous) = &selector {
                    if !Arc::ptr_eq(previous, index) {
                        let identity = identities
                            .entry(Arc::as_ptr(index) as usize)
                            .or_insert_with(|| {
                                crate::expression_identity::Identity::new(index.as_ref().clone())
                            });
                        if selector_identity.as_ref() != Some(identity) {
                            supported = false;
                        }
                    }
                } else {
                    let identity =
                        crate::expression_identity::Identity::new(index.as_ref().clone());
                    identities.insert(Arc::as_ptr(index) as usize, identity.clone());
                    selector_identity = Some(identity);
                    selector = Some(index.clone());
                }
                fields |= 1u64 << ordinal;
            });
        }
        if !supported || fields == 0 {
            return None;
        }
        let bundle_type = required!(capability(&function)?.selected_type(fields), "field_bundle");
        let bundle_logical = Type::from_arrow(&bundle_type).ok()?;
        let DataType::Struct(bundle_fields) = &bundle_type else {
            return None;
        };
        let selector = selector?;
        let mut row_only = true;
        selector.visit_unique(&mut |node| match node {
            Expr::AlignedMapInput { axis, .. } if axis == "row" => {}
            Expr::GetField { .. }
            | Expr::Binary { .. }
            | Expr::IntegerBinary { .. }
            | Expr::Literal { .. }
            | Expr::RuntimeLiteral { .. } => {}
            _ => row_only = false,
        });
        if !row_only || !crate::logical_expr::can_defer_selected_body(&selector) {
            return None;
        }
        let mut failed = false;
        let mut rewrite = |node: &Expr| {
            if let Expr::ArrayIndex { value, .. } = node {
                if let Some(path) = entry_path(value) {
                    if path.len() == 3 && path[0] == "value" && path[1] == *native_field {
                        let ordinal = native_fields
                            .iter()
                            .position(|field| field.name() == &path[2])?;
                        let bundle_ordinal = (0..ordinal)
                            .filter(|bit| fields & (1u64 << bit) != 0)
                            .count();
                        return Some(field(
                            Expr::AlignedMapInput {
                                axis: "selected_entry".into(),
                                name: None,
                                dtype: bundle_logical.clone(),
                            },
                            bundle_fields.get(bundle_ordinal)?.name(),
                        ));
                    }
                }
            }
            if let Some(path) = entry_path(node) {
                if path.len() < 2 || path[0] != "value" {
                    failed = true;
                    return None;
                }
                let Some((_, source)) = producer_fields.iter().find(|(name, _)| name == &path[1])
                else {
                    failed = true;
                    return None;
                };
                if path[1] == *native_field {
                    failed = true;
                    return None;
                }
                let mut source = source.clone();
                for name in &path[2..] {
                    source = field(source, name);
                }
                return Some(source);
            }
            None
        };
        let rewritten_fields = consumer_fields
            .iter()
            .filter(|(name, _)| proof.retains(name))
            .map(|(name, expression)| (name.clone(), map_shared(expression, &mut rewrite)))
            .collect::<Vec<_>>();
        if failed {
            return None;
        }
        let mut rewritten = proof.original().clone();
        let Type::Array {
            array: original_cell,
        } = proof.original().dtype(proof.schema()).ok()?
        else {
            return None;
        };
        let Type::Struct {
            r#struct: original_cell_fields,
        } = original_cell.as_ref()
        else {
            return None;
        };
        let Type::Struct {
            r#struct: original_value_fields,
        } = &original_cell_fields
            .iter()
            .find(|(name, _)| name == "value")?
            .1
        else {
            return None;
        };
        let selected_value_type = Type::Struct {
            r#struct: original_value_fields
                .iter()
                .filter(|(name, _)| proof.retains(name))
                .cloned()
                .collect(),
        };
        let Expr::AlignedMatrixMap { body, dtype, .. } = &mut rewritten else {
            unreachable!()
        };
        let Expr::IfElse {
            consequent,
            alternate,
            ..
        } = Arc::make_mut(body)
        else {
            unreachable!()
        };
        *consequent = Arc::new(Expr::Struct {
            fields: rewritten_fields,
        });
        *alternate = Arc::new(Expr::Literal {
            dtype: selected_value_type.clone(),
            value: serde_json::Value::Null,
        });
        *dtype = Type::Array {
            array: Box::new(Type::Struct {
                r#struct: vec![("value".into(), selected_value_type)],
            }),
        };
        let mut consumer_fields = proof.schema().fields().to_vec();
        let entry_argument = proof.entry_argument();
        consumer_fields[entry_argument] = Arc::new(
            consumer_fields[entry_argument]
                .as_ref()
                .clone()
                .with_data_type(raw_type.clone()),
        );
        let raw_schema =
            Schema::new_with_metadata(consumer_fields, proof.schema().metadata().clone());
        let consumer = required!(
            proof.prepare_rewritten(&rewritten, &raw_schema),
            "rewritten_consumer"
        );
        let root_types = [
            raw_item.data_type().clone(),
            rows.dtype(producer_schema).ok()?.to_arrow().ok()?,
            globals.dtype(producer_schema).ok()?.to_arrow().ok()?,
        ];
        let arguments = required!(
            native_arguments
                .iter()
                .map(|argument| {
                    Reader::prepare(
                        argument,
                        [&root_types[0], &root_types[1], &root_types[2]],
                        options,
                        false,
                    )
                })
                .collect::<Option<Vec<_>>>(),
            "original_arguments"
        );
        let original_captures = [
            PreparedCapture::prepare(rows, producer_schema, options)?,
            PreparedCapture::prepare(globals, producer_schema, options)?,
        ];
        let Expr::AlignedMatrixMap {
            rows: consumer_rows,
            globals: consumer_globals,
            ..
        } = proof.original()
        else {
            unreachable!()
        };
        let selector = Reader::prepare(
            &selector,
            [
                &root_types[0],
                &consumer_rows.dtype(proof.schema()).ok()?.to_arrow().ok()?,
                &consumer_globals
                    .dtype(proof.schema())
                    .ok()?
                    .to_arrow()
                    .ok()?,
            ],
            options,
            true,
        )?;
        if selector.domain != SourceDomain::OriginalRow
            || selector.program.data_type(&selector.schema).ok()? != DataType::Int32
        {
            return None;
        }
        Some(Self {
            function,
            arguments,
            original_captures,
            selector,
            fields,
            bundle_type,
            raw_type,
            value_index,
            consumer,
        })
    }
    /// Estimate argument storage before evaluating any source reader, using
    /// Arrow-reported storage and 32 bytes per reader node/lane. Foreign-owner
    /// retention and final output storage are separate from this admission.
    pub fn argument_budget(&self, entries: &ListArray, budget: usize) -> Option<usize> {
        let entry_count = entries.values().len();
        let mut bytes = entries
            .values()
            .get_array_memory_size()
            .checked_add(entry_count.checked_add(7)? / 8)?;
        for reader in &self.arguments {
            let count = match reader.domain {
                SourceDomain::OriginalEntry => entry_count,
                SourceDomain::OriginalRow => entries.len(),
                SourceDomain::Scalar => 1,
            };
            bytes = bytes.checked_add(count.checked_mul(reader.nodes)?.checked_mul(32)?)?;
        }
        bytes = bytes.checked_add(
            entries
                .len()
                .checked_mul(self.selector.nodes)?
                .checked_mul(32)?,
        )?;
        budget.checked_sub(bytes)
    }
    /// Original argument expressions are evaluated once for this raw owner.
    pub fn arguments(
        &self,
        arguments: &[ColumnarValue],
        entries: &ListArray,
    ) -> Result<PreparedArguments> {
        if entries.data_type() != &self.raw_type {
            return Err(DataFusionError::Execution(
                "selected producer original descriptor differs".into(),
            ));
        }
        let row_values = self.original_captures[0].evaluate(arguments)?;
        let globals = self.original_captures[1].evaluate(arguments)?;
        let raw_cells = entries.values();
        let values = raw_cells
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("selected original cell layout differs".into())
            })?
            .column(self.value_index);
        // Packed masks hide null-list gaps before Call.alleles expansion.
        // Full, all-valid owners borrow their existing masks without a scan.
        let inherited = NullBuffer::union(raw_cells.nulls(), values.nulls());
        let offsets = entries.value_offsets();
        let entry_validity = if entries.null_count() == 0
            && offsets[0] == 0
            && offsets[entries.len()] as usize == raw_cells.len()
        {
            inherited
        } else {
            let mut valid = NullBufferBuilder::new(raw_cells.len());
            let mut cursor = 0usize;
            for row in 0..entries.len() {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                valid.append_n_nulls(start - cursor);
                if entries.is_null(row) {
                    valid.append_n_nulls(end - start);
                } else if let Some(mask) = &inherited {
                    valid.append_buffer(&mask.slice(start, end - start));
                } else {
                    valid.append_n_non_nulls(end - start);
                }
                cursor = end;
            }
            valid.append_n_nulls(raw_cells.len() - cursor);
            valid.finish()
        };
        let cells = if entry_validity.is_none() {
            raw_cells.clone()
        } else {
            make_array(
                raw_cells
                    .to_data()
                    .into_builder()
                    .nulls(entry_validity.clone())
                    .build()?,
            )
        };
        let values = self
            .arguments
            .iter()
            .map(|argument| {
                let (root, rows) = match argument.domain {
                    SourceDomain::OriginalEntry => {
                        (ColumnarValue::Array(cells.clone()), cells.len())
                    }
                    SourceDomain::OriginalRow => (row_values.clone(), entries.len()),
                    SourceDomain::Scalar => (globals.clone(), 1),
                };
                if let ColumnarValue::Array(array) = &root {
                    if array.len() != rows {
                        return Err(DataFusionError::Execution(
                            "original argument row domain differs".into(),
                        ));
                    }
                }
                let value = argument.evaluate(root, rows)?;
                let domain = if matches!(value, ColumnarValue::Scalar(_)) {
                    SourceDomain::Scalar
                } else {
                    argument.domain
                };
                Ok(Argument { value, domain })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PreparedArguments {
            values,
            entry_validity,
        })
    }
    pub fn invoke(
        &self,
        arguments: &[ColumnarValue],
        original: &PreparedArguments,
        selection: AlignedSelection,
        max_scratch_bytes: usize,
    ) -> Result<Option<ColumnarValue>> {
        let captures = self.consumer.evaluate_captures(arguments)?;
        let entries = selection
            .entries
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("selected producer source is not List".into())
            })?;
        let entry_validity = &original.entry_validity;
        // The original entry-presence guard does not demand a row selector
        // when that row has no present entries. Keep arithmetic lazy there.
        let offsets = entries.value_offsets();
        let mut presence_by_parent = HashMap::new();
        let demanded = selection
            .parents
            .iter()
            .map(|parent| {
                let Some(parent) =
                    parent.filter(|parent| *parent >= 0 && (*parent as usize) < entries.len())
                else {
                    return false;
                };
                let parent = parent as usize;
                *presence_by_parent.entry(parent).or_insert_with(|| {
                    entries.is_valid(parent)
                        && (offsets[parent] as usize..offsets[parent + 1] as usize).any(|entry| {
                            entry_validity
                                .as_ref()
                                .is_none_or(|mask| mask.is_valid(entry))
                        })
                })
            })
            .collect::<Vec<_>>();
        let selector_rows = if demanded.iter().all(|value| *value) {
            captures[1].clone()
        } else {
            // A scalar row owner must not be expanded merely to mask an
            // unused row. The original program remains the bounded fallback.
            let ColumnarValue::Array(rows) = &captures[1] else {
                return Ok(None);
            };
            let mask = NullBuffer::from(demanded);
            ColumnarValue::Array(make_array(
                rows.to_data()
                    .into_builder()
                    .nulls(NullBuffer::union(rows.nulls(), Some(&mask)))
                    .build()?,
            ))
        };
        let selectors = self
            .selector
            .evaluate(selector_rows, selection.parents.len())?
            .into_array(selection.parents.len())?;
        let selectors = selectors
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("selected producer selector is not int32".into())
            })?;
        let request = Request {
            arguments: &original.values,
            entries,
            entry_validity: entry_validity.as_ref(),
            parents: &selection.parents,
            selectors,
            fields: self.fields,
            max_scratch_bytes,
        };
        let Some(bundle) = capability(&self.function)
            .unwrap()
            .evaluate_selected(&request)?
        else {
            return Ok(None);
        };
        if bundle.data_type() != &self.bundle_type {
            return Err(DataFusionError::Execution(
                "selected producer bundle descriptor differs".into(),
            ));
        }
        self.consumer
            .invoke_captures(selection, captures, Some(&bundle))
            .map(Some)
    }
}
