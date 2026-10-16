//! Table joins use DataFusion's join operators and native typed key comparisons.
//! Missing top-level key components never match; nested missing values remain
//! part of a present key.
use crate::{
    Result, ScalarIR, TableError, TableIR, TableJoinKind, TableNode, TableRef,
    globals::{GlobalBindings, GlobalsIR, GlobalsRef},
    plan::{self, PartitionLayout, PhysicalPlan, TablePlan, column},
};
use arrow_schema::Schema;
use datafusion_common::{NullEquality, ScalarValue};
use datafusion_expr::JoinType;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{CaseExpr, IsNullExpr, Literal},
};
use datafusion_physical_plan::{joins::SortMergeJoinExec, projection::ProjectionExec};
use std::sync::Arc;
use vail_execution::expressions::Type;

impl TableIR {
    /// Physical normalization preserves logical key values and key order.
    pub(crate) fn canonical_join_keys(self: &Arc<Self>) -> Result<TableRef> {
        let mut changed = false;
        let mut fields = Vec::new();
        let mut schema = Vec::new();
        for field in self.schema().fields() {
            let column = ScalarIR::Column {
                name: field.name().clone(),
            };
            if self.keys().contains(field.name()) {
                let dtype = Type::from_arrow(field.data_type())?;
                let arrow = dtype.to_arrow()?;
                changed |= arrow != *field.data_type();
                schema.push(field.as_ref().clone().with_data_type(arrow));
                fields.push((
                    field.name().clone(),
                    ScalarIR::Cast {
                        value: Arc::new(column),
                        dtype,
                    },
                ));
            } else {
                schema.push(field.as_ref().clone());
                fields.push((field.name().clone(), column));
            }
        }
        if !changed {
            return Ok(self.clone());
        }
        Ok(Self::new(
            Arc::new(Schema::new(schema)),
            self.keys().to_vec(),
            self.globals().clone(),
            TableNode::MapRows {
                child: self.clone(),
                fields,
                bindings: GlobalBindings::new(),
            },
        ))
    }

    /// A bijective field rename preserves each key value and its order.
    pub fn rename_fields(
        self: &Arc<Self>,
        mapping: &std::collections::BTreeMap<String, String>,
    ) -> Result<TableRef> {
        let known = self
            .schema()
            .fields()
            .iter()
            .chain(self.globals().schema().fields())
            .map(|field| field.name())
            .collect::<std::collections::HashSet<_>>();
        if mapping
            .iter()
            .any(|(old, new)| !known.contains(old) || new.is_empty())
        {
            return Err(TableError::Invalid(
                "rename requires known fields and nonempty names".into(),
            ));
        }
        let renamed = |name: &str| {
            mapping
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.to_owned())
        };
        let mut names = std::collections::HashSet::new();
        if known.iter().any(|name| !names.insert(renamed(name))) {
            return Err(TableError::Invalid(
                "renamed row and global fields must have distinct names".into(),
            ));
        }
        let globals = self.globals().map(
            self.globals()
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    (
                        renamed(field.name()),
                        ScalarIR::Global {
                            name: field.name().clone(),
                        },
                    )
                })
                .collect(),
        )?;
        let fields = self
            .schema()
            .fields()
            .iter()
            .map(|field| {
                (
                    renamed(field.name()),
                    ScalarIR::Column {
                        name: field.name().clone(),
                    },
                )
            })
            .collect();
        let schema = Arc::new(Schema::new(
            self.schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_name(renamed(field.name())))
                .collect::<Vec<_>>(),
        ));
        Ok(Self::new(
            schema,
            self.keys().iter().map(|key| renamed(key)).collect(),
            globals,
            TableNode::MapRows {
                child: self.clone(),
                fields,
                bindings: GlobalBindings::new(),
            },
        ))
    }

    pub fn join(
        self: &Arc<Self>,
        right: TableRef,
        kind: TableJoinKind,
        join_key: usize,
    ) -> Result<TableRef> {
        let left = self.canonical_join_keys()?;
        let right = right.canonical_join_keys()?;
        let semi = matches!(kind, TableJoinKind::Semi | TableJoinKind::Anti);
        if join_key > left.keys().len() || join_key > right.keys().len() {
            return Err(TableError::Invalid(
                "join key exceeds input key length".into(),
            ));
        }
        if semi && (join_key == 0 || join_key != right.keys().len()) {
            return Err(TableError::Invalid(
                "membership join requires the entire nonempty right key".into(),
            ));
        }
        for (left_name, right_key) in left.keys()[..join_key]
            .iter()
            .zip(&right.keys()[..join_key])
        {
            if Type::from_arrow(left.schema().field_with_name(left_name)?.data_type())?
                != Type::from_arrow(right.schema().field_with_name(right_key)?.data_type())?
            {
                return Err(TableError::Invalid("join key type mismatch".into()));
            }
        }
        let (schema, keys, globals) = if semi {
            (
                left.schema().clone(),
                left.keys().to_vec(),
                left.globals().clone(),
            )
        } else {
            let prefix = &left.keys()[..join_key];
            let mut fields = prefix
                .iter()
                .map(|name| {
                    left.schema()
                        .field_with_name(name)
                        .map(|field| field.clone().with_nullable(true))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            fields.extend(
                left.schema()
                    .fields()
                    .iter()
                    .filter(|f| !prefix.contains(f.name()))
                    .map(|f| f.as_ref().clone().with_nullable(true)),
            );
            let right_prefix = &right.keys()[..join_key];
            for field in right
                .schema()
                .fields()
                .iter()
                .filter(|f| !right_prefix.contains(f.name()))
            {
                if fields.iter().any(|f| f.name() == field.name()) {
                    return Err(TableError::Invalid(format!(
                        "join row field collision: {}",
                        field.name()
                    )));
                }
                fields.push(field.as_ref().clone().with_nullable(true));
            }
            let mut keys = left.keys().to_vec();
            keys.extend_from_slice(&right.keys()[join_key..]);
            (
                Arc::new(Schema::new(fields)),
                keys,
                combined_globals(left.globals(), right.globals())?,
            )
        };
        for field in schema.fields() {
            if globals.schema().index_of(field.name()).is_ok() {
                return Err(TableError::Invalid(format!(
                    "join row/global field collision: {}",
                    field.name()
                )));
            }
        }
        Ok(Self::new(
            schema,
            keys,
            globals,
            TableNode::Join {
                left: left.clone(),
                right,
                kind,
                join_key,
            },
        ))
    }

    pub fn multi_way_zip_join(
        tables: Vec<TableRef>,
        data_field_name: String,
        global_field_name: String,
    ) -> Result<TableRef> {
        let mut normalized = std::collections::HashMap::<usize, TableRef>::new();
        let tables = tables
            .iter()
            .map(|table| {
                let id = Arc::as_ptr(table) as usize;
                if let Some(value) = normalized.get(&id) {
                    return Ok(value.clone());
                }
                let value = table.canonical_join_keys()?;
                normalized.insert(id, value.clone());
                Ok(value)
            })
            .collect::<Result<Vec<TableRef>>>()?;
        let first = tables.first().ok_or_else(|| {
            TableError::Invalid("multi_way_zip_join requires at least one table".into())
        })?;
        for table in &tables {
            if table.keys() != first.keys()
                || logical_fields(table.schema())? != logical_fields(first.schema())?
                || logical_fields(table.globals().schema())?
                    != logical_fields(first.globals().schema())?
            {
                return Err(TableError::Invalid(
                    "multi_way_zip_join requires identical key, row and global types".into(),
                ));
            }
        }
        if data_field_name.is_empty()
            || global_field_name.is_empty()
            || first.keys().contains(&data_field_name)
            || first.keys().contains(&global_field_name)
            || data_field_name == global_field_name
        {
            return Err(TableError::Invalid(
                "invalid or colliding zip join output field names".into(),
            ));
        }
        let value_type = Type::Struct {
            r#struct: logical_fields(first.schema())?
                .into_iter()
                .filter(|(name, _)| !first.keys().contains(name))
                .collect(),
        };
        let mut fields = first
            .keys()
            .iter()
            .map(|name| first.schema().field_with_name(name).cloned())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        fields.push(arrow_schema::Field::new(
            &data_field_name,
            Type::Array {
                array: Box::new(value_type),
            }
            .to_arrow()?,
            true,
        ));
        let globals_type = Type::Struct {
            r#struct: logical_fields(first.globals().schema())?,
        };
        let mut bindings = GlobalBindings::new();
        let mut elements = Vec::with_capacity(tables.len());
        for (i, table) in tables.iter().enumerate() {
            let reference = format!("zip_globals_{i}");
            bindings.insert(reference.clone(), table.globals().clone());
            elements.push(ScalarIR::ExternalGlobal {
                reference,
                dtype: globals_type.clone(),
            });
        }
        let globals = GlobalsIR::empty().map_bound(
            vec![(
                global_field_name,
                ScalarIR::Array {
                    element_type: globals_type,
                    elements,
                },
            )],
            bindings,
        )?;
        Ok(Self::new(
            Arc::new(Schema::new(fields)),
            first.keys().to_vec(),
            globals,
            TableNode::MultiWayZipJoin {
                tables,
                data_field_name,
            },
        ))
    }
}
fn logical_fields(schema: &Schema) -> Result<Vec<(String, Type)>> {
    schema
        .fields()
        .iter()
        .map(|f| Ok((f.name().clone(), Type::from_arrow(f.data_type())?)))
        .collect()
}
fn combined_globals(left: &GlobalsRef, right: &GlobalsRef) -> Result<GlobalsRef> {
    let mut fields = left
        .schema()
        .fields()
        .iter()
        .map(|f| {
            (
                f.name().clone(),
                ScalarIR::Global {
                    name: f.name().clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    let dtype = Type::Struct {
        r#struct: logical_fields(right.schema())?,
    };
    for field in right.schema().fields() {
        if left.schema().index_of(field.name()).is_ok() {
            return Err(TableError::Invalid(format!(
                "join global field collision: {}",
                field.name()
            )));
        }
        fields.push((
            field.name().clone(),
            ScalarIR::GetField {
                value: Arc::new(ScalarIR::ExternalGlobal {
                    reference: "right_join_globals".into(),
                    dtype: dtype.clone(),
                }),
                name: field.name().clone(),
            },
        ));
    }
    let mut bindings = GlobalBindings::new();
    bindings.insert("right_join_globals".into(), right.clone());
    left.map_bound(fields, bindings)
}
fn match_key(input: &PhysicalPlan, name: &str) -> Result<Arc<dyn PhysicalExpr>> {
    let value = column(input, input.schema().index_of(name)?);
    let missing = ScalarValue::try_from(value.data_type(input.schema().as_ref())?)?;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(
            Arc::new(IsNullExpr::new(value.clone())),
            Arc::new(Literal::new(missing)),
        )],
        Some(value),
    )?))
}
fn prefer_left(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    absent: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(absent, right)],
        Some(left),
    )?))
}
/// Attach the ordering guaranteed by a keyed TableIR (including interval
/// clipping). This adapter does not read, sort, or repartition its input.
#[derive(Debug)]
pub(crate) struct JoinOrderingExec {
    input: PhysicalPlan,
    ordering: datafusion_physical_expr::LexOrdering,
    properties: Arc<datafusion_physical_plan::PlanProperties>,
}
impl JoinOrderingExec {
    pub(crate) fn new(
        input: PhysicalPlan,
        ordering: datafusion_physical_expr::LexOrdering,
    ) -> PhysicalPlan {
        let mut properties = (**input.properties()).clone();
        properties.eq_properties.add_ordering(ordering.clone());
        properties.set_eq_properties(properties.eq_properties.clone());
        Arc::new(Self {
            input,
            ordering,
            properties: Arc::new(properties),
        })
    }
}
impl datafusion_physical_plan::DisplayAs for JoinOrderingExec {
    fn fmt_as(
        &self,
        _: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "JoinKeyOrderingExec")
    }
}
impl datafusion_physical_plan::ExecutionPlan for JoinOrderingExec {
    fn name(&self) -> &'static str {
        "JoinKeyOrderingExec"
    }
    fn properties(&self) -> &Arc<datafusion_physical_plan::PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        visitor: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> datafusion_common::Result<
            datafusion_common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        for key in &self.ordering {
            let result = visitor(&key.expr)?;
            if result == datafusion_common::tree_node::TreeNodeRecursion::Stop {
                return Ok(result);
            }
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Plan(
                "join ordering requires one child".into(),
            ));
        }
        Ok(Self::new(children[0].clone(), self.ordering.clone()))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion_physical_plan::SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

pub(crate) fn lower(
    output: &TableRef,
    left: &TableRef,
    right: &TableRef,
    kind: TableJoinKind,
    join_key: usize,
) -> Result<TablePlan> {
    let left_plan = plan::ordinal(crate::partitioned::prepared_ordered(left)?.rows);
    let right_plan = plan::ordinal(crate::partitioned::prepared_ordered(right)?.rows);
    let left_width = left_plan.schema().fields().len();
    let right_width = right_plan.schema().fields().len();
    let left_ordinal = left_width - 1;
    let right_ordinal = right_width - 1;
    let on = if join_key == 0 {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        vec![(key.clone(), key)]
    } else {
        left.keys()[..join_key]
            .iter()
            .zip(&right.keys()[..join_key])
            .map(|(l, r)| Ok((match_key(&left_plan, l)?, match_key(&right_plan, r)?)))
            .collect::<Result<Vec<_>>>()?
    };
    let join_type = match kind {
        TableJoinKind::Inner => JoinType::Inner,
        TableJoinKind::Left => JoinType::Left,
        TableJoinKind::Right => JoinType::Right,
        TableJoinKind::Outer => JoinType::Full,
        TableJoinKind::Semi => JoinType::LeftSemi,
        TableJoinKind::Anti => JoinType::LeftAnti,
    };
    // Typed match keys preserve nested values and native Arrow ordering.
    let options = arrow_schema::SortOptions {
        descending: false,
        nulls_first: false,
    };
    let ordered_input = |input: PhysicalPlan, keys: Vec<Arc<dyn PhysicalExpr>>| -> PhysicalPlan {
        let ordering = datafusion_physical_expr::LexOrdering::new(
            keys.into_iter()
                .map(|expr| datafusion_physical_expr::PhysicalSortExpr::new(expr, options))
                .collect::<Vec<_>>(),
        )
        .expect("join always has at least one match key");
        JoinOrderingExec::new(input, ordering)
    };
    let left_plan = ordered_input(left_plan, on.iter().map(|(left, _)| left.clone()).collect());
    let right_plan = ordered_input(
        right_plan,
        on.iter().map(|(_, right)| right.clone()).collect(),
    );
    let sort_options = vec![options; on.len()];
    let joined: PhysicalPlan = Arc::new(SortMergeJoinExec::try_new(
        left_plan,
        right_plan,
        on,
        None,
        join_type,
        sort_options,
        NullEquality::NullEqualsNothing,
    )?);
    let semi = matches!(kind, TableJoinKind::Semi | TableJoinKind::Anti);
    let mut fields = Vec::new();
    for field in output.schema().fields() {
        let name = field.name();
        let expr = if let Ok(index) = left.schema().index_of(name) {
            let left_value = column(&joined, index);
            if !semi {
                if let Some(key_pos) = left.keys()[..join_key].iter().position(|k| k == name) {
                    let right_index = right.schema().index_of(&right.keys()[key_pos])?;
                    prefer_left(
                        left_value,
                        column(&joined, left_width + right_index),
                        Arc::new(IsNullExpr::new(column(&joined, left_ordinal))),
                    )?
                } else {
                    left_value
                }
            } else {
                left_value
            }
        } else {
            column(&joined, left_width + right.schema().index_of(name)?)
        };
        fields.push((expr, name.clone()));
    }
    // Stable side ordinals are used only to define output order within key ties.
    let left_order = plan::private_name(
        output.schema(),
        output.globals().schema(),
        "__vail_join_left_ordinal",
    );
    let right_order = plan::private_name(
        output.schema(),
        output.globals().schema(),
        "__vail_join_right_ordinal",
    );
    fields.push((column(&joined, left_ordinal), left_order.clone()));
    if !semi {
        fields.push((
            column(&joined, left_width + right_ordinal),
            right_order.clone(),
        ));
    }
    fields.push((
        Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
        plan::private_name(
            output.schema(),
            output.globals().schema(),
            "__vail_partition",
        ),
    ));
    let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new(fields, joined)?);
    let mut ordering = if output.keys().is_empty() {
        Vec::new()
    } else {
        vail_execution::OrderRequirement::new(
            output
                .keys()
                .iter()
                .map(|name| vail_execution::OrderKey::new(name, false, true))
                .collect(),
        )?
        .physical_ordering(&projected.schema())?
        .into_iter()
        .collect::<Vec<_>>()
    };
    let options = arrow_schema::SortOptions {
        descending: false,
        nulls_first: false,
    };
    ordering.push(datafusion_physical_expr::PhysicalSortExpr::new(
        column(&projected, projected.schema().index_of(&left_order)?),
        options,
    ));
    if !semi {
        ordering.push(datafusion_physical_expr::PhysicalSortExpr::new(
            column(&projected, projected.schema().index_of(&right_order)?),
            options,
        ));
    }
    let sorted: PhysicalPlan = Arc::new(datafusion_physical_plan::sorts::sort::SortExec::new(
        datafusion_physical_expr::LexOrdering::new(ordering)
            .expect("join ordinal ordering is nonempty"),
        projected,
    ));
    let mut fields = output
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (column(&sorted, i), f.name().clone()))
        .collect::<Vec<_>>();
    fields.push((
        Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
        plan::private_name(
            output.schema(),
            output.globals().schema(),
            "__vail_partition",
        ),
    ));
    Ok(TablePlan {
        rows: Arc::new(ProjectionExec::try_new(fields, sorted)?),
        layout: PartitionLayout::Fixed(1),
    })
}
