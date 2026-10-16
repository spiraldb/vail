//! Native lowering for Hail's multi-way keyed zip join.
use crate::plan::{PhysicalPlan, TablePlan, column};
use crate::{Result, TableError, TableRef};
use arrow_schema::{DataType, Field};
use datafusion_common::{NullEquality, ScalarValue};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::{
    PhysicalExpr, PhysicalSortExpr,
    expressions::{BinaryExpr, CaseExpr, IsNotNullExpr, IsNullExpr, Literal},
};
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::{
    filter::FilterExec, joins::SortMergeJoinExec, projection::ProjectionExec,
    sorts::sort::SortExec, sorts::sort_preserving_merge::SortPreservingMergeExec, union::UnionExec,
};
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type, lower_with_bindings};

/// Keep one representative per present key; missing keys remain distinct.
/// The ordinal records source tie order without partition window state.
fn add_key_ordinal(
    input: PhysicalPlan,
    key_name: &str,
    ordinal_name: &str,
    certified_order: bool,
) -> Result<PhysicalPlan> {
    let numbered = crate::plan::ordinal(input);
    let ordinal_index = numbered.schema().fields().len() - 1;
    let key_index = numbered.schema().index_of(key_name)?;
    let ordering = datafusion_physical_expr::LexOrdering::new(vec![
        PhysicalSortExpr::new(
            column(&numbered, key_index),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            },
        ),
        PhysicalSortExpr::new(
            column(&numbered, ordinal_index),
            arrow_schema::SortOptions::default(),
        ),
    ])
    .unwrap();
    // A single/keyless canonical key has the source's certified key order.
    // Composite partial-missing keys collapse to a missing canonical parent,
    // so those inputs need one initial canonical sort.
    let sorted = if certified_order {
        crate::joins::JoinOrderingExec::new(numbered, ordering)
    } else {
        Arc::new(SortExec::new(ordering, numbered)) as PhysicalPlan
    };
    let rows = crate::zip_dedup::distinct(sorted, key_index)?;
    let fields: Vec<_> = (0..rows.schema().fields().len())
        .map(|index| {
            (
                column(&rows, index),
                if index == ordinal_index {
                    ordinal_name.to_owned()
                } else {
                    rows.schema().field(index).name().clone()
                },
            )
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(fields, rows)?))
}

fn private_name(mut name: String, schema: &arrow_schema::Schema) -> String {
    while schema.index_of(&name).is_ok() {
        name.push('_');
    }
    name
}

// Only producers that establish canonical ordering create this carrier.
// The checked property is retained across each full merge's key projection.
struct SortedCanonical {
    rows: PhysicalPlan,
    key: String,
}
impl SortedCanonical {
    fn new(rows: PhysicalPlan, key: String) -> Result<Self> {
        let index = rows.schema().index_of(&key)?;
        let order = datafusion_physical_expr::LexOrdering::new(vec![PhysicalSortExpr::new(
            column(&rows, index),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap();
        if !rows.equivalence_properties().ordering_satisfy(order)? {
            return Err(TableError::Invalid(
                "zip producer must establish canonical key order".into(),
            ));
        }
        Ok(Self { rows, key })
    }
    fn attach(self, right: &Self) -> Result<Self> {
        let left_schema = self.rows.schema();
        let right_schema = right.rows.schema();
        let left_key = left_schema.index_of(&self.key)?;
        let right_key = right_schema.index_of(&right.key)?;
        if left_schema.field(left_key).data_type() != right_schema.field(right_key).data_type() {
            return Err(TableError::Invalid(
                "zip canonical key storage mismatch".into(),
            ));
        }
        let on = vec![(column(&self.rows, left_key), column(&right.rows, right_key))];
        // A fixed sorted domain drives LEFT merges. Unlike FULL merge output,
        // native LEFT merge output preserves the domain's key order.
        let merged: PhysicalPlan = Arc::new(SortMergeJoinExec::try_new(
            self.rows,
            right.rows.clone(),
            on,
            None,
            JoinType::Left,
            vec![arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            }],
            NullEquality::NullEqualsNothing,
        )?);
        let fields = (0..merged.schema().fields().len())
            .filter(|index| *index != left_schema.fields().len() + right_key)
            .map(|index| {
                (
                    column(&merged, index),
                    merged.schema().field(index).name().clone(),
                )
            })
            .collect::<Vec<_>>();
        Self::new(Arc::new(ProjectionExec::try_new(fields, merged)?), self.key)
    }
}

/// Lower a multi-way zip join. The result carries the union of all canonical
/// keys and an array containing one nullable row struct for each input table.
pub(crate) fn lower(
    tables: &[TableRef],
    output: &TableRef,
    data_field_name: &str,
) -> Result<TablePlan> {
    if tables.is_empty() {
        return Err(TableError::Invalid(
            "multi_way_zip_join requires at least one table".into(),
        ));
    }
    let keys = output.keys();
    let mut inputs = Vec::with_capacity(tables.len());
    let mut missing_inputs = Vec::new();
    let mut marker_names = Vec::with_capacity(tables.len());
    let mut payload_names = Vec::with_capacity(tables.len());
    let mut key_value_names = Vec::with_capacity(tables.len());
    let mut ordinal_names = Vec::with_capacity(tables.len());
    for (table_index, table) in tables.iter().enumerate() {
        let mut rows = match table.node() {
            crate::TableNode::Prepared {
                rows,
                n_partitions: 1,
                ..
            } if crate::zip_snapshot::is_cell(rows) => rows.clone(),
            _ => crate::partitioned::deferred_ordered(table.clone())?.rows,
        };
        let schema = rows.schema();
        let key_indices = keys
            .iter()
            .map(|key| schema.index_of(key))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut fields = Vec::<(Arc<dyn PhysicalExpr>, String)>::new();
        let mut used = schema.as_ref().clone();
        let canonical_name = private_name(format!("__vail_zip_key_{table_index}"), &used);
        let canonical = if key_indices.is_empty() {
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>
        } else {
            let key_struct = Expr::Struct {
                fields: keys
                    .iter()
                    .zip(key_indices.iter())
                    .map(|(key, i)| {
                        (
                            key.clone(),
                            Expr::Column {
                                name: schema.field(*i).name().clone(),
                            },
                        )
                    })
                    .collect(),
            };
            let canonical = lower_with_bindings(
                &key_struct,
                &schema,
                table.globals().schema(),
                &Default::default(),
            )?;
            let mut any_missing: Option<Arc<dyn PhysicalExpr>> = None;
            for index in &key_indices {
                let is_missing: Arc<dyn PhysicalExpr> =
                    Arc::new(IsNullExpr::new(column(&rows, *index)));
                any_missing = Some(match any_missing {
                    Some(previous) => Arc::new(BinaryExpr::new(previous, Operator::Or, is_missing)),
                    None => is_missing,
                });
            }
            Arc::new(CaseExpr::try_new(
                None,
                vec![(
                    any_missing.expect("non-empty key indices"),
                    Arc::new(Literal::new(ScalarValue::try_from(
                        canonical.data_type(&schema)?,
                    )?)),
                )],
                Some(canonical),
            )?)
        };
        fields.push((canonical, canonical_name.clone()));
        // Keep a non-null row-presence marker, including for tables with no value fields.
        let marker = private_name(format!("__vail_zip_present_{table_index}"), &used);
        used = arrow_schema::Schema::new(
            used.fields()
                .iter()
                .cloned()
                .chain([Arc::new(Field::new(&marker, DataType::Boolean, false))])
                .collect::<Vec<_>>(),
        );
        fields.push((
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            marker.clone(),
        ));
        let mut row_field_names = vec![String::new(); table.schema().fields().len()];
        let payload = table
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !keys.contains(field.name()))
            .map(|(index, _field)| {
                let name = private_name(format!("__vail_zip_row_{table_index}_{index}"), &used);
                row_field_names[index] = name.clone();
                Ok((column(&rows, index), name))
            })
            .collect::<Result<Vec<_>>>()?;
        payload_names.push(row_field_names);
        fields.extend(payload);
        // Preserve original key values separately for output key projection.
        let key_names = keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let index = schema.index_of(key)?;
                let name = private_name(format!("__vail_zip_key_value_{table_index}_{i}"), &used);
                Ok((column(&rows, index), name))
            })
            .collect::<Result<Vec<_>>>()?;
        key_value_names.push(
            key_names
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>(),
        );
        fields.extend(key_names);
        rows = Arc::new(ProjectionExec::try_new(fields, rows)?);
        let ordinal_name =
            private_name(format!("__vail_zip_ordinal_{table_index}"), &rows.schema());
        rows = add_key_ordinal(rows, &canonical_name, &ordinal_name, keys.len() <= 1)?;
        marker_names.push(marker.clone());
        ordinal_names.push(ordinal_name.clone());
        if keys.iter().any(|key| {
            table
                .schema()
                .field_with_name(key)
                .is_ok_and(|field| field.is_nullable())
        }) {
            missing_inputs.push(table_index);
        }
        inputs.push(SortedCanonical::new(rows, canonical_name)?);
    }

    let used = inputs
        .iter()
        .flat_map(|input| {
            input
                .rows
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        })
        .collect::<std::collections::HashSet<_>>();
    let mut domain_key = "__vail_zip_domain_key".to_owned();
    while used.contains(&domain_key) {
        domain_key.push('_');
    }
    let domains = inputs
        .iter()
        .map(|input| -> Result<PhysicalPlan> {
            let index = input.rows.schema().index_of(&input.key)?;
            let present: PhysicalPlan = Arc::new(FilterExec::try_new(
                Arc::new(IsNotNullExpr::new(column(&input.rows, index))),
                input.rows.clone(),
            )?);
            Ok(Arc::new(ProjectionExec::try_new(
                vec![(column(&present, index), domain_key.clone())],
                present,
            )?))
        })
        .collect::<Result<Vec<_>>>()?;
    let union = UnionExec::try_new(domains)?;
    let order = datafusion_physical_expr::LexOrdering::new(vec![PhysicalSortExpr::new(
        column(&union, 0),
        arrow_schema::SortOptions {
            descending: false,
            nulls_first: false,
        },
    )])
    .unwrap();
    let merged: PhysicalPlan = Arc::new(SortPreservingMergeExec::new(order, union));
    let domain = crate::zip_dedup::distinct(merged, 0)?;
    let mut sorted = SortedCanonical::new(domain, domain_key)?;
    for input in &inputs {
        sorted = sorted.attach(input)?;
    }
    let schema = sorted.rows.schema();
    let mut branches = vec![sorted.rows];
    // Missing canonical parents never match, including across equal-looking
    // source rows. Carry each occurrence with only its own source slots set.
    for index in missing_inputs {
        let input = &inputs[index];
        let key = input.rows.schema().index_of(&input.key)?;
        let missing: PhysicalPlan = Arc::new(FilterExec::try_new(
            Arc::new(IsNullExpr::new(column(&input.rows, key))),
            input.rows.clone(),
        )?);
        let fields = schema
            .fields()
            .iter()
            .map(|field| -> Result<(Arc<dyn PhysicalExpr>, String)> {
                let value = match missing.schema().index_of(field.name()) {
                    Ok(index) => column(&missing, index),
                    Err(_) => Arc::new(Literal::new(ScalarValue::try_from(field.data_type())?))
                        as Arc<dyn PhysicalExpr>,
                };
                Ok((value, field.name().clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        branches.push(Arc::new(ProjectionExec::try_new(fields, missing)?) as PhysicalPlan);
    }
    let joined = if branches.len() == 1 {
        branches.remove(0)
    } else {
        UnionExec::try_new(branches)?
    };
    let joined_schema = joined.schema();
    let mut projection = Vec::<(Arc<dyn PhysicalExpr>, String)>::new();
    for (key_index, key) in keys.iter().enumerate() {
        let dtype = output.schema().field_with_name(key)?.data_type().clone();
        let branches = (0..tables.len())
            .map(
                |index| -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
                    let marker = column(&joined, joined_schema.index_of(&marker_names[index])?);
                    let value = column(
                        &joined,
                        joined_schema.index_of(&key_value_names[index][key_index])?,
                    );
                    Ok((Arc::new(IsNotNullExpr::new(marker)), value))
                },
            )
            .collect::<Result<Vec<_>>>()?;
        let selected: Arc<dyn PhysicalExpr> = Arc::new(CaseExpr::try_new(
            None,
            branches,
            Some(Arc::new(Literal::new(ScalarValue::try_from(&dtype)?))),
        )?);
        let selected =
            vail_execution::expressions::cast_physical_storage(selected, &joined_schema, dtype)?;
        projection.push((selected, key.clone()));
    }
    let mut elements = Vec::with_capacity(tables.len());
    let mut item_fields = Vec::new();
    for (table_index, table) in tables.iter().enumerate() {
        let fields = table
            .schema()
            .fields()
            .iter()
            .filter(|field| !keys.contains(field.name()))
            .map(|field| Ok((field.name().clone(), Type::from_arrow(field.data_type())?)))
            .collect::<Result<Vec<_>>>()?;
        item_fields.push(fields.clone());
        let row_struct = Expr::Struct {
            fields: table
                .schema()
                .fields()
                .iter()
                .filter(|field| !keys.contains(field.name()))
                .map(|field| {
                    let index = table.schema().index_of(field.name()).unwrap();
                    (
                        field.name().clone(),
                        Expr::Column {
                            name: payload_names[table_index][index].clone(),
                        },
                    )
                })
                .collect(),
        };
        let marker = Expr::Column {
            name: marker_names[table_index].clone(),
        };
        elements.push(Expr::IfElse {
            condition: Arc::new(Expr::IsMissing {
                value: Arc::new(marker),
            }),
            consequent: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::try_from(DataType::Struct(
                    table
                        .schema()
                        .fields()
                        .iter()
                        .filter(|field| !keys.contains(field.name()))
                        .cloned()
                        .collect(),
                ))?,
            }),
            alternate: Arc::new(row_struct),
        });
    }
    let item_type = Type::Struct {
        r#struct: item_fields.into_iter().next().unwrap_or_default(),
    };
    let array_expr = Expr::Array {
        element_type: item_type,
        elements,
    };
    projection.push((
        lower_with_bindings(
            &array_expr,
            &joined_schema,
            output.globals().schema(),
            &Default::default(),
        )?,
        data_field_name.to_owned(),
    ));
    let output_schema = crate::plan::physical_schema(output);
    let partition_name = output_schema.field(output_schema.fields().len() - 1).name();
    projection.push((
        Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
        partition_name.clone(),
    ));
    let output_width = projection.len();
    let mut occupied = output_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<std::collections::HashSet<_>>();
    let mut ties = Vec::new();
    for (index, (marker, ordinal)) in marker_names.iter().zip(&ordinal_names).enumerate() {
        for (kind, name) in [("present", marker), ("ordinal", ordinal)] {
            let mut alias = format!("__vail_zip_output_{kind}_{index}");
            while !occupied.insert(alias.clone()) {
                alias.push('_');
            }
            projection.push((
                column(&joined, joined.schema().index_of(name)?),
                alias.clone(),
            ));
            ties.push(alias);
        }
    }
    let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new(projection, joined)?);
    // Canonical null parents are only a matching device. Actual partial-null
    // key tuples still obey TableIR's advertised lexicographic key order.
    let mut ordering = keys
        .iter()
        .chain(&ties)
        .map(|name| {
            Ok(PhysicalSortExpr::new(
                column(&projected, projected.schema().index_of(name)?),
                arrow_schema::SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let sorted: PhysicalPlan = if ordering.is_empty() {
        projected
    } else {
        // Legacy partition/head callers execute this physical plan directly;
        // enforce the sort's single-stream input here, not only via optimizer.
        let all: PhysicalPlan = Arc::new(
            datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec::new(projected),
        );
        Arc::new(SortExec::new(
            datafusion_physical_expr::LexOrdering::new(std::mem::take(&mut ordering)).unwrap(),
            all,
        ))
    };
    let fields = (0..output_width)
        .map(|index| {
            (
                column(&sorted, index),
                sorted.schema().field(index).name().clone(),
            )
        })
        .collect::<Vec<_>>();
    let rows: PhysicalPlan = Arc::new(ProjectionExec::try_new(fields, sorted)?);
    Ok(TablePlan {
        rows,
        layout: crate::plan::PartitionLayout::Fixed(1),
    })
}
