//! Native lowering for Hail's multi-way keyed zip join.
use crate::plan::{PhysicalPlan, TablePlan, column};
use crate::{Result, TableError, TableRef};
use arrow_schema::{DataType, Field};
use datafusion_common::{NullEquality, ScalarValue};
use datafusion_expr::{JoinType, Operator, WindowFunctionDefinition, window_frame::WindowFrame};
use datafusion_physical_expr::{
    PhysicalExpr, PhysicalSortExpr,
    expressions::{BinaryExpr, CaseExpr, IsNullExpr, Literal},
};
use datafusion_physical_plan::{
    filter::FilterExec,
    joins::SortMergeJoinExec,
    projection::ProjectionExec,
    sorts::sort::SortExec,
    windows::{WindowAggExec, create_window_expr},
};
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type, lower_with_bindings};

/// Add a per-key ordinal. Hail zips the nth row for each key from each input;
/// the source plans are already in key order, and this ordinal preserves their
/// stable order within duplicate-key groups.
fn add_key_ordinal(
    input: PhysicalPlan,
    key_name: &str,
    ordinal_name: &str,
    deduplicate_present_keys: bool,
) -> Result<PhysicalPlan> {
    let numbered = crate::plan::ordinal(input);
    let ordinal_index = numbered.schema().fields().len() - 1;
    let key_index = numbered.schema().index_of(key_name)?;
    let ordering = datafusion_physical_expr::LexOrdering::new(vec![
        PhysicalSortExpr::new(
            column(&numbered, key_index),
            arrow_schema::SortOptions::default(),
        ),
        PhysicalSortExpr::new(
            column(&numbered, ordinal_index),
            arrow_schema::SortOptions::default(),
        ),
    ])
    .expect("non-empty ordering");
    let sorted: PhysicalPlan = Arc::new(SortExec::new(ordering, numbered));
    let window = create_window_expr(
        &WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        ordinal_name.to_owned(),
        &[],
        &[column(&sorted, key_index)],
        &[PhysicalSortExpr::new(
            column(&sorted, ordinal_index),
            arrow_schema::SortOptions::default(),
        )],
        Arc::new(WindowFrame::new(Some(true))),
        sorted.schema(),
        false,
        false,
        None,
    )?;
    let windowed: PhysicalPlan = Arc::new(WindowAggExec::try_new(vec![window], sorted, true)?);
    if !deduplicate_present_keys {
        return Ok(windowed);
    }
    // Hail's join domain is distinct on present keys; which duplicate row is
    // retained is unspecified. Keep every missing-key row because missing keys
    // do not compare equal in the full join domain.
    let window_ordinal = windowed.schema().index_of(ordinal_name)?;
    let key_index = windowed.schema().index_of(key_name)?;
    let first = Arc::new(BinaryExpr::new(
        column(&windowed, window_ordinal),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::UInt64(Some(1)))),
    ));
    let missing_key = Arc::new(IsNullExpr::new(column(&windowed, key_index)));
    let keep = Arc::new(BinaryExpr::new(missing_key, Operator::Or, first));
    Ok(Arc::new(FilterExec::try_new(keep, windowed)?))
}

fn private_name(mut name: String, schema: &arrow_schema::Schema) -> String {
    while schema.index_of(&name).is_ok() {
        name.push('_');
    }
    name
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
    let mut marker_names = Vec::with_capacity(tables.len());
    let mut payload_names = Vec::with_capacity(tables.len());
    let mut key_value_names = Vec::with_capacity(tables.len());
    let mut ordinal_names = Vec::with_capacity(tables.len());
    for (table_index, table) in tables.iter().enumerate() {
        let mut rows = crate::partitioned::deferred_ordered(table.clone())?.rows;
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
            let canonical = vail_execution::ordering::canonical_key(lower_with_bindings(
                &key_struct,
                &schema,
                table.globals().schema(),
                &Default::default(),
            )?);
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
                    Arc::new(Literal::new(ScalarValue::Binary(None))),
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
        rows = add_key_ordinal(rows, &canonical_name, &ordinal_name, true)?;
        marker_names.push(marker.clone());
        ordinal_names.push(ordinal_name.clone());
        inputs.push((rows, canonical_name, marker, ordinal_name));
    }

    let (mut joined, mut canonical_name, _, _) = inputs.remove(0);
    for (right, right_key, _, _) in inputs {
        let left_schema = joined.schema();
        let right_schema = right.schema();
        let left_key_index = left_schema.index_of(&canonical_name)?;
        let right_key_index = right_schema.index_of(&right_key)?;
        let left_on = column(&joined, left_key_index);
        let right_on = column(&right, right_key_index);
        let sort_key = |input: PhysicalPlan, key: Arc<dyn PhysicalExpr>| -> PhysicalPlan {
            let ordering = datafusion_physical_expr::LexOrdering::new(vec![PhysicalSortExpr::new(
                key,
                arrow_schema::SortOptions::default(),
            )])
            .expect("zip join has a canonical key");
            Arc::new(SortExec::new(ordering, input))
        };
        let sorted_left = sort_key(joined.clone(), left_on.clone());
        let sorted_right = sort_key(right.clone(), right_on.clone());
        let merged: PhysicalPlan = Arc::new(SortMergeJoinExec::try_new(
            sorted_left,
            sorted_right,
            vec![(left_on, right_on)],
            None,
            JoinType::Full,
            vec![arrow_schema::SortOptions::default()],
            NullEquality::NullEqualsNothing,
        )?);
        let next_key = private_name(
            format!("__vail_zip_key_merge_{}", merged.schema().fields().len()),
            &merged.schema(),
        );
        let mut projection = (0..merged.schema().fields().len())
            .map(|i| (column(&merged, i), merged.schema().field(i).name().clone()))
            .collect::<Vec<_>>();
        let key_expr: Arc<dyn PhysicalExpr> = Arc::new(CaseExpr::try_new(
            None,
            vec![(
                Arc::new(IsNullExpr::new(column(&merged, left_key_index))),
                column(&merged, left_schema.fields().len() + right_key_index),
            )],
            Some(column(&merged, left_key_index)),
        )?);
        projection.push((key_expr, next_key.clone()));
        joined = Arc::new(ProjectionExec::try_new(projection, merged)?);
        canonical_name = next_key;
    }

    if !keys.is_empty() {
        let mut ordering = vec![PhysicalSortExpr::new(
            column(&joined, joined.schema().index_of(&canonical_name)?),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            },
        )];
        for (marker, ordinal) in marker_names.iter().zip(ordinal_names.iter()) {
            ordering.push(PhysicalSortExpr::new(
                column(&joined, joined.schema().index_of(marker)?),
                arrow_schema::SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ));
            ordering.push(PhysicalSortExpr::new(
                column(&joined, joined.schema().index_of(ordinal)?),
                arrow_schema::SortOptions::default(),
            ));
        }
        let ordering =
            datafusion_physical_expr::LexOrdering::new(ordering).expect("non-empty ordering");
        joined = Arc::new(SortExec::new(ordering, joined));
    }
    let joined_schema = joined.schema();
    let mut projection = Vec::<(Arc<dyn PhysicalExpr>, String)>::new();
    for (key_index, key) in keys.iter().enumerate() {
        let mut value = Expr::RuntimeLiteral {
            value: ScalarValue::try_from(
                output
                    .schema()
                    .field(output.schema().index_of(key)?)
                    .data_type(),
            )?,
        };
        for table_index in (0..tables.len()).rev() {
            let candidate = key_value_names[table_index][key_index].clone();
            let marker = marker_names[table_index].clone();
            value = Expr::IfElse {
                condition: Box::new(Expr::IsMissing {
                    value: Box::new(Expr::Column { name: marker }),
                }),
                consequent: Box::new(value),
                alternate: Box::new(Expr::Column { name: candidate }),
            };
        }
        let expr = lower_with_bindings(
            &value,
            &joined_schema,
            output.globals().schema(),
            &Default::default(),
        )?;
        projection.push((expr, key.clone()));
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
            condition: Box::new(Expr::IsMissing {
                value: Box::new(marker),
            }),
            consequent: Box::new(Expr::RuntimeLiteral {
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
            alternate: Box::new(row_struct),
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
    let rows: PhysicalPlan = Arc::new(ProjectionExec::try_new(projection, joined)?);
    Ok(TablePlan {
        rows,
        layout: crate::plan::PartitionLayout::Fixed(1),
    })
}
