//! Per-list grouping delegated to DataFusion's native GROUP BY implementation.
//! Only owner/selection indices are built here; Arrow takes copy the values.
use arrow_array::{Array, ArrayRef, ListArray, StructArray, UInt32Array};
use arrow_buffer::OffsetBuffer;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use arrow_select::take::take;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, EmitTo, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_plan::aggregates::{group_values::new_group_values, order::GroupOrdering};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
enum Operation {
    First,
    Last,
    Group,
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectionGroups {
    operation: Operation,
    signature: Signature,
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let operation = match name {
        "__vail_dict_first" => Operation::First,
        "__vail_dict_last" => Operation::Last,
        "__vail_collection_group" => Operation::Group,
        _ => return None,
    };
    Some(Arc::new(ScalarUDF::from(CollectionGroups {
        operation,
        signature: Signature::any(1, Volatility::Immutable),
    })))
}

fn entry_fields(dtype: &DataType) -> Result<(Arc<Field>, Arc<Field>)> {
    let DataType::List(item) = dtype else {
        return plan_err!("collection grouping requires a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        return plan_err!("collection grouping requires key/value records");
    };
    let field = |name: &str| {
        fields
            .iter()
            .find(|f| f.name() == name)
            .cloned()
            .ok_or_else(|| {
                datafusion_common::plan_datafusion_err!("collection records require {name}")
            })
    };
    Ok((field("key")?, field("value")?))
}

impl ScalarUDFImpl for CollectionGroups {
    fn name(&self) -> &str {
        match self.operation {
            Operation::First => "__vail_dict_first",
            Operation::Last => "__vail_dict_last",
            Operation::Group => "__vail_collection_group",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let (key, value) = entry_fields(&args[0])?;
        if self.operation != Operation::Group {
            return Ok(args[0].clone());
        }
        let value = Field::new(
            "value",
            DataType::List(Arc::new(value.as_ref().clone().with_name("item"))),
            true,
        );
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![key, Arc::new(value)].into()),
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let input = args.args[0]
            .clone()
            .into_array(if scalar { 1 } else { args.number_rows })?;
        let entries = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("collection grouping requires lists")
        })?;
        let output_type = self.return_type(&[input.data_type().clone()])?;
        let result = grouped(entries, &self.operation, &output_type)?;
        if scalar {
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&result, 0).map_err(|error| {
                    datafusion_common::DataFusionError::Execution(error.to_string())
                })?,
            ))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn grouped(entries: &ListArray, operation: &Operation, output_type: &DataType) -> Result<ArrayRef> {
    let records = entries
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("collection entries must be structs")
        })?;
    let keys = records.column_by_name("key").unwrap();
    let values = records.column_by_name("value").unwrap();
    let mut sources = Vec::new();
    let mut owners = Vec::new();
    for row in 0..entries.len() {
        if entries.is_null(row) {
            continue;
        }
        let owner = u32::try_from(row).map_err(|_| {
            datafusion_common::exec_datafusion_err!(
                "collection owner count exceeds Arrow index capacity"
            )
        })?;
        let start = entries.value_offsets()[row] as usize;
        let end = entries.value_offsets()[row + 1] as usize;
        for entry in start..end {
            if records.is_null(entry) {
                return exec_err!("collection entry records must be defined");
            }
            sources.push(entry as u32);
            owners.push(owner);
        }
    }
    let sources = UInt32Array::from(sources);
    let owner_array: ArrayRef = Arc::new(UInt32Array::from(owners));
    let key_array = take(keys.as_ref(), &sources, None)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("owner", DataType::UInt32, false),
        Field::new("key", key_array.data_type().clone(), true),
    ]));
    let mut grouping = new_group_values(schema, &GroupOrdering::None)?;
    let mut ids = Vec::new();
    grouping.intern(&[owner_array, key_array], &mut ids)?;
    let count = grouping.len();
    let mut representatives = vec![u32::MAX; count];
    let mut group_lengths = vec![0usize; count];
    for (input, &group) in ids.iter().enumerate() {
        if representatives[group] == u32::MAX || *operation == Operation::Last {
            representatives[group] = sources.value(input);
        }
        group_lengths[group] += 1;
    }
    let emitted = grouping.emit(EmitTo::All)?;
    let group_owners = emitted[0].as_any().downcast_ref::<UInt32Array>().unwrap();
    let mut owner_lengths = vec![0usize; entries.len()];
    for owner in group_owners.values() {
        owner_lengths[*owner as usize] += 1;
    }
    let representatives = UInt32Array::from(representatives);
    let representative_keys = take(keys.as_ref(), &representatives, None)?;
    let options = Some(SortOptions {
        descending: false,
        nulls_first: false,
    });
    let order = lexsort_to_indices(
        &[
            SortColumn {
                values: emitted[0].clone(),
                options,
            },
            SortColumn {
                values: representative_keys.clone(),
                options,
            },
        ],
        None,
    )?;
    let DataType::List(item) = output_type else {
        unreachable!()
    };
    let output_values: ArrayRef = if *operation != Operation::Group {
        let selected = take(&representatives, &order, None)?;
        take(
            records,
            selected.as_any().downcast_ref::<UInt32Array>().unwrap(),
            None,
        )?
    } else {
        let sorted_groups = order.values();
        let offsets = OffsetBuffer::<i32>::from_lengths(
            sorted_groups
                .iter()
                .map(|&group| group_lengths[group as usize]),
        );
        let mut cursors = vec![0usize; count];
        for (position, &group) in sorted_groups.iter().enumerate() {
            cursors[group as usize] = offsets[position] as usize;
        }
        let mut selections = vec![0u32; sources.len()];
        for (input, &group) in ids.iter().enumerate() {
            selections[cursors[group]] = sources.value(input);
            cursors[group] += 1;
        }
        let selected = take(values.as_ref(), &UInt32Array::from(selections), None)?;
        let DataType::Struct(fields) = item.data_type() else {
            unreachable!()
        };
        let DataType::List(value_field) = fields[1].data_type() else {
            unreachable!()
        };
        let grouped_values = Arc::new(ListArray::try_new(
            value_field.clone(),
            offsets,
            selected,
            None,
        )?);
        let records = StructArray::try_new(
            fields.clone(),
            vec![
                take(representative_keys.as_ref(), &order, None)?,
                grouped_values,
            ],
            None,
        )?;
        Arc::new(records)
    };
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        OffsetBuffer::from_lengths(owner_lengths),
        output_values,
        entries.nulls().cloned(),
    )?))
}
