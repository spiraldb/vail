//! Batch dictionary lookup using DataFusion's grouping index. Scalar
//! dictionaries are indexed once for the entire batch of query keys.
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, StructArray, UInt32Array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_plan::aggregates::{group_values::new_group_values, order::GroupOrdering};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
struct Lookup {
    signature: Signature,
    many: bool,
}
pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    matches!(name, "__vail_dict_lookup" | "__vail_dict_lookup_many").then(|| {
        Arc::new(ScalarUDF::from(Lookup {
            signature: Signature::any(2, Volatility::Immutable),
            many: name == "__vail_dict_lookup_many",
        }))
    })
}
impl ScalarUDFImpl for Lookup {
    fn name(&self) -> &str {
        if self.many {
            "__vail_dict_lookup_many"
        } else {
            "__vail_dict_lookup"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [DataType::List(item), query] = args else {
            return plan_err!("dictionary lookup requires dictionary and key");
        };
        let DataType::Struct(fields) = item.data_type() else {
            return plan_err!("dictionary entries must be records");
        };
        let key = fields.iter().find(|field| field.name() == "key");
        let value = fields.iter().find(|field| field.name() == "value");
        let (Some(key), Some(value)) = (key, value) else {
            return plan_err!("dictionary entries require key and value");
        };
        let query = if self.many {
            let DataType::List(item) = query else {
                return plan_err!("batched dictionary keys must be lists");
            };
            item.data_type()
        } else {
            query
        };
        if key.data_type() != query {
            return plan_err!("dictionary query key type mismatch");
        }
        let result = DataType::Struct(
            vec![
                Arc::new(Field::new("found", DataType::Boolean, true)),
                value.clone(),
            ]
            .into(),
        );
        Ok(if self.many {
            DataType::List(Arc::new(Field::new("item", result, true)))
        } else {
            result
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar_dict = matches!(args.args[0], ColumnarValue::Scalar(_));
        let all_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if all_scalar { 1 } else { args.number_rows };
        let dictionaries = args.args[0]
            .clone()
            .into_array(if scalar_dict { 1 } else { rows })?;
        let query_input = args.args[1].clone().into_array(rows)?;
        let query_lists = if self.many {
            Some(query_input.as_any().downcast_ref::<ListArray>().unwrap())
        } else {
            None
        };
        let query = if let Some(lists) = query_lists {
            let start = lists.value_offsets()[0] as usize;
            let end = lists.value_offsets()[lists.len()] as usize;
            lists.values().slice(start, end - start)
        } else {
            query_input.clone()
        };
        let dtype = self.return_type(&[
            dictionaries.data_type().clone(),
            query_input.data_type().clone(),
        ])?;
        let lists = dictionaries.as_any().downcast_ref::<ListArray>().unwrap();
        let entries = lists
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let mut sources = Vec::new();
        let mut owners = Vec::new();
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let owner = u32::try_from(row).map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "dictionary owner exceeds Arrow index capacity"
                )
            })?;
            for source in lists.value_offsets()[row]..lists.value_offsets()[row + 1] {
                if entries.is_null(source as usize) {
                    return exec_err!("dictionary entry must be defined");
                }
                sources.push(source as u32);
                owners.push(owner);
            }
        }
        let sources = UInt32Array::from(sources);
        let keys = entries.column_by_name("key").unwrap();
        let dictionary_keys = if scalar_dict && lists.is_valid(0) {
            let start = lists.value_offsets()[0] as usize;
            keys.slice(start, lists.value_length(0) as usize)
        } else {
            take(keys.as_ref(), &sources, None)?
        };
        let owner_array: ArrayRef = Arc::new(UInt32Array::from(owners));
        let mut grouping = new_group_values(
            Arc::new(Schema::new(vec![
                Field::new("owner", DataType::UInt32, false),
                Field::new("key", query.data_type().clone(), true),
            ])),
            &GroupOrdering::None,
        )?;
        let mut groups = Vec::new();
        grouping.intern(&[owner_array, dictionary_keys], &mut groups)?;
        let mut selected = vec![None; grouping.len()];
        for (index, &group) in groups.iter().enumerate() {
            if selected[group].is_none() {
                selected[group] = Some(sources.value(index));
            }
        }
        let query_rows = if let Some(queries) = query_lists {
            let mut owners = Vec::with_capacity(query.len());
            for row in 0..rows {
                owners.extend(std::iter::repeat_n(row, queries.value_length(row) as usize));
            }
            owners
        } else {
            (0..rows).collect()
        };
        let query_owners = query_rows
            .iter()
            .map(|&row| {
                u32::try_from(if scalar_dict { 0 } else { row }).map_err(|_| {
                    datafusion_common::exec_datafusion_err!(
                        "dictionary owner exceeds Arrow index capacity"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        groups.clear();
        grouping.intern(
            &[Arc::new(UInt32Array::from(query_owners)), query],
            &mut groups,
        )?;
        let valid = query_rows
            .iter()
            .map(|&row| lists.is_valid(if scalar_dict { 0 } else { row }))
            .collect::<Vec<_>>();
        let matches = groups
            .iter()
            .enumerate()
            .map(|(row, &group)| {
                if valid[row] {
                    selected.get(group).copied().flatten()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let found: ArrayRef = Arc::new(BooleanArray::from(
            matches.iter().map(Option::is_some).collect::<Vec<_>>(),
        ));
        let values = take(
            entries.column_by_name("value").unwrap().as_ref(),
            &UInt32Array::from(matches),
            None,
        )?;
        let item_type = if let DataType::List(item) = &dtype {
            item.data_type().clone()
        } else {
            dtype.clone()
        };
        let DataType::Struct(fields) = item_type else {
            unreachable!()
        };
        let output =
            StructArray::try_new(fields, vec![found, values], Some(NullBuffer::from(valid)))?;
        if let Some(queries) = query_lists {
            let DataType::List(item) = dtype else {
                unreachable!()
            };
            let start = queries.value_offsets()[0];
            let offsets = arrow_buffer::OffsetBuffer::new(
                queries
                    .value_offsets()
                    .iter()
                    .map(|offset| offset - start)
                    .collect::<Vec<_>>()
                    .into(),
            );
            let output = Arc::new(ListArray::try_new(
                item,
                offsets,
                Arc::new(output),
                queries.nulls().cloned(),
            )?);
            if all_scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::List(output)))
            } else {
                Ok(ColumnarValue::Array(output))
            }
        } else if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(output))))
        } else {
            Ok(ColumnarValue::Array(Arc::new(output)))
        }
    }
}
