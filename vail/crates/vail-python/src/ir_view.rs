//! Explicit inspection views retain native scalar nodes; no diagnostic wire encoding.
use crate::{NativeExpr, NativeType, value_error};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use vail_table::{TableIR, TableNode};
fn expr(e: &vail_execution::expressions::Expr) -> NativeExpr {
    NativeExpr::from_expr(e.clone())
}
fn fields<'py>(
    py: Python<'py>,
    fs: &[(String, vail_execution::expressions::Expr)],
) -> PyResult<Bound<'py, PyList>> {
    PyList::new(
        py,
        fs.iter()
            .map(|(n, e)| {
                PyList::new(
                    py,
                    vec![
                        n.into_pyobject(py)?.into_any(),
                        Py::new(py, expr(e))?.into_bound(py).into_any(),
                    ],
                )
            })
            .collect::<PyResult<Vec<_>>>()?,
    )
}
pub fn table<'py>(py: Python<'py>, t: &TableIR) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    let name = match t.node() {
        TableNode::Prepared { n_partitions, .. } => {
            d.set_item("n_partitions", n_partitions)?;
            "PreparedDataFusionPartitions"
        }
        TableNode::IntervalRead {
            child,
            intervals,
            filter,
            ..
        } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("interval_count", intervals.len())?;
            d.set_item("filter_intervals", filter)?;
            "TableReadIntervals"
        }
        TableNode::Read { source } => {
            d.set_item("metadata", source.metadata_location())?;
            "TableRead"
        }
        TableNode::BgenSelect { source, variants } => {
            d.set_item("source", table(py, source)?)?;
            d.set_item("variants", table(py, variants)?)?;
            "BgenVariantSelection"
        }
        TableNode::TextRead {
            paths,
            format,
            options_json,
        } => {
            d.set_item("paths", paths)?;
            d.set_item("format", format)?;
            d.set_item(
                "options",
                crate::native_ir::option_object(py, options_json)?,
            )?;
            "DataFusionTextRead"
        }
        TableNode::Range {
            n, n_partitions, ..
        } => {
            d.set_item("n", n)?;
            d.set_item("n_partitions", n_partitions)?;
            "TableRange"
        }
        TableNode::Parallelize { rows, n_partitions } => {
            d.set_item("rows", rows.num_rows())?;
            d.set_item("n_partitions", n_partitions)?;
            "TableParallelize"
        }
        TableNode::Expression {
            expression,
            n_partitions,
            ..
        } => {
            d.set_item("expression", expr(expression))?;
            d.set_item("n_partitions", n_partitions)?;
            "TableParallelizeExpression"
        }
        TableNode::MapRows {
            child, fields: fs, ..
        } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("fields", fields(py, fs)?)?;
            "TableMapRows"
        }
        TableNode::Aggregate {
            child,
            keys,
            fields: fs,
            ..
        } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("keys", fields(py, keys)?)?;
            d.set_item("fields", fields(py, fs)?)?;
            "TableGroupedAggregate"
        }
        TableNode::Filter {
            child,
            predicate,
            keep,
            ..
        } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("predicate", expr(predicate))?;
            d.set_item("keep", keep)?;
            "TableFilter"
        }
        TableNode::MapGlobals { child } => {
            d.set_item("child", table(py, child)?)?;
            "TableMapGlobals"
        }
        TableNode::MapPartitions {
            child,
            rows_name,
            body,
            ..
        } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("rows_name", rows_name)?;
            d.set_item("body", expr(body))?;
            "TableMapPartitions"
        }
        TableNode::MatrixEntriesByKeyRuns { child, .. } => {
            d.set_item("child", table(py, child)?)?;
            "MatrixEntriesByKeyRuns"
        }
        TableNode::Lookup {
            left,
            right,
            keys,
            field,
            ..
        } => {
            d.set_item("left", table(py, left)?)?;
            d.set_item("right", table(py, right)?)?;
            d.set_item(
                "keys",
                PyList::new(
                    py,
                    keys.iter()
                        .map(|e| Py::new(py, expr(e)))
                        .collect::<PyResult<Vec<_>>>()?,
                )?,
            )?;
            d.set_item("field", field)?;
            "TableLookup"
        }
        TableNode::Join {
            left,
            right,
            kind,
            join_key,
        } => {
            d.set_item("left", table(py, left)?)?;
            d.set_item("right", table(py, right)?)?;
            d.set_item("kind", format!("{kind:?}"))?;
            d.set_item("join_key", join_key)?;
            "TableJoin"
        }
        TableNode::MultiWayZipJoin {
            tables,
            data_field_name,
        } => {
            d.set_item(
                "tables",
                PyList::new(
                    py,
                    tables
                        .iter()
                        .map(|t| table(py, t))
                        .collect::<PyResult<Vec<_>>>()?,
                )?,
            )?;
            d.set_item("data_field_name", data_field_name)?;
            "TableMultiWayZipJoin"
        }
        TableNode::Union { children } => {
            d.set_item(
                "children",
                PyList::new(
                    py,
                    children
                        .iter()
                        .map(|t| table(py, t))
                        .collect::<PyResult<Vec<_>>>()?,
                )?,
            )?;
            "TableUnion"
        }
        TableNode::Distinct { child } => {
            d.set_item("child", table(py, child)?)?;
            "TableDistinct"
        }
        TableNode::KeyBy { child, keys } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("keys", keys)?;
            "TableKeyBy"
        }
        TableNode::OrderBy { child, keys } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("keys", keys)?;
            "TableOrderBy"
        }
        TableNode::Head { child, n } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("n", n)?;
            "TableHead"
        }
        TableNode::Tail { child, n } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("n", n)?;
            "TableTail"
        }
        TableNode::Repartition { child, n, strategy } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("n", n)?;
            d.set_item("strategy", format!("{strategy:?}"))?;
            "TableRepartition"
        }
        TableNode::FilterPartitions { child, parts, keep } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("parts", parts)?;
            d.set_item("keep", keep)?;
            "TableFilterPartitions"
        }
        TableNode::GroupWithinPartitions { child, name, n } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("name", name)?;
            d.set_item("n", n)?;
            "TableGroupWithinPartitions"
        }
        TableNode::Explode { child, path } => {
            d.set_item("child", table(py, child)?)?;
            d.set_item("path", path)?;
            "TableExplode"
        }
    };
    d.set_item("node", name)?;
    d.set_item(
        "schema",
        t.schema()
            .fields()
            .iter()
            .map(|f| {
                Ok((
                    f.name().clone(),
                    NativeType::new_type(
                        vail_execution::expressions::Type::from_arrow(f.data_type())
                            .map_err(value_error)?,
                    ),
                ))
            })
            .collect::<PyResult<Vec<_>>>()?,
    )?;
    Ok(d)
}
