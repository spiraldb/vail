//! Native types and expression constructors. Python holds these objects directly.
use crate::value_error;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use serde_json::Value;
use std::sync::Arc;
use vail_execution::expressions::{
    ArrayReduction, BinaryOperator, Expr, PrimitiveType, Type, UnaryOperator,
};
use vail_execution::reference::{ParInterval, ReferenceDefinition};

pub(crate) fn option_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(v) = value.cast::<PyBool>() {
        return Ok(Value::Bool(v.extract()?));
    }
    if let Ok(v) = value.cast::<PyInt>() {
        return Ok(Value::Number(v.extract::<i64>()?.into()));
    }
    if let Ok(v) = value.cast::<PyFloat>() {
        return serde_json::Number::from_f64(v.extract()?)
            .map(Value::Number)
            .ok_or_else(|| value_error("configuration requires finite numbers"));
    }
    if let Ok(v) = value.cast::<PyString>() {
        return Ok(Value::String(v.extract()?));
    }
    if let Ok(v) = value.cast::<PyDict>() {
        return Ok(Value::Object(
            v.iter()
                .map(|(k, v)| Ok((k.extract()?, option_value(&v)?)))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(v) = value.try_iter() {
        return Ok(Value::Array(
            v.map(|v| option_value(&v?)).collect::<PyResult<_>>()?,
        ));
    }
    Err(value_error("unsupported configuration value"))
}
pub(crate) fn option_object<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Bool(v) => v.into_pyobject(py)?.to_owned().into_any(),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                i.into_pyobject(py)?.into_any()
            } else if let Some(i) = v.as_u64() {
                i.into_pyobject(py)?.into_any()
            } else {
                v.as_f64().unwrap().into_pyobject(py)?.into_any()
            }
        }
        Value::String(v) => v.into_pyobject(py)?.into_any(),
        Value::Array(v) => PyList::new(
            py,
            v.iter()
                .map(|v| option_object(py, v))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any(),
        Value::Object(v) => {
            let d = PyDict::new(py);
            for (k, v) in v {
                d.set_item(k, option_object(py, v)?)?;
            }
            d.into_any()
        }
    })
}
fn get<'py>(d: &Bound<'py, PyAny>, key: &str) -> PyResult<Bound<'py, PyAny>> {
    d.get_item(key)
}
pub(crate) fn reference(value: &Bound<'_, PyAny>) -> PyResult<ReferenceDefinition> {
    let extract = |key: &str| get(value, key);
    let par = extract("par")?
        .try_iter()?
        .map(|v| {
            let v = v?;
            Ok(ParInterval {
                contig: get(&v, "contig")?.extract()?,
                start: get(&v, "start")?.extract()?,
                end: get(&v, "end")?.extract()?,
            })
        })
        .collect::<PyResult<_>>()?;
    Ok(ReferenceDefinition {
        name: extract("name")?.extract()?,
        contigs: extract("contigs")?.extract()?,
        lengths: extract("lengths")?.extract()?,
        x_contigs: extract("x_contigs")?.extract()?,
        y_contigs: extract("y_contigs")?.extract()?,
        mt_contigs: extract("mt_contigs")?.extract()?,
        par,
    })
}
fn reference_object<'py>(
    py: Python<'py>,
    value: &ReferenceDefinition,
) -> PyResult<Bound<'py, PyAny>> {
    let d = PyDict::new(py);
    d.set_item("name", &value.name)?;
    d.set_item("contigs", &value.contigs)?;
    d.set_item("lengths", &value.lengths)?;
    d.set_item("x_contigs", &value.x_contigs)?;
    d.set_item("y_contigs", &value.y_contigs)?;
    d.set_item("mt_contigs", &value.mt_contigs)?;
    let intervals = PyList::empty(py);
    for interval in &value.par {
        let item = PyDict::new(py);
        item.set_item("contig", &interval.contig)?;
        item.set_item("start", interval.start)?;
        item.set_item("end", interval.end)?;
        intervals.append(item)?;
    }
    d.set_item("par", intervals)?;
    Ok(d.into_any())
}
#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct NativeType {
    pub inner: Arc<Type>,
}
impl NativeType {
    pub fn new_type(value: Type) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}
pub(crate) fn native_type(value: &Bound<'_, PyAny>) -> PyResult<Type> {
    if let Ok(v) = value.extract::<PyRef<'_, NativeType>>() {
        return Ok((*v.inner).clone());
    }
    if let Ok(v) = value.getattr("_native_type") {
        return native_type(&v);
    }
    if let Ok(s) = value.extract::<String>() {
        let p = match s.as_str() {
            "int32" => PrimitiveType::Int32,
            "int64" => PrimitiveType::Int64,
            "float32" => PrimitiveType::Float32,
            "float64" => PrimitiveType::Float64,
            "bool" => PrimitiveType::Bool,
            "str" => PrimitiveType::Str,
            "call" => PrimitiveType::Call,
            _ => return Err(value_error("unknown primitive type")),
        };
        return Ok(Type::Primitive(p));
    }
    Err(value_error("expected a native type handle"))
}
#[pymethods]
impl NativeType {
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new_type(native_type(value)?))
    }
    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
    #[getter]
    fn kind(&self) -> String {
        match self.inner.as_ref() {
            Type::Primitive(p) => format!("{p:?}").to_lowercase(),
            Type::Array { .. } => "array".into(),
            Type::Set { .. } => "set".into(),
            Type::Dict { .. } => "dict".into(),
            Type::Struct { .. } => "struct".into(),
            Type::Tuple { .. } => "tuple".into(),
            Type::Interval { .. } => "interval".into(),
            Type::Ndarray { .. } => "ndarray".into(),
            Type::Locus { .. } => "locus".into(),
        }
    }
    #[getter]
    fn children(&self) -> Vec<Self> {
        match self.inner.as_ref() {
            Type::Array { array: t } | Type::Set { set: t } | Type::Interval { interval: t } => {
                vec![Self::new_type((**t).clone())]
            }
            Type::Dict { dict } => vec![
                Self::new_type(dict.0.clone()),
                Self::new_type(dict.1.clone()),
            ],
            Type::Tuple { tuple } => tuple.iter().cloned().map(Self::new_type).collect(),
            Type::Struct { r#struct: fields } => fields
                .iter()
                .map(|(_, t)| Self::new_type(t.clone()))
                .collect(),
            Type::Ndarray { ndarray } => vec![Self::new_type(ndarray.0.clone())],
            _ => vec![],
        }
    }
    /// Select one child without cloning unrelated subtrees.
    fn child(&self, index: usize) -> PyResult<Self> {
        let selected = match self.inner.as_ref() {
            Type::Array { array } | Type::Set { set: array } | Type::Interval { interval: array } =>
                (index == 0).then_some(array.as_ref()),
            Type::Dict { dict } => match index { 0 => Some(&dict.0), 1 => Some(&dict.1), _ => None },
            Type::Tuple { tuple } => tuple.get(index),
            Type::Struct { r#struct: fields } => fields.get(index).map(|(_, ty)| ty),
            Type::Ndarray { ndarray } => (index == 0).then_some(&ndarray.0),
            _ => None,
        }.ok_or_else(|| pyo3::exceptions::PyIndexError::new_err("type child index out of range"))?;
        Ok(Self::new_type(selected.clone()))
    }
    #[getter]
    fn field_names(&self) -> Vec<String> {
        match self.inner.as_ref() {
            Type::Struct { r#struct: fields } => fields.iter().map(|(n, _)| n.clone()).collect(),
            _ => vec![],
        }
    }
    #[getter]
    fn ndim(&self) -> PyResult<usize> {
        match self.inner.as_ref() {
            Type::Ndarray { ndarray } => Ok(ndarray.1),
            _ => Err(value_error("not an ndarray type")),
        }
    }
    fn field(&self, name: &str) -> PyResult<Self> {
        match self.inner.as_ref() {
            Type::Struct { r#struct: fields } => fields
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, t)| Self::new_type(t.clone()))
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name.to_owned())),
            _ => Err(value_error("not a struct type")),
        }
    }
    fn reference<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self.inner.as_ref() {
            Type::Locus { locus } => reference_object(py, locus),
            _ => Err(value_error("not a locus type")),
        }
    }
    fn can_cast_to(&self, target: &Self) -> bool {
        self.inner.ensure_castable_to(&target.inner).is_ok()
    }
    fn __hash__(&self) -> isize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut h);
        h.finish() as isize
    }
    #[staticmethod]
    #[pyo3(signature=(kind, children, names=None, rank=None, reference_value=None))]
    fn construct(
        kind: &str,
        children: Vec<PyRef<'_, NativeType>>,
        names: Option<Vec<String>>,
        rank: Option<usize>,
        reference_value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let ts = children
            .iter()
            .map(|t| (*t.inner).clone())
            .collect::<Vec<_>>();
        let one = || {
            if ts.len() != 1 {
                return Err(value_error("type requires exactly one element type"));
            }
            Ok(ts[0].clone())
        };
        let t = match kind {
            "array" => Type::Array {
                array: Box::new(one()?),
            },
            "set" => Type::Set {
                set: Box::new(one()?),
            },
            "interval" => Type::Interval {
                interval: Box::new(one()?),
            },
            "tuple" => Type::Tuple { tuple: ts },
            "struct" => {
                let names = names.ok_or_else(|| value_error("struct requires names"))?;
                if names.len() != ts.len() {
                    return Err(value_error("struct field count mismatch"));
                }
                Type::Struct {
                    r#struct: names.into_iter().zip(ts).collect(),
                }
            }
            "dict" => {
                if ts.len() != 2 {
                    return Err(value_error("dict requires key and value types"));
                }
                Type::Dict {
                    dict: Box::new((ts[0].clone(), ts[1].clone())),
                }
            }
            "ndarray" => Type::Ndarray {
                ndarray: Box::new((
                    one()?,
                    rank.ok_or_else(|| value_error("ndarray requires rank"))?,
                )),
            },
            "locus" => Type::Locus {
                locus: reference(
                    reference_value.ok_or_else(|| value_error("locus requires reference"))?,
                )?,
            },
            _ => return Err(value_error("unknown type constructor")),
        };
        t.to_arrow().map_err(value_error)?;
        Ok(Self::new_type(t))
    }

    fn arrow_type(&self) -> PyResult<crate::arrow_memory::NativeArrow> {
        let dtype = self.inner.to_arrow().map_err(value_error)?;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            dtype.clone(),
            true,
        )]));
        Ok(crate::arrow_memory::NativeArrow::single(
            arrow_array::RecordBatch::try_new(schema, vec![arrow_array::new_empty_array(&dtype)])
                .map_err(value_error)?,
        ))
    }
    #[staticmethod]
    fn from_arrow(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        use arrow_array::RecordBatchReader;
        let reader = crate::arrow_memory::import(value)?;
        if reader.schema().fields().len() != 1 {
            return Err(value_error("type import requires one Arrow field"));
        }
        Ok(Self::new_type(
            Type::from_arrow(reader.schema().field(0).data_type()).map_err(value_error)?,
        ))
    }

    fn __repr__(&self) -> String {
        format!("NativeType({:?})", self.inner)
    }
}
#[pyclass(frozen, subclass, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct NativeExpr {
    pub inner: Arc<Expr>,
}
impl NativeExpr {
    fn literal_precedence(&self) -> bool {
        fn visit(node: &Expr, memo: &mut std::collections::HashMap<*const Expr, bool>) -> bool {
            let key = node as *const Expr;
            if let Some(found) = memo.get(&key) { return *found; }
            let found = match node {
                Expr::Literal { .. } | Expr::RuntimeLiteral { .. } => true,
                Expr::Cast { value, .. } | Expr::Unary { value, .. } => visit(value, memo),
                Expr::Binary { left, right, .. } => visit(left, memo) && visit(right, memo),
                Expr::Array { elements, .. } | Expr::Tuple { elements } => elements.iter().all(|v| visit(v,memo)),
                Expr::Struct { fields } => fields.iter().all(|(_,v)| visit(v,memo)),
                _ => false,
            };
            memo.insert(key,found);
            found
        }
        match self.inner.as_ref() {
            Expr::Literal { .. } | Expr::RuntimeLiteral { .. } => true,
            Expr::Array { elements, .. } => {
                let mut memo = std::collections::HashMap::new();
                elements.iter().all(|v| visit(v,&mut memo))
            }
            _ => false,
        }
    }
    fn operation(&self) -> &'static str {
        match self.inner.as_ref() {
            Expr::MatrixField { .. } => "matrix_field",
            Expr::MatrixScope { .. } => "matrix_scope",
            Expr::MatrixNumCols { .. } => "matrix_num_cols",
            Expr::MatrixColumnPosition { .. } => "matrix_column_position",
            Expr::TableLookup { .. } => "table_lookup",
            Expr::RuntimeLiteral { .. } => "literal",
            Expr::Aggregate { .. } => "aggregate",
            Expr::AggregateFold { .. } => "aggregate_fold",
            Expr::AggGroupBy { .. } => "agg_group_by",
            Expr::AggArray { .. } => "agg_array",
            Expr::AggFilter { .. } => "agg_filter",
            Expr::AggExplode { .. } => "agg_explode",
            Expr::LambdaVariable { .. } => "lambda_variable",
            Expr::Switch { .. } => "switch",
            Expr::NestedFunction { .. } => "nested_function",
            Expr::CollectionCast { .. } => "collection_cast",
            Expr::IntegerBinary { .. } => "integer_binary",
            Expr::StringFunction { .. } => "string_function",
            Expr::Tuple { .. } => "tuple",
            Expr::ArrayMap { .. } => "array_map",
            Expr::ArrayFold { .. } => "array_fold",
            Expr::ArrayPredicate { .. } => "array_predicate",
            Expr::ArrayFilter { .. } => "array_filter",
            Expr::StreamRange { .. } => "stream_range",
            Expr::ArrayGrouped { .. } => "array_grouped",
            Expr::ExternalGlobal { .. } => "external_global",
            Expr::Global { .. } => "global",
            Expr::Column { .. } => "column",
            Expr::Literal { .. } => "literal",
            Expr::Binary { .. } => "binary",
            Expr::Unary { .. } => "unary",
            Expr::Cast { .. } => "cast",
            Expr::ToString { .. } => "to_string",
            Expr::GetField { .. } => "get_field",
            Expr::Interval { .. } => "interval",
            Expr::IntervalContains { .. } => "interval_contains",
            Expr::IntervalOverlaps { .. } => "interval_overlaps",
            Expr::Struct { .. } => "struct",
            Expr::IfElse { .. } => "if_else",
            Expr::Die { .. } => "die",
            Expr::IsMissing { .. } => "is_missing",
            Expr::Array { .. } => "array",
            Expr::ArrayFlatten { .. } => "array_flatten",
            Expr::ArrayLength { .. } => "array_length",
            Expr::ArrayIndex { .. } => "array_index",
            Expr::ArraySlice { .. } => "array_slice",
            Expr::ArrayReduce { .. } => "array_reduce",
            Expr::Locus { .. } => "locus",
            Expr::LocusOp { .. } => "locus_op",
            Expr::LocusValid { .. } => "locus_valid",
            Expr::LocusFromGlobalPosition { .. } => "locus_from_global_position",
            Expr::ParseLocus { .. } => "parse_locus",
            Expr::ParseCsvLine { .. } => "parse_csv_line",
        }
    }
    pub fn from_expr(value: Expr) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}
pub(crate) fn expression(value: &Bound<'_, PyAny>) -> PyResult<Expr> {
    if let Ok(v) = value.extract::<PyRef<'_, NativeExpr>>() {
        return Ok((*v.inner).clone());
    }
    if let Ok(v) = value.getattr("_native_expr") {
        return expression(&v);
    }
    build(value)
}
fn expression_ref(value: &Bound<'_, PyAny>) -> PyResult<Arc<Expr>> {
    if let Ok(v) = value.extract::<PyRef<'_, NativeExpr>>() {
        return Ok(v.inner.clone());
    }
    if let Ok(v) = value.getattr("_native_expr") {
        return expression_ref(&v);
    }
    Ok(Arc::new(build(value)?))
}
fn expressions(value: &Bound<'_, PyAny>) -> PyResult<Vec<Expr>> {
    value.try_iter()?.map(|v| expression(&v?)).collect()
}
fn fields(value: &Bound<'_, PyAny>) -> PyResult<Vec<(String, Expr)>> {
    value
        .try_iter()?
        .map(|v| {
            let v = v?;
            Ok((v.get_item(0)?.extract()?, expression(&v.get_item(1)?)?))
        })
        .collect()
}

fn build(d: &Bound<'_, PyAny>) -> PyResult<Expr> {
    let op: String = get(d, "op")?.extract()?;
    Ok(match op.as_str() {
        "matrix_field" => Expr::MatrixField {
            axis: get(d, "axis")?.extract::<String>()?,
            name: get(d, "name")?.extract::<String>()?,
        },
        "matrix_scope" => Expr::MatrixScope {
            axis: get(d, "axis")?.extract::<String>()?,
            value: expression_ref(&get(d, "value")?)?,
        },
        "matrix_num_cols" => Expr::MatrixNumCols {},
        "matrix_column_position" => Expr::MatrixColumnPosition {},
        "table_lookup" => Expr::TableLookup {
            reference: get(d, "reference")?.extract::<String>()?,
            keys: expressions(&get(d, "keys")?)?,
            all_matches: if d.get_item("all_matches").is_ok() {
                get(d, "all_matches")?.extract::<bool>()?
            } else {
                false
            },
            query_interval: if d.get_item("query_interval").is_ok() {
                get(d, "query_interval")?.extract::<bool>()?
            } else {
                false
            },
        },
        "aggregate" => Expr::Aggregate {
            function: get(d, "function")?.extract::<String>()?,
            value: d
                .get_item("value")
                .ok()
                .filter(|v| !v.is_none())
                .map(|v| expression_ref(&v))
                .transpose()?,
            result_type: d
                .get_item("options")
                .ok()
                .and_then(|v| v.get_item("__vail_result_type").ok())
                .map(|v| native_type(&v))
                .transpose()?,
            options: if d.get_item("options").is_ok() {
                let options = get(d, "options")?.cast_into::<PyDict>()?.copy()?;
                if options.contains("__vail_result_type")? {
                    options.del_item("__vail_result_type")?;
                }
                option_value(options.as_any())?
            } else {
                Value::Null
            },
            init: if d.get_item("init").is_ok() {
                fields(&get(d, "init")?)?
            } else {
                Vec::new()
            },
        },
        "aggregate_fold" => Expr::AggregateFold {
            initial: expression_ref(&get(d, "initial")?)?,
            seq: expression_ref(&get(d, "seq")?)?,
            combine: expression_ref(&get(d, "combine")?)?,
            state_parameter: get(d, "state_parameter")?.extract::<String>()?,
            left_parameter: get(d, "left_parameter")?.extract::<String>()?,
            right_parameter: get(d, "right_parameter")?.extract::<String>()?,
        },
        "agg_group_by" => Expr::AggGroupBy {
            key: expression_ref(&get(d, "key")?)?,
            body: expression_ref(&get(d, "body")?)?,
        },
        "agg_array" => Expr::AggArray {
            value: expression_ref(&get(d, "value")?)?,
            parameter: get(d, "parameter")?.extract::<String>()?,
            body: expression_ref(&get(d, "body")?)?,
        },
        "agg_filter" => Expr::AggFilter {
            predicate: expression_ref(&get(d, "predicate")?)?,
            body: expression_ref(&get(d, "body")?)?,
        },
        "agg_explode" => Expr::AggExplode {
            value: expression_ref(&get(d, "value")?)?,
            parameter: get(d, "parameter")?.extract::<String>()?,
            body: expression_ref(&get(d, "body")?)?,
        },
        "lambda_variable" => Expr::LambdaVariable {
            name: get(d, "name")?.extract::<String>()?,
        },
        "nested_function" => Expr::NestedFunction {
            function: get(d, "function")?.extract::<String>()?,
            args: expressions(&get(d, "args")?)?,
            dtype: native_type(&get(d, "dtype")?)?,
        },
        "collection_cast" => Expr::CollectionCast {
            value: expression_ref(&get(d, "value")?)?,
            dtype: native_type(&get(d, "dtype")?)?,
        },
        "integer_binary" => Expr::IntegerBinary {
            operator: match get(d, "operator")?.extract::<String>()?.as_str() {
                "add" => BinaryOperator::Add,
                "sub" => BinaryOperator::Sub,
                "mul" => BinaryOperator::Mul,
                "div" => BinaryOperator::Div,
                "floordiv" => BinaryOperator::Floordiv,
                "mod" => BinaryOperator::Mod,
                "pow" => BinaryOperator::Pow,
                "eq" => BinaryOperator::Eq,
                "ne" => BinaryOperator::Ne,
                "lt" => BinaryOperator::Lt,
                "le" => BinaryOperator::Le,
                "gt" => BinaryOperator::Gt,
                "ge" => BinaryOperator::Ge,
                "and" => BinaryOperator::And,
                "or" => BinaryOperator::Or,
                _ => return Err(value_error("invalid operator")),
            },
            left: expression_ref(&get(d, "left")?)?,
            right: expression_ref(&get(d, "right")?)?,
        },
        "string_function" | "scalar_function" => Expr::StringFunction {
            function: get(d, "function")?.extract::<String>()?,
            args: expressions(&get(d, "args")?)?,
        },
        "tuple" => Expr::Tuple {
            elements: expressions(&get(d, "elements")?)?,
        },
        "array_map" => Expr::ArrayMap {
            value: expression_ref(&get(d, "value")?)?,
            parameter: get(d, "parameter")?.extract::<String>()?,
            body: expression_ref(&get(d, "body")?)?,
        },
        "array_fold" => Expr::ArrayFold {
            value: expression_ref(&get(d, "value")?)?,
            zero: expression_ref(&get(d, "zero")?)?,
            accumulator: get(d, "accumulator")?.extract::<String>()?,
            parameter: get(d, "parameter")?.extract::<String>()?,
            body: expression_ref(&get(d, "body")?)?,
            scan: get(d, "scan")?.extract::<bool>()?,
        },
        "array_predicate" => Expr::ArrayPredicate {
            value: expression_ref(&get(d, "value")?)?,
            parameter: get(d, "parameter")?.extract::<String>()?,
            predicate: expression_ref(&get(d, "predicate")?)?,
            first: get(d, "first")?.extract::<bool>()?,
        },
        "array_filter" => Expr::ArrayFilter {
            value: expression_ref(&get(d, "value")?)?,
            parameter: get(d, "parameter")?.extract::<String>()?,
            predicate: expression_ref(&get(d, "predicate")?)?,
        },
        "stream_range" => Expr::StreamRange {
            start: expression_ref(&get(d, "start")?)?,
            stop: expression_ref(&get(d, "stop")?)?,
            step: expression_ref(&get(d, "step")?)?,
        },
        "array_grouped" => Expr::ArrayGrouped {
            value: expression_ref(&get(d, "value")?)?,
            size: expression_ref(&get(d, "size")?)?,
        },
        "external_global" => Expr::ExternalGlobal {
            reference: get(d, "reference")?.extract::<String>()?,
            dtype: native_type(&get(d, "dtype")?)?,
        },
        "global" => Expr::Global {
            name: get(d, "name")?.extract::<String>()?,
        },
        "column" => Expr::Column {
            name: get(d, "name")?.extract::<String>()?,
        },
        "literal" => {
            let dtype = NativeType::new_type(native_type(&get(d, "dtype")?)?);
            return Ok((*NativeExpr::literal(&dtype, &get(d, "value")?)?.inner).clone());
        }
        "binary" => Expr::Binary {
            operator: match get(d, "operator")?.extract::<String>()?.as_str() {
                "add" => BinaryOperator::Add,
                "sub" => BinaryOperator::Sub,
                "mul" => BinaryOperator::Mul,
                "div" => BinaryOperator::Div,
                "floordiv" => BinaryOperator::Floordiv,
                "mod" => BinaryOperator::Mod,
                "pow" => BinaryOperator::Pow,
                "eq" => BinaryOperator::Eq,
                "ne" => BinaryOperator::Ne,
                "lt" => BinaryOperator::Lt,
                "le" => BinaryOperator::Le,
                "gt" => BinaryOperator::Gt,
                "ge" => BinaryOperator::Ge,
                "and" => BinaryOperator::And,
                "or" => BinaryOperator::Or,
                _ => return Err(value_error("invalid operator")),
            },
            left: expression_ref(&get(d, "left")?)?,
            right: expression_ref(&get(d, "right")?)?,
        },
        "unary" => Expr::Unary {
            operator: match get(d, "operator")?.extract::<String>()?.as_str() {
                "neg" => UnaryOperator::Neg,
                "pos" => UnaryOperator::Pos,
                "not" => UnaryOperator::Not,
                _ => return Err(value_error("invalid operator")),
            },
            value: expression_ref(&get(d, "value")?)?,
        },
        "cast" => Expr::Cast {
            dtype: native_type(&get(d, "dtype")?)?,
            value: expression_ref(&get(d, "value")?)?,
        },
        "to_string" => Expr::ToString {
            value: expression_ref(&get(d, "value")?)?,
        },
        "get_field" => Expr::GetField {
            value: expression_ref(&get(d, "value")?)?,
            name: get(d, "name")?.extract::<String>()?,
        },
        "interval" => Expr::Interval {
            start: expression_ref(&get(d, "start")?)?,
            end: expression_ref(&get(d, "end")?)?,
            includes_start: expression_ref(&get(d, "includes_start")?)?,
            includes_end: expression_ref(&get(d, "includes_end")?)?,
        },
        "interval_contains" => Expr::IntervalContains {
            interval: expression_ref(&get(d, "interval")?)?,
            point: expression_ref(&get(d, "point")?)?,
        },
        "interval_overlaps" => Expr::IntervalOverlaps {
            left: expression_ref(&get(d, "left")?)?,
            right: expression_ref(&get(d, "right")?)?,
        },
        "struct" => Expr::Struct {
            fields: fields(&get(d, "fields")?)?,
        },
        "switch" => Expr::Switch {
            value: expression_ref(&get(d, "value")?)?,
            parameter: get(d, "parameter")?.extract()?,
            cases: get(d, "cases")?
                .try_iter()?
                .map(|pair| {
                    let pair = pair?;
                    Ok((
                        expression(&pair.get_item(0)?)?,
                        expression(&pair.get_item(1)?)?,
                    ))
                })
                .collect::<PyResult<Vec<_>>>()?,
            default: expression_ref(&get(d, "default")?)?,
            missing: d
                .get_item("missing")
                .ok()
                .filter(|value| !value.is_none())
                .map(|value| expression_ref(&value))
                .transpose()?,
        },
        "if_else" => Expr::IfElse {
            condition: expression_ref(&get(d, "condition")?)?,
            consequent: expression_ref(&get(d, "consequent")?)?,
            alternate: expression_ref(&get(d, "alternate")?)?,
        },
        "die" => Expr::Die {
            message: expression_ref(&get(d, "message")?)?,
            dtype: native_type(&get(d, "dtype")?)?,
        },
        "is_missing" => Expr::IsMissing {
            value: expression_ref(&get(d, "value")?)?,
        },
        "array" => Expr::Array {
            element_type: native_type(&get(d, "element_type")?)?,
            elements: expressions(&get(d, "elements")?)?,
        },
        "array_flatten" => Expr::ArrayFlatten {
            value: expression_ref(&get(d, "value")?)?,
        },
        "array_length" => Expr::ArrayLength {
            value: expression_ref(&get(d, "value")?)?,
        },
        "array_index" => Expr::ArrayIndex {
            value: expression_ref(&get(d, "value")?)?,
            index: expression_ref(&get(d, "index")?)?,
        },
        "array_slice" => Expr::ArraySlice {
            value: expression_ref(&get(d, "value")?)?,
            start: d
                .get_item("start")
                .ok()
                .filter(|v| !v.is_none())
                .map(|v| expression_ref(&v))
                .transpose()?,
            stop: d
                .get_item("stop")
                .ok()
                .filter(|v| !v.is_none())
                .map(|v| expression_ref(&v))
                .transpose()?,
            step: d
                .get_item("step")
                .ok()
                .filter(|v| !v.is_none())
                .map(|v| expression_ref(&v))
                .transpose()?,
        },
        "array_reduce" => Expr::ArrayReduce {
            operation: match get(d, "operation")?.extract::<String>()?.as_str() {
                "sum" => ArrayReduction::Sum,
                "product" => ArrayReduction::Product,
                "min" => ArrayReduction::Min,
                "max" => ArrayReduction::Max,
                _ => return Err(value_error("invalid operator")),
            },
            filter_missing: get(d, "filter_missing")?.extract::<bool>()?,
            value: expression_ref(&get(d, "value")?)?,
        },
        "locus" => Expr::Locus {
            reference: reference(&get(d, "reference")?)?,
            contig: expression_ref(&get(d, "contig")?)?,
            position: expression_ref(&get(d, "position")?)?,
        },
        "locus_op" => Expr::LocusOp {
            operation: get(d, "operation")?.extract::<String>()?,
            value: expression_ref(&get(d, "value")?)?,
        },
        "locus_valid" => Expr::LocusValid {
            reference: reference(&get(d, "reference")?)?,
            contig: expression_ref(&get(d, "contig")?)?,
            position: expression_ref(&get(d, "position")?)?,
        },
        "locus_from_global_position" => Expr::LocusFromGlobalPosition {
            reference: reference(&get(d, "reference")?)?,
            position: expression_ref(&get(d, "position")?)?,
        },
        "parse_locus" => Expr::ParseLocus {
            reference: reference(&get(d, "reference")?)?,
            value: expression_ref(&get(d, "value")?)?,
        },
        "parse_csv_line" => Expr::ParseCsvLine {
            value: expression_ref(&get(d, "value")?)?,
            delimiter: get(d, "delimiter")?.extract::<String>()?,
            quote: get(d, "quote")?.extract::<Option<String>>()?,
            missing: if d.get_item("missing").is_ok() {
                get(d, "missing")?.extract::<Vec<String>>()?
            } else {
                Vec::new()
            },
            expected_fields: get(d, "expected_fields")?.extract::<usize>()?,
        },
        _ => return Err(value_error(format!("unknown expression operation {op}"))),
    })
}

#[pymethods]
impl NativeExpr {
    fn literal_dict_precedence(&self) -> bool { self.literal_precedence() }
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: expression_ref(value)?,
        })
    }
    #[staticmethod]
    fn literal(dtype: &NativeType, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        use datafusion_common::ScalarValue;
        let scalar = if value
            .extract::<PyRef<'_, crate::arrow_memory::NativeArrow>>()
            .is_ok()
        {
            let batch = crate::arrow_memory::scalar(value)?;
            if batch.num_rows() != 1 || batch.num_columns() != 1 {
                return Err(value_error("literal requires one Arrow value"));
            }
            crate::vail_scalar_at(batch.column(0), 0)?
        } else if value.is_none() {
            ScalarValue::try_from(&dtype.inner.to_arrow().map_err(value_error)?)
                .map_err(value_error)?
        } else {
            match dtype.inner.as_ref() {
                Type::Primitive(PrimitiveType::Int32) => ScalarValue::Int32(Some(value.extract()?)),
                Type::Primitive(PrimitiveType::Int64) => ScalarValue::Int64(Some(value.extract()?)),
                Type::Primitive(PrimitiveType::Float32) => {
                    ScalarValue::Float32(Some(value.extract()?))
                }
                Type::Primitive(PrimitiveType::Float64) => {
                    ScalarValue::Float64(Some(value.extract()?))
                }
                Type::Primitive(PrimitiveType::Bool) => {
                    ScalarValue::Boolean(Some(value.extract()?))
                }
                Type::Primitive(PrimitiveType::Str) => ScalarValue::Utf8(Some(value.extract()?)),
                _ => {
                    let batch = crate::arrow_memory::scalar(value)?;
                    if batch.num_rows() != 1 || batch.num_columns() != 1 {
                        return Err(value_error("literal requires one typed Arrow value"));
                    }
                    crate::vail_scalar_at(batch.column(0), 0)?
                }
            }
        };
        let actual = Type::from_arrow(&scalar.data_type()).map_err(value_error)?;
        if actual != *dtype.inner {
            return Err(value_error("literal type mismatch"));
        }
        let scalar = if dtype.inner.contains_unordered() {
            let array = scalar.to_array_of_size(1).map_err(value_error)?;
            let array = vail_execution::expressions::normalize_literal_array(&array, &dtype.inner)
                .map_err(value_error)?;
            crate::vail_scalar_at(&array, 0)?
        } else {
            scalar
        };
        Ok(Self::from_expr(Expr::RuntimeLiteral { value: scalar }))
    }

    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        if key == "op" {
            return Ok(self.operation().into_pyobject(py)?.into_any());
        }
        self.view(py)?
            .get_item(key)?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_owned()))
    }
    #[pyo3(signature=(key, default=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        default: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key == "op" {
            return Ok(self.operation().into_pyobject(py)?.into_any());
        }
        Ok(self
            .view(py)?
            .get_item(key)?
            .or(default)
            .unwrap_or_else(|| py.None().into_bound(py)))
    }
    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.view(py)?.call_method0("keys")
    }
    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.view(py)?.call_method0("items")
    }
    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.view(py)?.call_method0("values")
    }
    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.view(py)?.call_method0("__iter__")
    }
    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.view(py)?.len())
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        if let Ok(other) = other.extract::<PyRef<'_, NativeExpr>>() {
            return Arc::ptr_eq(&self.inner, &other.inner) || self.inner == other.inner;
        }
        expression(other).is_ok_and(|v| *self.inner == v)
    }
    fn __repr__(&self) -> String {
        format!("NativeExpr({:?})", self.inner)
    }
    fn contains_random(&self) -> bool {
        fn visit(node: &Expr, seen: &mut std::collections::HashSet<*const Expr>) -> bool {
            if !seen.insert(node as *const Expr) {
                return false;
            }
            if matches!(node, Expr::NestedFunction { function, .. } if function.starts_with("random_"))
            {
                return true;
            }
            let mut found = false;
            node.visit_children(&mut |child| {
                if !found {
                    found = visit(child, seen);
                }
            });
            found
        }
        visit(&self.inner, &mut std::collections::HashSet::new())
    }
    fn references_rows(&self) -> bool {crate::random_binding::rows_referenced(&self.inner)}
    fn bind_random_rows(&self,row: PyRef<'_,NativeExpr>,mode: &str) -> PyResult<Self> {
        Ok(Self{inner:Arc::new(crate::random_binding::bind_rows(&self.inner,&row.inner,mode)?)})
    }
    fn bind_random_context(&self,uid: Vec<PyRef<'_,NativeExpr>>) -> PyResult<Self> {
        let uid=uid.into_iter().map(|v|v.inner.clone()).collect::<Vec<_>>();
        Ok(Self{inner:Arc::new(crate::random_binding::context(&self.inner,&uid)?)})
    }
    fn collection_aggregate(&self, value: PyRef<'_, NativeExpr>, parameter: &str,
        element: PyRef<'_, NativeType>, rows: PyRef<'_, NativeType>,
        globals: PyRef<'_, NativeType>, axes: Vec<(String, PyRef<'_, NativeType>)>,
        lookups: Vec<(String, PyRef<'_, NativeType>)>, lexical: Vec<(String, PyRef<'_, NativeType>)>) -> PyResult<Self> {
        let context=crate::collection_aggregate::Context {
            rows:crate::collection_aggregate::schema(&rows.inner)?,
            globals:crate::collection_aggregate::schema(&globals.inner)?,
            axes:axes.into_iter().map(|(name,ty)|(name,(*ty.inner).clone())).collect(),
            lookups:lookups.into_iter().map(|(name,ty)|(name,(*ty.inner).clone())).collect(),
            lexical:lexical.into_iter().map(|(name,ty)|(name,(*ty.inner).clone())).collect(),
        };
        Ok(Self { inner: Arc::new(crate::collection_aggregate::rewrite(value.inner.clone(), &self.inner, parameter, (*element.inner).clone(), &context)?) })
    }
    fn replace_parameter(&self,name: &str,value: PyRef<'_,NativeExpr>) -> PyResult<Self> {
        Ok(Self{inner:Arc::new(crate::random_binding::replace_parameter(&self.inner,name,&value.inner)?)})
    }
    fn input_global_names(&self) -> PyResult<Vec<String>> {
        crate::input_binding::global_names(&self.inner)
    }
    fn bind_input_globals(&self, globals: Vec<(String, PyRef<'_, NativeExpr>)>) -> PyResult<Self> {
        let globals = globals
            .into_iter()
            .map(|(name, value)| (name, value.inner.clone()))
            .collect();
        Ok(Self {
            inner: crate::input_binding::bind(&self.inner, &globals)?,
        })
    }
    fn same_node_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    fn view<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        match self.inner.as_ref() {
            Expr::MatrixField { axis, name } => {
                d.set_item("op", "matrix_field")?;
                d.set_item("axis", axis.into_pyobject(py)?.into_any())?;
                d.set_item("name", name.into_pyobject(py)?.into_any())?;
            }
            Expr::MatrixScope { axis, value } => {
                d.set_item("op", "matrix_scope")?;
                d.set_item("axis", axis.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::MatrixNumCols {} => {
                d.set_item("op", "matrix_num_cols")?;
            }
            Expr::MatrixColumnPosition {} => {
                d.set_item("op", "matrix_column_position")?;
            }
            Expr::TableLookup {
                reference,
                keys,
                all_matches,
                query_interval,
            } => {
                d.set_item("op", "table_lookup")?;
                d.set_item("reference", reference.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "keys",
                    PyList::new(
                        py,
                        keys.iter()
                            .map(|v| Py::new(py, NativeExpr::from_expr(v.clone())))
                            .collect::<PyResult<Vec<_>>>()?,
                    )?
                    .into_any(),
                )?;
                d.set_item(
                    "all_matches",
                    all_matches.into_pyobject(py)?.to_owned().into_any(),
                )?;
                d.set_item(
                    "query_interval",
                    query_interval.into_pyobject(py)?.to_owned().into_any(),
                )?;
            }
            Expr::Aggregate {
                function,
                value,
                options,
                result_type,
                init,
            } => {
                d.set_item("op", "aggregate")?;
                d.set_item("function", function.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "value",
                    match value {
                        Some(v) => Py::new(py, NativeExpr { inner: v.clone() })?
                            .into_bound(py)
                            .into_any(),
                        None => py.None().into_bound(py),
                    },
                )?;
                let config = option_object(py, options)?;
                if let Some(dtype) = result_type {
                    config.set_item(
                        "__vail_result_type",
                        Py::new(py, NativeType::new_type(dtype.clone()))?,
                    )?;
                }
                d.set_item("options", config)?;
                d.set_item("init", projection_view(py, init)?.into_any())?;
            }
            Expr::AggregateFold {
                initial,
                seq,
                combine,
                state_parameter,
                left_parameter,
                right_parameter,
            } => {
                d.set_item("op", "aggregate_fold")?;
                d.set_item(
                    "initial",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: initial.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "seq",
                    Py::new(py, NativeExpr { inner: seq.clone() })?
                        .into_bound(py)
                        .into_any(),
                )?;
                d.set_item(
                    "combine",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: combine.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "state_parameter",
                    state_parameter.into_pyobject(py)?.into_any(),
                )?;
                d.set_item(
                    "left_parameter",
                    left_parameter.into_pyobject(py)?.into_any(),
                )?;
                d.set_item(
                    "right_parameter",
                    right_parameter.into_pyobject(py)?.into_any(),
                )?;
            }
            Expr::AggGroupBy { key, body } => {
                d.set_item("op", "agg_group_by")?;
                d.set_item(
                    "key",
                    Py::new(py, NativeExpr { inner: key.clone() })?
                        .into_bound(py)
                        .into_any(),
                )?;
                d.set_item(
                    "body",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: body.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::AggArray {
                value,
                parameter,
                body,
            } => {
                d.set_item("op", "agg_array")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("parameter", parameter.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "body",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: body.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::AggFilter { predicate, body } => {
                d.set_item("op", "agg_filter")?;
                d.set_item(
                    "predicate",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: predicate.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "body",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: body.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::AggExplode {
                value,
                parameter,
                body,
            } => {
                d.set_item("op", "agg_explode")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("parameter", parameter.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "body",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: body.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::LambdaVariable { name } => {
                d.set_item("op", "lambda_variable")?;
                d.set_item("name", name.into_pyobject(py)?.into_any())?;
            }
            Expr::NestedFunction {
                function,
                args,
                dtype,
            } => {
                d.set_item("op", "nested_function")?;
                d.set_item("function", function.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "args",
                    PyList::new(
                        py,
                        args.iter()
                            .map(|v| Py::new(py, NativeExpr::from_expr(v.clone())))
                            .collect::<PyResult<Vec<_>>>()?,
                    )?
                    .into_any(),
                )?;
                d.set_item(
                    "dtype",
                    Py::new(py, NativeType::new_type(dtype.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
            }
            Expr::CollectionCast { value, dtype } => {
                d.set_item("op", "collection_cast")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "dtype",
                    Py::new(py, NativeType::new_type(dtype.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
            }
            Expr::IntegerBinary {
                operator,
                left,
                right,
            } => {
                d.set_item("op", "integer_binary")?;
                d.set_item(
                    "operator",
                    format!("{:?}", operator)
                        .to_lowercase()
                        .into_pyobject(py)?
                        .into_any(),
                )?;
                d.set_item(
                    "left",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: left.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "right",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: right.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::StringFunction { function, args } => {
                d.set_item("op", "string_function")?;
                d.set_item("function", function.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "args",
                    PyList::new(
                        py,
                        args.iter()
                            .map(|v| Py::new(py, NativeExpr::from_expr(v.clone())))
                            .collect::<PyResult<Vec<_>>>()?,
                    )?
                    .into_any(),
                )?;
            }
            Expr::Tuple { elements } => {
                d.set_item("op", "tuple")?;
                d.set_item(
                    "elements",
                    PyList::new(
                        py,
                        elements
                            .iter()
                            .map(|v| Py::new(py, NativeExpr::from_expr(v.clone())))
                            .collect::<PyResult<Vec<_>>>()?,
                    )?
                    .into_any(),
                )?;
            }
            Expr::ArrayMap {
                value,
                parameter,
                body,
            } => {
                d.set_item("op", "array_map")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("parameter", parameter.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "body",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: body.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ArrayFold {
                value,
                zero,
                accumulator,
                parameter,
                body,
                scan,
            } => {
                d.set_item("op", "array_fold")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "zero",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: zero.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("accumulator", accumulator.into_pyobject(py)?.into_any())?;
                d.set_item("parameter", parameter.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "body",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: body.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("scan", scan.into_pyobject(py)?.to_owned().into_any())?;
            }
            Expr::ArrayPredicate {
                value,
                parameter,
                predicate,
                first,
            } => {
                d.set_item("op", "array_predicate")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("parameter", parameter.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "predicate",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: predicate.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("first", first.into_pyobject(py)?.to_owned().into_any())?;
            }
            Expr::ArrayFilter {
                value,
                parameter,
                predicate,
            } => {
                d.set_item("op", "array_filter")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("parameter", parameter.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "predicate",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: predicate.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::StreamRange { start, stop, step } => {
                d.set_item("op", "stream_range")?;
                d.set_item(
                    "start",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: start.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "stop",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: stop.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "step",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: step.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ArrayGrouped { value, size } => {
                d.set_item("op", "array_grouped")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "size",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: size.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ExternalGlobal { reference, dtype } => {
                d.set_item("op", "external_global")?;
                d.set_item("reference", reference.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "dtype",
                    Py::new(py, NativeType::new_type(dtype.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
            }
            Expr::Global { name } => {
                d.set_item("op", "global")?;
                d.set_item("name", name.into_pyobject(py)?.into_any())?;
            }
            Expr::Column { name } => {
                d.set_item("op", "column")?;
                d.set_item("name", name.into_pyobject(py)?.into_any())?;
            }
            Expr::Literal { dtype, value } => {
                d.set_item("op", "literal")?;
                d.set_item(
                    "dtype",
                    Py::new(py, NativeType::new_type(dtype.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
                d.set_item("value", option_object(py, value)?)?;
            }
            Expr::Binary {
                operator,
                left,
                right,
            } => {
                d.set_item("op", "binary")?;
                d.set_item(
                    "operator",
                    format!("{:?}", operator)
                        .to_lowercase()
                        .into_pyobject(py)?
                        .into_any(),
                )?;
                d.set_item(
                    "left",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: left.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "right",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: right.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::Unary { operator, value } => {
                d.set_item("op", "unary")?;
                d.set_item(
                    "operator",
                    format!("{:?}", operator)
                        .to_lowercase()
                        .into_pyobject(py)?
                        .into_any(),
                )?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::Cast { dtype, value } => {
                d.set_item("op", "cast")?;
                d.set_item(
                    "dtype",
                    Py::new(py, NativeType::new_type(dtype.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ToString { value } => {
                d.set_item("op", "to_string")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::GetField { value, name } => {
                d.set_item("op", "get_field")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("name", name.into_pyobject(py)?.into_any())?;
            }
            Expr::Interval {
                start,
                end,
                includes_start,
                includes_end,
            } => {
                d.set_item("op", "interval")?;
                d.set_item(
                    "start",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: start.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "end",
                    Py::new(py, NativeExpr { inner: end.clone() })?
                        .into_bound(py)
                        .into_any(),
                )?;
                d.set_item(
                    "includes_start",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: includes_start.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "includes_end",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: includes_end.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::IntervalContains { interval, point } => {
                d.set_item("op", "interval_contains")?;
                d.set_item(
                    "interval",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: interval.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "point",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: point.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::IntervalOverlaps { left, right } => {
                d.set_item("op", "interval_overlaps")?;
                d.set_item(
                    "left",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: left.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "right",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: right.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::Struct { fields } => {
                d.set_item("op", "struct")?;
                d.set_item("fields", projection_view(py, fields)?.into_any())?;
            }
            Expr::Switch {
                value,
                parameter,
                cases,
                default,
                missing,
            } => {
                d.set_item("op", "switch")?;
                d.set_item("parameter", parameter)?;
                for (name, node) in [("value", value), ("default", default)] {
                    d.set_item(
                        name,
                        Py::new(
                            py,
                            NativeExpr {
                                inner: node.clone(),
                            },
                        )?,
                    )?;
                }
                if let Some(missing) = missing {
                    d.set_item(
                        "missing",
                        Py::new(
                            py,
                            NativeExpr {
                                inner: missing.clone(),
                            },
                        )?,
                    )?;
                } else {
                    d.set_item("missing", py.None())?;
                }
                let pairs = cases
                    .iter()
                    .map(|(condition, result)| {
                        PyList::new(
                            py,
                            [
                                Py::new(py, NativeExpr::from_expr(condition.clone()))?,
                                Py::new(py, NativeExpr::from_expr(result.clone()))?,
                            ],
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                d.set_item("cases", PyList::new(py, pairs)?)?;
            }
            Expr::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                d.set_item("op", "if_else")?;
                d.set_item(
                    "condition",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: condition.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "consequent",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: consequent.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "alternate",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: alternate.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::Die { message, dtype } => {
                d.set_item("op", "die")?;
                d.set_item(
                    "message",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: message.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "dtype",
                    Py::new(py, NativeType::new_type(dtype.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
            }
            Expr::IsMissing { value } => {
                d.set_item("op", "is_missing")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::Array {
                element_type,
                elements,
            } => {
                d.set_item("op", "array")?;
                d.set_item(
                    "element_type",
                    Py::new(py, NativeType::new_type(element_type.clone()))?
                        .into_bound(py)
                        .into_any(),
                )?;
                d.set_item(
                    "elements",
                    PyList::new(
                        py,
                        elements
                            .iter()
                            .map(|v| Py::new(py, NativeExpr::from_expr(v.clone())))
                            .collect::<PyResult<Vec<_>>>()?,
                    )?
                    .into_any(),
                )?;
            }
            Expr::ArrayFlatten { value } => {
                d.set_item("op", "array_flatten")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ArrayLength { value } => {
                d.set_item("op", "array_length")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ArrayIndex { value, index } => {
                d.set_item("op", "array_index")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "index",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: index.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ArraySlice {
                value,
                start,
                stop,
                step,
            } => {
                d.set_item("op", "array_slice")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "start",
                    match start {
                        Some(v) => Py::new(py, NativeExpr { inner: v.clone() })?
                            .into_bound(py)
                            .into_any(),
                        None => py.None().into_bound(py),
                    },
                )?;
                d.set_item(
                    "stop",
                    match stop {
                        Some(v) => Py::new(py, NativeExpr { inner: v.clone() })?
                            .into_bound(py)
                            .into_any(),
                        None => py.None().into_bound(py),
                    },
                )?;
                d.set_item(
                    "step",
                    match step {
                        Some(v) => Py::new(py, NativeExpr { inner: v.clone() })?
                            .into_bound(py)
                            .into_any(),
                        None => py.None().into_bound(py),
                    },
                )?;
            }
            Expr::ArrayReduce {
                operation,
                filter_missing,
                value,
            } => {
                d.set_item("op", "array_reduce")?;
                d.set_item(
                    "operation",
                    format!("{:?}", operation)
                        .to_lowercase()
                        .into_pyobject(py)?
                        .into_any(),
                )?;
                d.set_item(
                    "filter_missing",
                    filter_missing.into_pyobject(py)?.to_owned().into_any(),
                )?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::Locus {
                reference,
                contig,
                position,
            } => {
                d.set_item("op", "locus")?;
                d.set_item("reference", reference_object(py, reference)?)?;
                d.set_item(
                    "contig",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: contig.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "position",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: position.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::LocusOp { operation, value } => {
                d.set_item("op", "locus_op")?;
                d.set_item("operation", operation.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::LocusValid {
                reference,
                contig,
                position,
            } => {
                d.set_item("op", "locus_valid")?;
                d.set_item("reference", reference_object(py, reference)?)?;
                d.set_item(
                    "contig",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: contig.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item(
                    "position",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: position.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::LocusFromGlobalPosition {
                reference,
                position,
            } => {
                d.set_item("op", "locus_from_global_position")?;
                d.set_item("reference", reference_object(py, reference)?)?;
                d.set_item(
                    "position",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: position.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ParseLocus { reference, value } => {
                d.set_item("op", "parse_locus")?;
                d.set_item("reference", reference_object(py, reference)?)?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
            }
            Expr::ParseCsvLine {
                value,
                delimiter,
                quote,
                missing,
                expected_fields,
            } => {
                d.set_item("op", "parse_csv_line")?;
                d.set_item(
                    "value",
                    Py::new(
                        py,
                        NativeExpr {
                            inner: value.clone(),
                        },
                    )?
                    .into_bound(py)
                    .into_any(),
                )?;
                d.set_item("delimiter", delimiter.into_pyobject(py)?.into_any())?;
                d.set_item("quote", quote.into_pyobject(py)?.into_any())?;
                d.set_item("missing", missing.into_pyobject(py)?.into_any())?;
                d.set_item(
                    "expected_fields",
                    expected_fields.into_pyobject(py)?.into_any(),
                )?;
            }
            Expr::RuntimeLiteral { value } => {
                d.set_item("op", "literal")?;
                d.set_item(
                    "dtype",
                    NativeType::new_type(
                        Type::from_arrow(&value.data_type()).map_err(value_error)?,
                    ),
                )?;
                d.set_item(
                    "value",
                    crate::arrow_memory::NativeArrow::single(
                        arrow_array::RecordBatch::try_new(
                            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                                "value",
                                value.data_type(),
                                true,
                            )])),
                            vec![value.to_array_of_size(1).map_err(value_error)?],
                        )
                        .map_err(value_error)?,
                    ),
                )?;
            }
        }
        Ok(d)
    }
}

fn projection_view<'py>(
    py: Python<'py>,
    fields: &[(String, Expr)],
) -> PyResult<Bound<'py, PyList>> {
    PyList::new(
        py,
        fields
            .iter()
            .map(|(n, v)| {
                PyList::new(
                    py,
                    vec![
                        n.into_pyobject(py)?.into_any(),
                        Py::new(py, NativeExpr::from_expr(v.clone()))?
                            .into_bound(py)
                            .into_any(),
                    ],
                )
            })
            .collect::<PyResult<Vec<_>>>()?,
    )
}
