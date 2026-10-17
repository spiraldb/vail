//! Value-independent aggregate signatures shared by scan typing and execution.
//! Runtime initializer values remain lazy; factories validate their values when
//! the action opens. This module checks types, names, arity and static options.
use crate::expressions::{ExprError, PrimitiveType as P, Type};
use serde_json::Value;

fn invalid(message: impl Into<String>) -> ExprError {
    ExprError::Invalid(message.into())
}
fn primitive(p: P) -> Type {
    Type::Primitive(p)
}
fn array(t: Type) -> Type {
    Type::Array { array: Box::new(t) }
}
fn fields(items: &[(&str, Type)]) -> Type {
    Type::Struct {
        r#struct: items
            .iter()
            .map(|(name, t)| ((*name).into(), t.clone()))
            .collect(),
    }
}
fn numeric(t: &Type) -> bool {
    matches!(
        t,
        Type::Primitive(P::Int32 | P::Int64 | P::Float32 | P::Float64)
    )
}
fn field<'a>(t: &'a Type, name: &str) -> Result<&'a Type, ExprError> {
    match t {
        Type::Struct { r#struct: fields } => fields.iter().find(|(n, _)| n == name).map(|(_, t)| t),
        _ => None,
    }
    .ok_or_else(|| invalid(format!("aggregate requires field {name:?}")))
}
// Factories consume packed fields positionally. Keep names, order and arity
// together so checked typing cannot describe a different argument layout.
fn packed(input: &Type, required: &[&str], optional: Option<&str>) -> Result<(), ExprError> {
    let Type::Struct { r#struct: fields } = input else {
        return Err(invalid("aggregate requires packed struct input"));
    };
    let valid_len = fields.len() == required.len()
        || optional.is_some_and(|name| {
            fields.len() == required.len() + 1 && fields[required.len()].0 == name
        });
    require(
        valid_len
            && fields
                .iter()
                .zip(required)
                .all(|((name, _), expected)| name == expected),
        "aggregate packed field names/order/arity mismatch",
    )
}

fn require(condition: bool, message: &str) -> Result<(), ExprError> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}

pub fn result_type(
    function: &str,
    input: Option<&Type>,
    options: &Value,
    init: &[(String, Type)],
) -> Result<Type, ExprError> {
    let options = options
        .as_object()
        .ok_or_else(|| invalid("aggregate options must be an object"))?;
    let allowed: &[&str] = match function {
        "count" | "sum" | "product" | "min" | "max" | "mean" | "stddev_pop" | "count_where"
        | "fraction" | "any" | "all" | "collect" | "collect_as_set" | "corr" | "inbreeding"
        | "info_score" | "ndarray_sum" => &[],
        "take" => &["n", "ordered"],
        "hist" => &["start", "end", "bins"],
        "approx_quantiles" | "approx_median" => &["quantiles", "scalar", "k"],
        "linreg" => &["nested_dim", "n_predictors"],
        "downsample" => &["n_divisions"],
        "hardy_weinberg_test" => &["one_sided"],
        "call_stats" => &["n_alleles"],
        "approx_cdf" => {
            return Err(invalid(
                "approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable",
            ));
        }
        _ => return Err(invalid(format!("unsupported aggregate {function:?}"))),
    };
    for (key, value) in options {
        require(
            allowed.contains(&key.as_str())
                || matches!(
                    key.as_str(),
                    "__vail_scan" | "__vail_exploded" | "__vail_rng_uid"
                ),
            "unknown aggregate option",
        )?;
        if matches!(
            key.as_str(),
            "ordered"
                | "scalar"
                | "one_sided"
                | "__vail_scan"
                | "__vail_exploded"
                | "__vail_rng_uid"
        ) {
            require(
                value.is_boolean(),
                "aggregate Boolean option has invalid type",
            )?;
        }
        if matches!(
            key.as_str(),
            "n" | "nested_dim" | "n_predictors" | "n_divisions" | "bins" | "n_alleles"
        ) {
            require(
                value.as_u64().is_some(),
                &format!("aggregate option {key:?} must be a nonnegative integer"),
            )?;
        }
    }
    let i32t = primitive(P::Int32);
    let i64t = primitive(P::Int64);
    let f64t = primitive(P::Float64);
    let boolt = primitive(P::Bool);
    let mut names = std::collections::HashSet::new();
    for (name, ty) in init {
        require(names.insert(name), "duplicate aggregate initializer")?;
        require(
            !options.contains_key(name),
            "conflicting aggregate initializer and static option",
        )?;
        let expected = match (function, name.as_str()) {
            ("hist", "start" | "end") => f64t.clone(),
            ("hist", "bins") | ("call_stats", "n_alleles") => i32t.clone(),
            ("approx_quantiles" | "approx_median", "quantiles") => array(f64t.clone()),
            _ => {
                return Err(invalid(format!(
                    "unexpected aggregate initializer {name:?}"
                )));
            }
        };
        require(*ty == expected, "aggregate initializer type mismatch")?;
    }
    let required: &[&str] = match function {
        "hist" => &["start", "end", "bins"],
        "call_stats" => &["n_alleles"],
        _ => &[],
    };
    for name in required {
        require(
            names.contains(&name.to_string()) != options.contains_key(*name),
            "aggregate parameter requires exactly one initializer or static option",
        )?;
    }
    for name in ["start", "end"] {
        if let Some(value) = options.get(name) {
            require(value.as_f64().is_some(), "hist bounds must be numeric")?;
        }
    }
    if let Some(value) = options.get("quantiles") {
        require(
            value.is_null()
                || value.as_array().is_some_and(|values| {
                    values.iter().all(|v| v.is_null() || v.as_f64().is_some())
                }),
            "quantiles option must be a numeric array",
        )?;
    }
    // An exploded count carries an array solely to describe its multiplicity.
    if function == "count" {
        require(
            input.is_none()
                || options.get("__vail_exploded").and_then(Value::as_bool) == Some(true),
            "count does not take an input",
        )?;
        return Ok(i64t);
    }
    let input = input.ok_or_else(|| invalid(format!("{function} requires input")))?;
    Ok(match function {
        "count_where" | "fraction" | "any" | "all" => {
            require(*input == boolt, "Boolean aggregate requires Boolean input")?;
            match function {
                "count_where" => i64t,
                "fraction" => f64t,
                _ => boolt,
            }
        }
        "sum" | "product" | "min" | "max" | "mean" | "stddev_pop" => {
            require(numeric(input), "numeric aggregate requires numeric input")?;
            match function {
                "min" | "max" => input.clone(),
                "sum" | "product" if matches!(input, Type::Primitive(P::Int32 | P::Int64)) => i64t,
                _ => f64t,
            }
        }
        "collect" => array(input.clone()),
        "collect_as_set" => Type::Set {
            set: Box::new(input.clone()),
        },
        "take" => {
            require(
                options
                    .get("n")
                    .and_then(Value::as_u64)
                    .is_some_and(|n| n <= i32::MAX as u64),
                "take requires int32 nonnegative n",
            )?;
            let value = if options.get("ordered").and_then(Value::as_bool) == Some(true) {
                require(
                    matches!(input, Type::Struct { r#struct: fields }
                    if fields.len() == 2 && fields[0].0 == "value" && fields[1].0 == "ordering"),
                    "ordered take requires exactly value then ordering fields",
                )?;
                field(input, "ordering")?.ensure_keyable()?;
                field(input, "value")?
            } else {
                input
            };
            array(value.clone())
        }
        "hist" => {
            require(numeric(input), "hist requires numeric input")?;
            fields(&[
                ("bin_edges", array(f64t.clone())),
                ("bin_freq", array(i64t.clone())),
                ("n_smaller", i64t.clone()),
                ("n_larger", i64t),
            ])
        }
        "approx_quantiles" | "approx_median" => {
            require(numeric(input), "quantiles require numeric input")?;
            require(
                options
                    .get("k")
                    .and_then(Value::as_i64)
                    .is_some_and(|k| k >= i32::MIN as i64 && k <= u16::MAX as i64),
                "quantile k must fit Hail's int32 parameter and the sketch's maximum k",
            )?;
            if options.get("scalar").and_then(Value::as_bool) == Some(true) {
                input.clone()
            } else {
                array(input.clone())
            }
        }
        "corr" => {
            packed(input, &["x", "y"], None)?;
            require(
                numeric(field(input, "x")?) && numeric(field(input, "y")?),
                "corr requires numeric x and y",
            )?;
            f64t
        }
        "linreg" => {
            packed(input, &["y", "x"], Some("weight"))?;
            let predictors = options.get("n_predictors").and_then(Value::as_u64);
            let x = field(input, "x")?;
            let predictor_count = predictors.unwrap_or(0) as usize;
            let predictor_type_matches = match x {
                Type::Array { array: element } => **element == f64t,
                Type::Primitive(P::Float64) => predictor_count == 1,
                Type::Struct { r#struct: columns } => {
                    columns.len() == predictor_count && columns.iter().all(|(_, ty)| *ty == f64t)
                }
                _ => false,
            };
            require(
                *field(input, "y")? == f64t && predictor_type_matches,
                "linreg requires float64 y and float64 scalar, array, or struct predictors matching n_predictors",
            )?;
            if let Ok(weight) = field(input, "weight") {
                require(*weight == f64t, "linreg weight must be float64")?;
            }
            require(
                predictors.is_some_and(|n| n > 0 && n < 128)
                    && options
                        .get("nested_dim")
                        .and_then(Value::as_u64)
                        .zip(predictors)
                        .is_some_and(|(nested, n)| nested <= n),
                "invalid linreg dimensions",
            )?;
            let doubles = array(f64t.clone());
            fields(&[
                ("beta", doubles.clone()),
                ("standard_error", doubles.clone()),
                ("t_stat", doubles.clone()),
                ("p_value", doubles),
                ("multiple_standard_error", f64t.clone()),
                ("multiple_r_squared", f64t.clone()),
                ("adjusted_r_squared", f64t.clone()),
                ("f_stat", f64t.clone()),
                ("multiple_p_value", f64t),
                ("n", i64t),
            ])
        }
        "downsample" => {
            packed(input, &["x", "y"], Some("label"))?;
            require(
                numeric(field(input, "x")?) && numeric(field(input, "y")?),
                "downsample requires numeric coordinates",
            )?;
            if let Ok(label) = field(input, "label") {
                require(
                    *label == primitive(P::Str) || *label == array(primitive(P::Str)),
                    "downsample label must be str or array<str>",
                )?;
            }
            require(
                options
                    .get("n_divisions")
                    .and_then(Value::as_u64)
                    .is_some_and(|n| (4..=i32::MAX as u64).contains(&n)),
                "downsample n_divisions must be at least four and fit int32",
            )?;
            array(Type::Tuple {
                tuple: vec![f64t.clone(), f64t, array(primitive(P::Str))],
            })
        }
        "hardy_weinberg_test" => {
            require(*input == primitive(P::Call), "HWE requires Call input")?;
            fields(&[("het_freq_hwe", f64t.clone()), ("p_value", f64t)])
        }
        "inbreeding" => {
            require(
                *field(input, "__vail_genomic_call")? == primitive(P::Call)
                    && *field(input, "__vail_genomic_prior")? == f64t,
                "inbreeding requires Call and float64 prior",
            )?;
            fields(&[
                ("f_stat", f64t.clone()),
                ("n_called", i64t.clone()),
                ("expected_homs", f64t.clone()),
                ("observed_homs", i64t),
            ])
        }
        "call_stats" => {
            require(
                *field(input, "__vail_genomic_call")? == primitive(P::Call),
                "call_stats requires Call",
            )?;
            require(
                matches!(
                    field(input, "__vail_genomic_alleles")?,
                    Type::Primitive(P::Int32) | Type::Array { .. }
                ),
                "call_stats requires allele count or array",
            )?;
            fields(&[
                ("AC", array(i32t.clone())),
                ("AF", array(f64t)),
                ("AN", i32t.clone()),
                ("homozygote_count", array(i32t)),
            ])
        }
        "info_score" => {
            require(
                matches!(input, Type::Array { array } if matches!(array.as_ref(),Type::Primitive(P::Float32 | P::Float64))),
                "info_score requires floating probability array",
            )?;
            fields(&[("score", f64t), ("n_included", i32t)])
        }
        "ndarray_sum" => {
            require(
                matches!(input, Type::Ndarray { ndarray } if numeric(&ndarray.0)),
                "ndarray_sum requires numeric ndarray",
            )?;
            input.clone()
        }
        _ => unreachable!("supported function dispatched above"),
    })
}
