//! Packed Call adaptation. Expression operations compose DataFusion arithmetic
//! and array kernels; scalar decoding is confined to ingestion/aggregate adapters.
use crate::expressions::{BinaryOperator as B, Expr, PrimitiveType as P, Type};
use std::sync::Arc;
pub use vail_ordering::call::{pack, unpack};
fn ty(p: P) -> Type {
    Type::Primitive(p)
}
fn n(v: i64) -> Expr {
    Expr::Literal {
        dtype: ty(P::Int64),
        value: serde_json::json!(v),
    }
}
fn b(op: B, l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        operator: op,
        left: Arc::new(l),
        right: Arc::new(r),
    }
}
fn c(p: P, v: Expr) -> Expr {
    Expr::Cast {
        dtype: ty(p),
        value: Arc::new(v),
    }
}
fn choose(cond: Expr, yes: Expr, no: Expr) -> Expr {
    Expr::IfElse {
        condition: Arc::new(cond),
        consequent: Arc::new(yes),
        alternate: Arc::new(no),
    }
}
fn field(value: &Expr, name: &str) -> Expr {
    Expr::GetField {
        value: Arc::new(value.clone()),
        name: name.into(),
    }
}
fn array(items: Vec<Expr>) -> Expr {
    Expr::Array {
        element_type: ty(P::Int32),
        elements: items,
    }
}
fn null(t: Type) -> Expr {
    Expr::Literal {
        dtype: t,
        value: serde_json::Value::Null,
    }
}
fn error(t: Type) -> Expr {
    Expr::Die {
        dtype: t,
        message: Arc::new(Expr::Literal {
            dtype: ty(P::Str),
            value: serde_json::json!("invalid packed Call or allele representation"),
        }),
    }
}
fn missing(value: Expr) -> Expr {
    Expr::IsMissing {
        value: Arc::new(value),
    }
}
fn word_parts(value: &Expr) -> (Expr, Expr, Expr) {
    let word = c(P::Int64, field(value, "__vail_call_word"));
    let unsigned = choose(
        b(B::Lt, word.clone(), n(0)),
        b(B::Add, word.clone(), n(1i64 << 32)),
        word.clone(),
    );
    let repr = b(B::Floordiv, unsigned.clone(), n(8));
    let ploidy = b(B::Mod, b(B::Floordiv, unsigned.clone(), n(2)), n(4));
    let phase = b(B::Ne, b(B::Mod, unsigned, n(2)), n(0));
    (repr, ploidy, phase)
}
pub fn getter(value: &Expr, name: &str) -> Option<Expr> {
    if !matches!(name, "__vail_call_alleles" | "__vail_call_phased") {
        return None;
    }
    let (repr, ploidy, phase) = word_parts(value);
    if name == "__vail_call_phased" {
        return Some(phase);
    }
    let root = b(
        B::Pow,
        c(P::Float64, b(B::Add, b(B::Mul, n(8), repr.clone()), n(1))),
        Expr::Literal {
            dtype: ty(P::Float64),
            value: serde_json::json!(0.5),
        },
    );
    let k = c(P::Int64, b(B::Div, b(B::Sub, root, n(1)), n(2)));
    let j = b(
        B::Sub,
        repr.clone(),
        b(
            B::Floordiv,
            b(B::Mul, k.clone(), b(B::Add, k.clone(), n(1))),
            n(2),
        ),
    );
    let second = choose(phase, b(B::Sub, k.clone(), j.clone()), k);
    let list = choose(
        b(B::Eq, ploidy.clone(), n(0)),
        array(vec![]),
        choose(
            b(B::Eq, ploidy.clone(), n(1)),
            array(vec![c(P::Int32, repr)]),
            choose(
                b(B::Eq, ploidy, n(2)),
                array(vec![c(P::Int32, j), c(P::Int32, second)]),
                error(Type::Array {
                    array: Box::new(ty(P::Int32)),
                }),
            ),
        ),
    );
    Some(choose(
        missing(value.clone()),
        null(Type::Array {
            array: Box::new(ty(P::Int32)),
        }),
        list,
    ))
}
pub fn construct(value: &Expr) -> Expr {
    let alleles = field(value, "__vail_call_alleles");
    let phase = field(value, "__vail_call_phased");
    let len = c(
        P::Int64,
        Expr::ArrayLength {
            value: Arc::new(alleles.clone()),
        },
    );
    let item = |i: i32| {
        c(
            P::Int64,
            Expr::ArrayIndex {
                value: Arc::new(alleles.clone()),
                index: Arc::new(Expr::Literal {
                    dtype: ty(P::Int32),
                    value: serde_json::json!(i),
                }),
            },
        )
    };
    let a = choose(b(B::Gt, len.clone(), n(0)), item(0), n(0));
    let d = choose(b(B::Eq, len.clone(), n(2)), item(1), n(0));
    let minimum = choose(b(B::Lt, a.clone(), d.clone()), a.clone(), d.clone());
    let maximum = choose(b(B::Gt, a.clone(), d.clone()), a.clone(), d.clone());
    let j = choose(phase.clone(), a.clone(), minimum);
    let k = choose(phase.clone(), b(B::Add, a.clone(), d.clone()), maximum);
    let diploid = b(
        B::Add,
        b(
            B::Floordiv,
            b(B::Mul, k.clone(), b(B::Add, k.clone(), n(1))),
            n(2),
        ),
        j,
    );
    let repr = choose(
        b(B::Eq, len.clone(), n(0)),
        n(0),
        choose(b(B::Eq, len.clone(), n(1)), a.clone(), diploid),
    );
    let invalid = [
        b(B::Gt, len.clone(), n(2)),
        b(B::Lt, a, n(0)),
        b(B::Lt, d, n(0)),
        b(B::And, b(B::Eq, len.clone(), n(2)), b(B::Gt, k, n(32767))),
        b(B::Ge, repr.clone(), n(1 << 29)),
        b(B::Lt, repr.clone(), n(0)),
    ]
    .into_iter()
    .reduce(|a, b_| b(B::Or, a, b_))
    .unwrap();
    let packed = b(
        B::Add,
        b(B::Add, b(B::Mul, repr, n(8)), b(B::Mul, len, n(2))),
        c(P::Int64, phase),
    );
    let signed = choose(
        b(B::Gt, packed.clone(), n(i32::MAX as i64)),
        b(B::Sub, packed.clone(), n(1i64 << 32)),
        packed,
    );
    let storage = Expr::Cast {
        dtype: ty(P::Call),
        value: Arc::new(Expr::Struct {
            fields: vec![("__vail_call_word".into(), c(P::Int32, signed))],
        }),
    };
    choose(
        missing(value.clone()),
        null(ty(P::Call)),
        choose(invalid, error(ty(P::Call)), storage),
    )
}
