//! Native semantic identities for closure-backed functions.
//!
//! A digest makes plans readable; equality always compares the complete native
//! key, signature and result type. Hash collisions never equate distinct kernels.
//! Captured executable closures are deliberately excluded from identity.
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, AggregateUDFImpl, ColumnarValue,
    GroupsAccumulator, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

fn fingerprint(key: &impl Hash) -> u64 {
    let mut state = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut state);
    state.finish()
}

pub(crate) fn name(key: &impl Hash, label: &str) -> String {
    format!("{label}_{:016x}", fingerprint(key))
}

pub(crate) struct Scalar<K> {
    key: K,
    fingerprint: u64,
    name: String,
    signature: Signature,
    output: DataType,
    function: Arc<dyn Fn(ScalarFunctionArgs) -> Result<ColumnarValue> + Send + Sync>,
}
impl<K: fmt::Debug> fmt::Debug for Scalar<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemanticScalar")
            .field("name", &self.name)
            .finish()
    }
}
impl<K> Scalar<K> {
    pub(crate) fn key(&self) -> &K {
        &self.key
    }
}
impl<K: PartialEq> PartialEq for Scalar<K> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.key == other.key
            && self.signature == other.signature
            && self.output == other.output
    }
}
impl<K: Eq> Eq for Scalar<K> {}
impl<K: Hash> Hash for Scalar<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}
impl<K: fmt::Debug + Eq + Hash + Send + Sync + 'static> ScalarUDFImpl for Scalar<K> {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        (self.function)(args)
    }
}
pub(crate) fn scalar<K: fmt::Debug + Eq + Hash + Send + Sync + 'static>(
    label: &str,
    key: K,
    signature: Signature,
    output: DataType,
    function: ScalarFunctionImplementation,
) -> ScalarUDF {
    scalar_with_args(
        label,
        key,
        signature,
        output,
        Arc::new(move |arguments| function(&arguments.args)),
    )
}

pub(crate) fn scalar_with_args<K: fmt::Debug + Eq + Hash + Send + Sync + 'static>(
    label: &str,
    key: K,
    signature: Signature,
    output: DataType,
    function: Arc<dyn Fn(ScalarFunctionArgs) -> Result<ColumnarValue> + Send + Sync>,
) -> ScalarUDF {
    let name = name(&(&key, &signature, &output), label);
    let fingerprint = fingerprint(&(&name, &key, &signature, &output));
    ScalarUDF::from(Scalar {
        fingerprint,
        key,
        name,
        signature,
        output,
        function,
    })
}

struct Aggregate<K> {
    key: K,
    fingerprint: u64,
    name: String,
    signature: Signature,
    output: DataType,
    fields: Vec<FieldRef>,
    factory: AccumulatorFactoryFunction,
    groups_factory: Option<Arc<dyn Fn() -> Result<Box<dyn GroupsAccumulator>> + Send + Sync>>,
    groups_supported: Option<Arc<dyn for<'a> Fn(&AccumulatorArgs<'a>) -> bool + Send + Sync>>,
}
impl<K: fmt::Debug> fmt::Debug for Aggregate<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemanticAggregate")
            .field("name", &self.name)
            .finish()
    }
}
impl<K: PartialEq> PartialEq for Aggregate<K> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.key == other.key
            && self.signature == other.signature
            && self.output == other.output
            && self.fields == other.fields
    }
}
impl<K: Eq> Eq for Aggregate<K> {}
impl<K: Hash> Hash for Aggregate<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}
impl<K: fmt::Debug + Eq + Hash + Send + Sync + 'static> AggregateUDFImpl for Aggregate<K> {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output.clone())
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        (self.factory)(args)
    }
    fn state_fields(&self, _: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(self.fields.clone())
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.groups_supported
            .as_ref()
            .is_some_and(|supported| supported(&args))
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if !self.groups_accumulator_supported(args) {
            return Err(datafusion_common::DataFusionError::NotImplemented(
                "groups accumulator is not supported for these arguments".into(),
            ));
        }
        self.groups_factory.as_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::NotImplemented(
                "groups accumulator is not supported".into(),
            )
        })?()
    }
}
pub(crate) fn aggregate<K: fmt::Debug + Eq + Hash + Send + Sync + 'static>(
    label: &str,
    key: K,
    inputs: Vec<DataType>,
    output: DataType,
    states: Vec<DataType>,
    factory: AccumulatorFactoryFunction,
) -> Arc<AggregateUDF> {
    let name = name(&(&key, &inputs, &output, &states), label);
    aggregate_named(&name, key, inputs, output, states, factory)
}
pub(crate) fn aggregate_named<K: fmt::Debug + Eq + Hash + Send + Sync + 'static>(
    label: &str,
    key: K,
    inputs: Vec<DataType>,
    output: DataType,
    states: Vec<DataType>,
    factory: AccumulatorFactoryFunction,
) -> Arc<AggregateUDF> {
    aggregate_named_with_groups(label, key, inputs, output, states, factory, None, None)
}
pub(crate) fn aggregate_named_with_groups<K: fmt::Debug + Eq + Hash + Send + Sync + 'static>(
    label: &str,
    key: K,
    inputs: Vec<DataType>,
    output: DataType,
    states: Vec<DataType>,
    factory: AccumulatorFactoryFunction,
    groups_factory: Option<Arc<dyn Fn() -> Result<Box<dyn GroupsAccumulator>> + Send + Sync>>,
    groups_supported: Option<Arc<dyn for<'a> Fn(&AccumulatorArgs<'a>) -> bool + Send + Sync>>,
) -> Arc<AggregateUDF> {
    let signature = Signature::exact(inputs, Volatility::Immutable);
    let fields = states
        .into_iter()
        .enumerate()
        .map(|(i, t)| Arc::new(Field::new(i.to_string(), t, true)))
        .collect::<Vec<_>>();
    let name = label.to_owned();
    let fingerprint = fingerprint(&(&name, &key, &signature, &output, &fields));
    Arc::new(AggregateUDF::from(Aggregate {
        fingerprint,
        key,
        name,
        signature,
        output,
        fields,
        factory,
        groups_factory,
        groups_supported,
    }))
}

/// Recover a typed semantic key without relying on a function's display name.
pub(crate) fn aggregate_key<K: 'static>(udf: &AggregateUDF) -> Option<&K> {
    udf.inner().downcast_ref::<Aggregate<K>>().map(|a| &a.key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Clone)]
    struct Key(u32, Arc<AtomicUsize>);
    impl PartialEq for Key {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }
    impl Eq for Key {}
    impl Hash for Key {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.1.fetch_add(1, Ordering::Relaxed);
            // Force collisions to exercise full semantic equality.
            0u8.hash(state);
        }
    }
    #[test]
    fn cached_hash_does_not_rehash_key_or_equate_collisions() {
        let calls = Arc::new(AtomicUsize::new(0));
        let make = |value| {
            scalar_with_args(
                "test",
                Key(value, calls.clone()),
                Signature::nullary(Volatility::Immutable),
                DataType::Int32,
                Arc::new(|_| {
                    Ok(ColumnarValue::Scalar(
                        datafusion_common::ScalarValue::Int32(Some(1)),
                    ))
                }),
            )
        };
        let left = make(1);
        let equal = make(1);
        let different = make(2);
        calls.store(0, Ordering::Relaxed);
        assert_eq!(fingerprint(&left), fingerprint(&equal));
        assert_eq!(fingerprint(&left), fingerprint(&different));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(left, equal);
        assert_ne!(left, different);
    }
}
