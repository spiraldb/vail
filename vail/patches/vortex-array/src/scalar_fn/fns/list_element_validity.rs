// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Typed, capture-free list-element logical validity projection.
//! This ordinary array operation does not certify selective storage decoding.

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use prost::Message;
use vortex_error::{VortexResult, vortex_bail, vortex_ensure, vortex_err};
use vortex_proto::expr as pb;
use vortex_session::{VortexSession, registry::CachedId};

use crate::arrays::listview::{ListViewArrayExt, ListViewArraySlotsExt};
use crate::arrays::{ListView, ListViewArray};
use crate::dtype::{DType, FieldName, Nullability};
use crate::expr::proto::{ExprSerializeProtoExt, deserialize_expr_proto};
use crate::expr::{BoundExpression, Expression, root};
use crate::scalar_fn::fns::cast::Cast;
use crate::scalar_fn::fns::list_field::{ListField, list_field};
use crate::scalar_fn::fns::list_project::Project;
use crate::scalar_fn::{
    Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable, ScalarFnVTableExt,
};
use crate::{ArrayRef, Canonical, ExecutionCtx, IntoArray};

/// A resolved field selector bound once to its complete original input schema.
/// The bound cache is derived from immutable selector/schema and is not a body callback.
#[derive(Clone, Debug)]
pub struct ElementValidityPath {
    selector: Expression,
    bound_selector: BoundExpression,
    input_dtype: DType,
    output_dtype: DType,
    element_projection: BoundExpression,
}

impl PartialEq for ElementValidityPath {
    fn eq(&self, other: &Self) -> bool {
        self.selector == other.selector && self.input_dtype == other.input_dtype
    }
}
impl Eq for ElementValidityPath {}
impl Hash for ElementValidityPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.selector.hash(state);
        self.input_dtype.hash(state);
    }
}
impl Display for ElementValidityPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "logical_validity({})", self.selector)
    }
}

impl ElementValidityPath {
    /// Prepared genuine Bool projection for an element LayoutReader. Outer list
    /// validity remains owned by the list reader and is not applied here.
    pub fn input_dtype(&self) -> &DType {
        &self.input_dtype
    }

    pub fn element_projection(&self) -> &BoundExpression {
        &self.element_projection
    }

    fn from_selector(selector: Expression) -> VortexResult<Self> {
        // Closed serialized grammar: ListField(Project?(Cast(Root, original schema))).
        // The existing prepared FieldPath supplies positions/requiredness; the Cast
        // is an immutable schema annotation and is a no-op on the exact input.
        vortex_ensure!(
            selector.is::<ListField>() && selector.children().len() == 1,
            "Element validity requires a resolved ListField selector"
        );
        let mut input = &selector.children()[0];
        if input.is::<Project>() {
            vortex_ensure!(
                input.children().len() == 1,
                "Invalid field projection arity"
            );
            input = &input.children()[0];
        }
        let Some(input_dtype) = input.as_opt::<Cast>() else {
            vortex_bail!("Element validity selector lacks original schema");
        };
        vortex_ensure!(
            input.children().len() == 1 && input.children()[0].is_root(),
            "Element validity selector is not capture-free Root"
        );
        let input_dtype = input_dtype.clone();
        let DType::List(_, nullable) = &input_dtype else {
            vortex_bail!("Element validity currently requires List input");
        };
        let output_dtype = DType::List(Arc::new(DType::Bool(Nullability::NonNullable)), *nullable);
        let bound_selector = selector.bind(&input_dtype)?;
        let DType::List(projected_element, _) = bound_selector.children()[0].dtype() else {
            vortex_bail!("Element selector input is not List");
        };
        let element_expression = selector
            .as_opt::<ListField>()
            .ok_or_else(|| vortex_err!("Element selector is not ListField"))?
            .element_expression(projected_element)?;
        let DType::List(original_element, _) = &input_dtype else {
            unreachable!()
        };
        let element_projection =
            crate::expr::is_not_null(element_expression).bind(original_element)?;
        Ok(Self {
            selector,
            bound_selector,
            input_dtype,
            output_dtype,
            element_projection,
        })
    }
}

/// Project every selected field's LOGICAL validity to a genuine non-null Bool.
#[derive(Clone)]
pub struct ListElementValidity;

impl ScalarFnVTable for ListElementValidity {
    type Options = ElementValidityPath;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.list.element_validity");
        *ID
    }
    fn serialize(&self, options: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        // Reuse normal expression serialization, including the existing typed
        // Cast and ListField metadata. No arbitrary lambda/callback serde format.
        Ok(Some(options.selector.serialize_proto()?.encode_to_vec()))
    }
    fn deserialize(&self, metadata: &[u8], session: &VortexSession) -> VortexResult<Self::Options> {
        ElementValidityPath::from_selector(deserialize_expr_proto(
            &pb::Expr::decode(metadata)?,
            session,
        )?)
    }
    fn arity(&self, _: &Self::Options) -> Arity {
        Arity::Exact(1)
    }
    fn child_name(&self, _: &Self::Options, _: usize) -> ChildName {
        ChildName::from("list")
    }
    fn return_dtype(&self, options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
        vortex_ensure!(
            args[0] == options.input_dtype,
            "Element validity original input schema changed"
        );
        Ok(options.output_dtype.clone())
    }
    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let input = args.get(0)?;
        vortex_ensure!(
            input.dtype() == &options.input_dtype,
            "Element validity execution input schema changed"
        );
        let selected = input
            .apply_bound(&options.bound_selector)?
            .execute::<Canonical>(ctx)?
            .into_array()
            .try_downcast::<ListView>()
            .map_err(|_| vortex_err!("Element validity canonical input is not a list"))?;
        let values = selected
            .elements()
            .validity()?
            .to_array(selected.elements().len());
        let result = ListViewArray::try_new(
            values,
            selected.offsets().clone(),
            selected.sizes().clone(),
            selected.validity()?,
        )?;
        // SAFETY: this changes only element type/values, preserving list cardinality
        // and the selector's exact offsets/sizes/outer logical validity.
        Ok(unsafe { result.with_zero_copy_to_list(selected.is_zero_copy_to_list()) }.into_array())
    }
}

/// Resolve a Root-relative field path once, preserving original input schema and
/// ancestor/target logical nulls. First scope is variable List; other shapes fail
/// explicitly rather than silently changing their type or missingness.
pub fn list_element_validity(
    child: Expression,
    dtype: &DType,
    names: &[FieldName],
) -> VortexResult<Expression> {
    let schema_root = Cast.new_expr(dtype.clone(), vec![root()]);
    let selector = list_field(schema_root, dtype, names)?;
    let options = ElementValidityPath::from_selector(selector)?;
    Ok(ListElementValidity.new_expr(options, vec![child]))
}
