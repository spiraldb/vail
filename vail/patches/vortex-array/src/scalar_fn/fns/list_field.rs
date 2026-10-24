// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Typed list-element field extraction, preserving all logical parent masks.

use crate::arrays::listview::{ListViewArrayExt, ListViewArraySlotsExt};
use crate::arrays::struct_::StructArrayExt;
use crate::arrays::{ListView, ListViewArray, StructArray};
use crate::builtins::ArrayBuiltins;
use crate::dtype::{DType, FieldName};
use crate::expr::Expression;
use crate::scalar_fn::{
    Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable, ScalarFnVTableExt,
};
use crate::{ArrayRef, Canonical, ExecutionCtx, IntoArray};
use std::sync::Arc;
use vortex_error::{VortexResult, vortex_bail, vortex_err};
use vortex_session::{VortexSession, registry::CachedId};

/// Field positions and parent-nullability choices are bound at expression preparation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldPath(Vec<(usize, bool)>);

impl std::fmt::Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[derive(Clone)]
pub struct ListField;

impl ScalarFnVTable for ListField {
    type Options = FieldPath;
    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.list.field");
        *ID
    }
    fn serialize(&self, path: &FieldPath) -> VortexResult<Option<Vec<u8>>> {
        let mut bytes = Vec::with_capacity(path.0.len() * 5);
        for &(index, nullable) in &path.0 {
            bytes.extend_from_slice(
                &u32::try_from(index)
                    .map_err(|_| vortex_err!("field index too large"))?
                    .to_le_bytes(),
            );
            bytes.push(u8::from(nullable));
        }
        Ok(Some(bytes))
    }
    fn deserialize(&self, bytes: &[u8], _: &VortexSession) -> VortexResult<FieldPath> {
        if bytes.len() % 5 != 0 {
            vortex_bail!("invalid list field path");
        }
        let mut path = Vec::with_capacity(bytes.len() / 5);
        for chunk in bytes.chunks_exact(5) {
            if chunk[4] > 1 {
                vortex_bail!("invalid parent nullability");
            }
            path.push((
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as usize,
                chunk[4] == 1,
            ));
        }
        Ok(FieldPath(path))
    }
    fn arity(&self, _: &FieldPath) -> Arity {
        Arity::Exact(1)
    }
    fn child_name(&self, _: &FieldPath, _: usize) -> ChildName {
        ChildName::from("list")
    }
    fn return_dtype(&self, path: &FieldPath, args: &[DType]) -> VortexResult<DType> {
        let DType::List(element, nullable) = &args[0] else {
            vortex_bail!("list_field requires list");
        };
        let mut element = element.as_ref().clone();
        for &(index, parent_nullable) in &path.0 {
            let DType::Struct(fields, nullability) = element else {
                vortex_bail!("list field path requires struct");
            };
            if nullability.is_nullable() != parent_nullable {
                vortex_bail!("list field parent type changed");
            }
            element = fields
                .fields()
                .nth(index)
                .ok_or_else(|| vortex_err!("list field index missing"))?;
            if parent_nullable {
                element = element.with_nullability(crate::dtype::Nullability::Nullable);
            }
        }
        Ok(DType::List(Arc::new(element), *nullable))
    }
    fn execute(
        &self,
        path: &FieldPath,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let list = args
            .get(0)?
            .execute::<Canonical>(ctx)?
            .into_array()
            .try_downcast::<ListView>()
            .map_err(|_| vortex_err!("list field canonical shape mismatch"))?;
        let mut elements = list.elements().clone();
        for &(index, parent_nullable) in &path.0 {
            let parent = elements.execute::<StructArray>(ctx)?;
            elements = parent.unmasked_field(index).clone();
            if parent_nullable {
                elements = elements.mask(parent.validity()?.to_array(parent.len()))?;
            }
        }
        Ok(ListViewArray::try_new(
            elements,
            list.offsets().clone(),
            list.sizes().clone(),
            list.listview_validity(),
        )?
        .into_array())
    }
}

/// Extract a nested numeric (or other) field from every list element.
pub fn list_field(
    child: Expression,
    dtype: &DType,
    names: &[FieldName],
) -> VortexResult<Expression> {
    let DType::List(element, _) = dtype else {
        vortex_bail!("list_field requires list");
    };
    let mut element = element.as_ref().clone();
    let mut path = Vec::with_capacity(names.len());
    for name in names {
        let DType::Struct(fields, nullable) = element else {
            vortex_bail!("list field path requires struct");
        };
        let index = fields
            .find(name.as_ref())
            .ok_or_else(|| vortex_err!("list field missing: {name}"))?;
        // A single-path Project below this expression retains only one field
        // at each struct level. Bind positions in that projected schema.
        path.push((0, nullable.is_nullable()));
        element = fields
            .fields()
            .nth(index)
            .ok_or_else(|| vortex_err!("list field missing"))?;
        if nullable.is_nullable() {
            element = element.with_nullability(crate::dtype::Nullability::Nullable);
        }
    }
    let child = if names.is_empty() {
        child
    } else {
        super::list_project::project_paths(&[names.to_vec()], child, dtype)?
    };
    Ok(ListField.new_expr(FieldPath(path), vec![child]))
}
