//! Arrow C Stream ownership transfer. No IPC buffers or row-value encoding.
use crate::value_error;
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::SchemaRef;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods};

pub(crate) fn import(value: &Bound<'_, PyAny>) -> PyResult<ArrowArrayStreamReader> {
    let capsule = value.call_method0("__arrow_c_stream__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let ptr = capsule.pointer_checked(Some(c"arrow_array_stream"))?;
    // Arrow's protocol transfers ownership by moving the stream and nulling
    // the producer's release callback. No Python runs while the pointer is used.
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(ptr.as_ptr().cast()) };
    ArrowArrayStreamReader::try_new(stream).map_err(value_error)
}

pub(crate) fn batch(value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let reader = import(value)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(value_error)?;
    arrow_select::concat::concat_batches(&schema, &batches).map_err(value_error)
}

#[pyclass(frozen)]
pub(crate) struct NativeArrow {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}
impl NativeArrow {
    pub fn single(batch: RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            batches: vec![batch],
        }
    }
}
#[pymethods]
impl NativeArrow {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if requested_schema.is_some_and(|s| !s.is_none()) {
            return Err(value_error("Arrow schema negotiation is not supported"));
        }
        let reader = RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        );
        PyCapsule::new_with_value(
            py,
            FFI_ArrowArrayStream::new(Box::new(reader)),
            c"arrow_array_stream",
        )
    }
}
