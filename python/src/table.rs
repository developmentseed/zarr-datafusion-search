use std::sync::Arc;

use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyCapsule;
use zarr_datafusion_search::table_provider::ZarrTableProvider;

#[pyclass(name = "ZarrTable", frozen)]
pub struct PyZarrTable(Arc<ZarrTableProvider>);

#[pymethods]
impl PyZarrTable {
    #[new]
    pub fn new(zarr_path: String, group_path: PyBackedStr) -> PyResult<Self> {
        let table_provider =
            ZarrTableProvider::new_filesystem(zarr_path, &group_path).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create ZarrTableProvider: {}",
                    e
                ))
            })?;
        Ok(PyZarrTable(Arc::new(table_provider)))
    }

    pub fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_provider".into();

        let provider = FFI_TableProvider::new(self.0.clone(), false, None);
        PyCapsule::new(py, provider, Some(name))
    }
}
