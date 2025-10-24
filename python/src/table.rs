use std::sync::Arc;

use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyCapsule, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::AnyObjectStore;
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

    #[classmethod]
    pub(crate) fn from_icechunk<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
        group_path: PyBackedStr,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = session
            .getattr("_session")?
            .call_method0("as_bytes")?
            .extract()?;
        let icechunk_session = icechunk::session::Session::from_bytes(bytes).unwrap();

        dbg!("Created icechunk session from msgpack serialization");
        dbg!(icechunk_session.config());

        future_into_py(py, async move {
            let table_provider = ZarrTableProvider::new_icechunk(icechunk_session, &group_path)
                .await
                .unwrap();
            Ok(Self(Arc::new(table_provider)))
        })
    }

    #[classmethod]
    pub(crate) fn from_obstore<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        store: AnyObjectStore,
        group_path: PyBackedStr,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = store.into_dyn();
        future_into_py(py, async move {
            let table_provider = ZarrTableProvider::new_object_store(store, &group_path)
                .await
                .unwrap();
            Ok(Self(Arc::new(table_provider)))
        })
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
