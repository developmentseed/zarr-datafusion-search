use std::sync::Arc;

use datafusion::execution::TaskContextProvider;
use datafusion::prelude::SessionContext;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyCapsule, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::AnyObjectStore;
use zarr_datafusion_search::table_provider::ZarrTableProvider;

#[pyclass(name = "ZarrTable", frozen)]
pub struct PyZarrTable {
    provider: Arc<ZarrTableProvider>,
    // Session context needs to outlive the FFI boundary
    _ctx: Arc<SessionContext>,
}

#[pymethods]
impl PyZarrTable {
    #[classmethod]
    pub(crate) fn from_icechunk<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
        group_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes: Vec<u8> = session
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

            Ok(Self {
                provider: Arc::new(table_provider),
                _ctx: Arc::new(SessionContext::new()),
            })
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
            Ok(Self {
                provider: Arc::new(table_provider),
                _ctx: Arc::new(SessionContext::new()),
            })
        })
    }

    pub fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        _session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_provider".into();

        // Use the stored SessionContext to create the task context provider
        let task_ctx_provider: Arc<dyn TaskContextProvider> = self._ctx.clone();
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let provider =
            FFI_TableProvider::new(self.provider.clone(), false, None, task_ctx_provider, None);
        PyCapsule::new(py, provider, Some(name))
    }
}
