#![cfg_attr(not(test), warn(unused_crate_dependencies))]

// Use patched version of zarrs-metadata
use zarrs_metadata as _;

mod icechunk;
mod table;

use pyo3::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// Raise RuntimeWarning for debug builds
#[pyfunction]
fn check_debug_build(_py: Python) -> PyResult<()> {
    #[cfg(debug_assertions)]
    {
        use pyo3::exceptions::PyRuntimeWarning;
        use pyo3::intern;
        use pyo3::types::PyTuple;

        let warnings_mod = _py.import(intern!(_py, "warnings"))?;
        let warning = PyRuntimeWarning::new_err(
            "zarr-datafusion-search has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(_py, vec![warning])?;
        warnings_mod.call_method1(intern!(_py, "warn"), args)?;
    }
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<table::PyZarrTable>()?;

    Ok(())
}
