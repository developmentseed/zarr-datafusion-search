use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use pyo3_object_store::AnyObjectStore;
use stac::Bbox;
use stac::api::{Fields, Filter, Items, Search, Sortby};
use std::sync::Arc;
use zarrs_storage::AsyncReadableWritableListableStorageTraits;

use zarr_datafusion_search::ingest::ingest_stac_api;

/// Resolve a Python object into an `Arc<dyn ObjectStore>`.
///
/// Accepts (in order of precedence):
/// 1. A raw obstore store (e.g. `obstore.store.S3Store`)
/// 2. A `zarr.storage.ObjectStore` — extracts its `.store` obstore attribute
/// 3. A `zarr.storage.LocalStore` — creates a `LocalFileSystem` from `.root`
/// 4. A `zarr.Group` — unwraps via `.store` and recurses
fn resolve_object_store(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn ObjectStore>> {
    // 1. Try direct extraction as an obstore store
    if let Ok(os) = obj.extract::<AnyObjectStore>() {
        return Ok(os.into_dyn());
    }

    // 2. Try .store attribute — could be zarr.storage.ObjectStore (wraps obstore)
    //    or zarr.Group (wraps a zarr store)
    if let Ok(inner) = obj.getattr("store") {
        // If .store yields an obstore store, use it directly
        if let Ok(os) = inner.extract::<AnyObjectStore>() {
            return Ok(os.into_dyn());
        }
        // Otherwise recurse — handles zarr.Group → zarr store → obstore/local
        return resolve_object_store(&inner);
    }

    // 3. Try .root attribute (zarr.storage.LocalStore)
    if let Ok(root) = obj.getattr("root") {
        let path: String = root.str()?.to_string();
        let local_fs = LocalFileSystem::new_with_prefix(&path).map_err(|e| {
            PyValueError::new_err(format!("Invalid local store root '{path}': {e}"))
        })?;
        return Ok(Arc::new(local_fs) as Arc<dyn ObjectStore>);
    }

    Err(PyValueError::new_err(
        "Unrecognized store type. Expected a zarr.Group, zarr.storage.ObjectStore, \
         zarr.storage.LocalStore, or an obstore ObjectStore.",
    ))
}

// -- Flexible Python input types, following rustac-py's pattern --

/// Accepts either a Python string or a dict.
#[derive(FromPyObject)]
pub enum StringOrDict {
    String(String),
    Dict(Py<PyDict>),
}

/// Accepts either a Python string or a list of strings.
#[derive(FromPyObject)]
pub enum StringOrList {
    String(String),
    List(Vec<String>),
}

impl From<StringOrList> for Vec<String> {
    fn from(value: StringOrList) -> Vec<String> {
        match value {
            StringOrList::List(list) => list,
            StringOrList::String(s) => vec![s],
        }
    }
}

// -- Search builder (adapted from rustac-py) --

/// Builds a [`Search`] from Python arguments.
pub fn build_search<'py>(
    py: Python<'py>,
    intersects: Option<StringOrDict>,
    ids: Option<StringOrList>,
    collections: Option<StringOrList>,
    limit: Option<u64>,
    bbox: Option<Vec<f64>>,
    datetime: Option<String>,
    include: Option<StringOrList>,
    exclude: Option<StringOrList>,
    sortby: Option<StringOrList>,
    filter: Option<StringOrDict>,
    query: Option<Bound<'py, PyDict>>,
) -> PyResult<Search> {
    let mut fields = Fields::default();
    if let Some(include) = include {
        fields.include = include.into();
    }
    if let Some(exclude) = exclude {
        fields.exclude = exclude.into();
    }
    let fields = if fields.include.is_empty() && fields.exclude.is_empty() {
        None
    } else {
        Some(fields)
    };

    let query = query
        .map(|query| pythonize::depythonize(&query))
        .transpose()
        .map_err(|e| PyValueError::new_err(format!("Invalid query: {e}")))?;

    let bbox = bbox
        .map(Bbox::try_from)
        .transpose()
        .map_err(|e| PyValueError::new_err(format!("Invalid bbox: {e}")))?;

    let sortby = sortby
        .map(|sortby| {
            Vec::<String>::from(sortby)
                .into_iter()
                .map(|s| s.parse::<Sortby>().unwrap()) // parse is infallible
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let filter = filter
        .map(|filter| match filter {
            StringOrDict::Dict(cql_json) => {
                pythonize::depythonize(&cql_json.bind_borrowed(py)).map(Filter::Cql2Json)
            }
            StringOrDict::String(cql2_text) => Ok(Filter::Cql2Text(cql2_text)),
        })
        .transpose()
        .map_err(|e| PyValueError::new_err(format!("Invalid filter: {e}")))?;
    let filter = filter
        .map(|f| f.into_cql2_json())
        .transpose()
        .map_err(|e| PyValueError::new_err(format!("Invalid CQL2 filter: {e}")))?;

    let items = Items {
        limit,
        bbox,
        datetime,
        query,
        fields,
        sortby,
        filter,
        ..Default::default()
    };

    let intersects = intersects
        .map(|intersects| match intersects {
            StringOrDict::Dict(json) => pythonize::depythonize(&json.bind_borrowed(py))
                .map_err(|e| PyValueError::new_err(format!("Invalid intersects geometry: {e}"))),
            StringOrDict::String(s) => s
                .parse()
                .map_err(|e| PyValueError::new_err(format!("Invalid GeoJSON string: {e}"))),
        })
        .transpose()?;

    let ids = ids.map(Vec::from).unwrap_or_default();
    let collections = collections.map(Vec::from).unwrap_or_default();

    Search {
        items,
        intersects,
        ids,
        collections,
    }
    .normalize_datetimes()
    .map_err(|e| PyValueError::new_err(format!("Invalid datetime: {e}")))
}

// -- PyO3 function --

/// Ingest STAC API search results into a Zarr store.
///
/// Queries a STAC API, converts matching items to Arrow, and writes them as
/// 1-D Zarr arrays under the ``/meta`` group. Supports both
/// ``obstore``-backed stores and Icechunk sessions.
///
/// Parameters
/// ----------
/// url : str
///     Base URL of the STAC API
///     (e.g. ``"https://earth-search.aws.element84.com/v1"``).
/// store : zarr.Group or zarr.storage.ObjectStore or zarr.storage.LocalStore or obstore.store.ObjectStore, optional
///     A zarr group, zarr store, or obstore object store to write into.
///     Accepts a ``zarr.Group`` (extracts its underlying store),
///     a ``zarr.storage.ObjectStore`` (extracts the underlying obstore store),
///     a ``zarr.storage.LocalStore`` (uses its ``root`` path), or a raw
///     obstore store directly. Mutually exclusive with ``session``.
/// session : icechunk.Session, optional
///     An Icechunk writable session to write into. Mutually exclusive with
///     ``store``.
/// intersects : str or dict, optional
///     GeoJSON geometry (as a string or dict) to filter items by spatial
///     intersection.
/// ids : str or list[str], optional
///     One or more STAC item IDs to fetch.
/// collections : str or list[str], optional
///     One or more collection IDs to search within.
/// max_items : int, optional
///     Maximum number of items to ingest. When ``None``, all matching items
///     are fetched.
/// limit : int, optional
///     Page size for the STAC API search request.
/// bbox : list[float], optional
///     Bounding box filter as ``[west, south, east, north]``.
/// datetime : str, optional
///     Datetime filter as a single datetime or a ``/``-separated range
///     (e.g. ``"2024-01-01/2024-06-01"``).
/// include : str or list[str], optional
///     Fields to include in the response (STAC API Fields extension).
/// exclude : str or list[str], optional
///     Fields to exclude from the response (STAC API Fields extension).
/// sortby : str or list[str], optional
///     Sort order (STAC API Sort extension), e.g. ``"+datetime"`` or
///     ``"-eo:cloud_cover"``.
/// filter : str or dict, optional
///     CQL2 filter as a text string or a CQL2-JSON dict (STAC API Filter
///     extension).
/// query : dict, optional
///     Legacy STAC API query parameters.
/// chunk_size : int, default 1000
///     Number of rows per Zarr chunk for newly created arrays. Ignored when
///     appending to an existing store.
/// asset_hrefs : list[str], optional
///     Asset keys (e.g. ``["B01", "thumbnail"]``) whose ``href`` values
///     should be extracted and written as ``/meta/asset_{key}`` string
///     arrays.
///
/// Returns
/// -------
/// int
///     The number of rows written.
#[pyfunction]
#[pyo3(signature = (url, *, store=None, session=None, intersects=None, ids=None, collections=None, max_items=None, limit=None, bbox=None, datetime=None, include=None, exclude=None, sortby=None, filter=None, query=None, chunk_size=1000, asset_hrefs=None))]
#[allow(clippy::too_many_arguments)]
pub fn ingest_stac_search<'py>(
    py: Python<'py>,
    url: String,
    store: Option<Bound<'py, PyAny>>,
    session: Option<Bound<'py, PyAny>>,
    intersects: Option<StringOrDict>,
    ids: Option<StringOrList>,
    collections: Option<StringOrList>,
    max_items: Option<u64>,
    limit: Option<u64>,
    bbox: Option<Vec<f64>>,
    datetime: Option<String>,
    include: Option<StringOrList>,
    exclude: Option<StringOrList>,
    sortby: Option<StringOrList>,
    filter: Option<StringOrDict>,
    query: Option<Bound<'py, PyDict>>,
    chunk_size: usize,
    asset_hrefs: Option<Vec<String>>,
) -> PyResult<Bound<'py, PyAny>> {
    // Deserialize icechunk session from Python if provided
    let icechunk_session = match session {
        Some(ref s) => {
            let bytes: Vec<u8> = s.getattr("_session")?.call_method0("as_bytes")?.extract()?;
            Some(
                icechunk::session::Session::from_bytes(bytes)
                    .map_err(|e| PyValueError::new_err(format!("Invalid icechunk session: {e}")))?,
            )
        }
        None => None,
    };

    let search = build_search(
        py,
        intersects,
        ids,
        collections,
        limit,
        bbox,
        datetime,
        include,
        exclude,
        sortby,
        filter,
        query,
    )?;

    let asset_href_strings = asset_hrefs.unwrap_or_default();

    let icechunk_store =
        icechunk_session.map(|sess| Arc::new(zarrs_icechunk::AsyncIcechunkStore::new(sess)));

    // Resolve the store parameter: accept zarr Groups, zarr stores, or obstore stores.
    // If a zarr.Group is passed, unwrap to its underlying store first.
    let resolved_object_store: Option<Arc<dyn ObjectStore>> = match store {
        Some(ref s) => Some(resolve_object_store(s)?),
        None => None,
    };

    let zarr_store: Arc<dyn AsyncReadableWritableListableStorageTraits> =
        match (&icechunk_store, resolved_object_store) {
            (Some(ic), None) => Arc::clone(ic) as _,
            (None, Some(s)) => Arc::new(zarrs_object_store::AsyncObjectStore::new(s)),
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "Provide either 'store' or 'session', not both",
                ));
            }
            (None, None) => {
                return Err(PyValueError::new_err("Provide either 'store' or 'session'"));
            }
        };

    future_into_py(py, async move {
        let asset_refs: Vec<&str> = asset_href_strings.iter().map(|s| s.as_str()).collect();
        let rows = ingest_stac_api(
            &url,
            search,
            zarr_store,
            chunk_size,
            &asset_refs,
            max_items,
        )
        .await
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // Commit icechunk session -- the deserialized session holds a
        // connection to the same backing store, but changes only exist in
        // its in-memory ChangeSet until committed.
        if let Some(ic) = &icechunk_store {
            if rows > 0 {
                ic.session()
                    .write()
                    .await
                    .commit("Ingest STAC search results", None)
                    .await
                    .map_err(|e| PyValueError::new_err(format!("Icechunk commit failed: {e}")))?;
            }
        }

        Ok(rows)
    })
}
