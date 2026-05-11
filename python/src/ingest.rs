use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::AnyObjectStore;
use stac::Bbox;
use stac::api::{Fields, Filter, Items, Search, Sortby};
use std::sync::Arc;
use zarrs_storage::AsyncReadableWritableListableStorageTraits;

use zarr_datafusion_search::ingest::ingest_stac_api;

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
/// store : ObjectStore, optional
///     An ``obstore`` object store to write into. Mutually exclusive with
///     ``session``.
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
    store: Option<AnyObjectStore>,
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
    let zarr_store: Arc<dyn AsyncReadableWritableListableStorageTraits> =
        match (&icechunk_store, store) {
            (Some(ic), None) => Arc::clone(ic) as _,
            (None, Some(s)) => Arc::new(zarrs_object_store::AsyncObjectStore::new(s.into_dyn())),
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
