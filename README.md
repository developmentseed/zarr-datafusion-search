# zarr-datafusion-search

This is a prototype for querying STAC or CMR style _metadata_ about Zarr arrays and groups using [DataFusion](https://datafusion.apache.org/), an extensible query engine written in Rust.

This concept was conceived by the team at [Earthmover](https://www.earthmover.io/) and is outlined in their whitepaper Level 2 Data Collections in Zarr / Icechunk.

## Schema

To store this _metadata_, zarr-datafusion-search uses a convention where the Zarr store represents each metadata "field" with a 1-dimensional array in a root group named `"meta"`.

Users can define arbitrary schemas where the 1-dimensional arrays each use a `dtype` that has an equivalent Arrow type in our supported [mappings](https://github.com/developmentseed/zarr-datafusion-search/issues/12).  A concrete example might look like

- Inside a Zarr group named `"meta"`
    - A `datetime64[ms]` array named `"date"` with `n` timestamps named `"date"` with `n` timestamps.
    - A `VariableLengthUTF8` array named `"collection"` with `n` string values.
    - A `VariableLengthBytes` array named `"bbox"` with `n` binary values, where each value is a WKB-encoded Polygon (or MultiPolygon) with the bounding box of that Zarr record.

This project is under active development so these conventions may change.

## Python API

DataFusion distributes [Python bindings](https://datafusion.apache.org/python/) via the `datafusion` PyPI package.

In addition, DataFusion-Python supports [_custom table providers_](https://datafusion.apache.org/python/user-guide/data-sources.html#custom-table-provider). These allow you to define a custom data source as a standalone Rust package, compile it as its own standalone Python package, but then load it into DataFusion-Python at runtime.

> [!NOTE]
> The underlying DataFusion TableProvider ABI is not entirely stable. So for now
> you must use the same version of DataFusion-Python as the version of
> DataFusion used to compile the custom table provider.

```py
from zarr_datafusion_search import ZarrTable
from datafusion import SessionContext

# Create a new DataFusion session context
ctx = SessionContext()

# Register a specific Zarr store as a table named "zarr_data"
ctx.register_table_provider("zarr_data", ZarrTable("zarr_store.zarr"))

# Now you can run SQL queries against the Zarr data
df = ctx.sql("SELECT * FROM zarr_data;")
df.show()
```

