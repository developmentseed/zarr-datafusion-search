# zarr-datafusion-search

This is a prototype for being able to query _metadata_ about Zarr arrays using [DataFusion](https://datafusion.apache.org/), an extensible query engine written in Rust.

## Zarr Schema

In particular, we assume there is a Zarr store with multiple 1-dimensional arrays:

- Inside a Zarr group named `"meta"`
    - An array named `"date"` with `n` timestamps, stored as a numpy `datetime64[ms]` array
    - An array named `"collection"` with `n` string values, stored as a `VariableLengthUTF8` array
    - An array named `"bbox"` with `n` string values, stored as a `VariableLengthUTF8` array, where each string is a WKT-encoded Polygon (or MultiPolygon) with the bounding box of that Zarr record.

        In the future, we will likely use a binary encoding like WKB, but Zarr's binary dtype is [not currently well-specified](https://github.com/zarr-developers/zarr-python/issues/3517).

This data schema may change over time.

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

