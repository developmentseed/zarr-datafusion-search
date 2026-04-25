# zarr-datafusion-search

A prototype for querying STAC or CMR style _metadata_ about Zarr arrays and groups
using [DataFusion](https://datafusion.apache.org/), an extensible query engine written
in Rust.

This concept was conceived by the team at [Earthmover](https://www.earthmover.io/)
and is outlined in their whitepaper *Level 2 Data Collections in Zarr / Icechunk*.

## Why

The Earthmover whitepaper outlines several rationales for storing
_metadata_ in a Zarr store.  The most compelling cases are

- **Heterogeneous Arrays** - With the advent of Virtualizarr we are  often representing chunks from source files that we don't control.  For Level 2 and Level 3 datasets like Sentinel 2 this means that virtual Zarr arrays have varying `dtypes`, `codecs` and `crs` values.
If the source arrays are heterogeneous, they cannot be concatenated along a dimension to form a single datacube.  Because of this we need an alternative to select or discover these arrays other than the normal coordinate or dimensional slicing we use with datacubes.

- **Synchronization** - Our current metadata management solutions (STAC, CMR, ODC) all use disconnected metadata stores which reference raw data assets in object storage.
This can present problems as systems require complex, fragile orchestration to maintain consistency between metadata indexes and source data.  Using Icechunk as store can alleviate this as array data and metadata updates can be completed in a single atomic transaction.


- **[User Guide](user-guide/installation.md)** — Installation, quickstart
- **[API Reference](api/zarr-table.md)** — `ZarrTable` class documentation
- **[Developer Docs](DEVELOP.md)** — Contributing and running benchmarks
