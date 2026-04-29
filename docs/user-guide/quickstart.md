# Quickstart

This guide walks through querying a Zarr store with SQL in three steps:
create a store, register it as a DataFusion table, and run queries.

## 1. Create a Zarr store

A `zarr-datafusion-search` store is a Zarr v3 group containing a `meta/`
subgroup. Each 1D array inside `meta/` becomes a SQL column. The simplest
way to build a test store is with `zarr-python` and `shapely`:

```python
import numpy as np
import shapely
import zarr
from zarr.dtype import VariableLengthBytes, VariableLengthUTF8

root = zarr.open_group("my_store.zarr", mode="w", zarr_format=3)
meta = root.create_group("meta")

# Timestamps
meta.create_array(
    "date",
    data=np.array(["2023-01-01", "2023-01-02", "2023-01-03"], dtype="datetime64[ms]"),
)

# String metadata
collection = meta.create_array(
    "collection",
    shape=(3,),
    dtype=VariableLengthUTF8(),
)
collection[:] = ["sentinel-2", "sentinel-2", "landsat-8"]

# Bounding boxes stored as WKB
bbox = meta.create_array(
    "bbox",
    shape=(3,),
    dtype=VariableLengthBytes(),
)
bbox[:] = shapely.to_wkb([
    shapely.box(-10.0, -10.0, 10.0, 10.0),
    shapely.box(-20.0, -20.0, 20.0, 20.0),
    shapely.box( 30.0,  30.0, 50.0, 50.0),
])
```
Note that the chunk size and shape must be the same for all of our `meta` arrays.  See
[zarr structure](../architecture/zarr.md) for more details on why.   
:
## 2. Register the store

Use `ZarrTable.from_obstore` to open the store through
[obstore](https://github.com/developmentseed/obstore), then register it
with a DataFusion `SessionContext`:

```python
from datafusion import SessionContext
from obstore.store import LocalStore
from zarr_datafusion_search import ZarrTable

store = LocalStore("my_store.zarr")
zarr_table = await ZarrTable.from_obstore(store, "/meta")

ctx = SessionContext()
ctx.register_table("my_data", zarr_table)
```

## 3. Query with SQL

Once registered, any DataFusion SQL query works against the table:

```python
# Inspect the schema
df = ctx.sql("SELECT * FROM my_data LIMIT 5")
print(df.schema())
df.show()

# Filter by date
df = ctx.sql("""
    SELECT date, collection
    FROM my_data
    WHERE date >= '2023-01-02'
""")
df.show()
```

## 4. Spatial queries

Spatial SQL functions are provided by `geodatafusion`, which is included as a
dependency and requires no separate install. Call `register_all` to make the
functions available in your session:

```python
from geodatafusion import register_all

register_all(ctx)

df = ctx.sql("""
    SELECT date, collection
    FROM my_data
    WHERE ST_Intersects(
        bbox,
        ST_GeomFromText('POLYGON((-15 -15, -15 15, 15 15, 15 -15, -15 -15))')
    )
""")
df.show()
```

## Using an Icechunk store

If your data lives in an [Icechunk](https://icechunk.io) repository, open a
read-only session and pass it directly to `ZarrTable.from_icechunk`:

```python
import icechunk
from zarr_datafusion_search import ZarrTable

storage = icechunk.local_filesystem_storage("my_repo")
repo = icechunk.Repository.open(storage)
session = repo.readonly_session("main")

zarr_table = await ZarrTable.from_icechunk(session=session, group_path="/meta")

ctx = SessionContext()
ctx.register_table("my_data", zarr_table)
```

From here the SQL interface is identical to the local store example above.
