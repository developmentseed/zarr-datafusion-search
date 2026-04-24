# ZarrTable

## Overview

`ZarrTable` is a DataFusion custom table provider that exposes a Zarr metadata
store as a SQL-queryable table.

## Import

```python
from zarr_datafusion_search import ZarrTable
```

## Constructor

```python
ZarrTable(path: str)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to the root of the Zarr store (local filesystem or object store URI) |

## Usage

Pass a `ZarrTable` instance to DataFusion's `register_table_provider`:

```python
from zarr_datafusion_search import ZarrTable
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_table_provider("my_table", ZarrTable("/path/to/store.zarr"))

df = ctx.sql("SELECT date, collection FROM my_table LIMIT 10")
df.show()
```

!!! note
    `ZarrTable` must be compiled against the same version of DataFusion as the
    `datafusion` Python package you have installed. Check `python/pyproject.toml`
    for the required `datafusion>=` version constraint.
