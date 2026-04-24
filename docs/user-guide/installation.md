# Installation

## Requirements

- Python 3.10 or later
- `datafusion >= 52.0`

## Install from PyPI

```bash
pip install zarr-datafusion-search
```

## Install with uv

```bash
uv add zarr-datafusion-search
```

## DataFusion version compatibility

`zarr-datafusion-search` is compiled against a specific version of the
DataFusion engine. The `datafusion` Python package you have installed must
match. Check the `datafusion>=` constraint in
[`python/pyproject.toml`](https://github.com/developmentseed/zarr-datafusion-search/blob/main/python/pyproject.toml)
for the required version.

If you see an error like `abi3 wheel is not compatible` or a symbol not found
at import time, this is almost always a DataFusion version mismatch.
