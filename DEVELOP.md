# Contributing

## Create example Zarr files

From the root directory, run:

```bash
uv run python scripts/generate_data.py
```

This will generate `data/zarr_store.zarr` from the root directory.

## Python bindings

From the `python/` directory, run:

```bash
uv run --no-project maturin develop --uv
```

The `--no-project` is necessary to avoid building the Rust code (in release mode) an extra time before we even reach the `maturin develop` command.

You need to add `--no-project` before any `uv run` command. For example, to run IPython:

```bash
uv run --no-project ipython
```
