# Contributing

## Rust
From the project root, run:

```bash
cargo test
```

A suite of benchmarks are available (though the remote S3 benchmarks use data in a protected bucket and requires credentials). Benchmarks are in separate binaries and can be run via

```bash
cargo datetime_local
cargo bbox_colunms_local
cargo bbox_local
```

## Python bindings

From the `python/` directory, run:

```bash
uv run --no-project maturin develop --uv
```

The `--no-project` is necessary to avoid building the Rust code (in release mode) an extra time before we even reach the `maturin develop` command.

You need to add `--no-project` before any `uv run` command. For example, to run IPython:

```bash
uv run pytest
```
