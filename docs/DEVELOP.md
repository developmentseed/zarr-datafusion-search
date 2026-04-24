# Contributing

## Rust

From the project root, run:

```bash
cargo test
```

A suite of benchmarks are available (though the remote S3 benchmarks use data in a
protected bucket and require credentials). Benchmarks are in separate binaries and
can be run via:

```bash
cargo bench --bench datetime_local
cargo bench --bench bbox_colunms_local
cargo bench --bench bbox_local
```

## Python bindings

From the `python/` directory, run:

```bash
uv run --no-project maturin develop --uv
```

The `--no-project` flag is necessary to avoid building the Rust code (in release
mode) an extra time before we even reach the `maturin develop` command.

Prefix all `uv run` commands with `--no-project`. For example:

```bash
uv run --no-project pytest
```
