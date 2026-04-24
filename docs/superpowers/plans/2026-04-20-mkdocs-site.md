# MkDocs Documentation Site Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up a MkDocs Material documentation site for zarr-datafusion-search, deployable to GitHub Pages at `https://developmentseed.github.io/zarr-datafusion-search`.

**Architecture:** Root-level `mkdocs.yml` + `docs/` directory. Stub pages with `!!! note "Coming soon"` admonitions for all sections except `index.md` (populated from README) and `DEVELOP.md` (copied from root). A single GitHub Actions workflow deploys on push to `main` using `mkdocs gh-deploy`.

**Tech Stack:** MkDocs, mkdocs-material, uv (dependency management), GitHub Actions

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| Create | `mkdocs.yml` | Site configuration |
| Modify | `pyproject.toml` | Add `docs` dependency group |
| Create | `docs/index.md` | Landing page (from README content) |
| Create | `docs/DEVELOP.md` | Copy of root `DEVELOP.md` |
| Create | `docs/user-guide/installation.md` | Stub |
| Create | `docs/user-guide/quickstart.md` | Stub |
| Create | `docs/user-guide/schema.md` | Stub |
| Create | `docs/user-guide/spatial-queries.md` | Stub |
| Create | `docs/examples/local.md` | Stub |
| Create | `docs/examples/s3-icechunk.md` | Stub |
| Create | `docs/api/zarr-table.md` | Hand-written API reference |
| Create | `docs/architecture/spatial-indexing.md` | Stub |
| Create | `docs/architecture/query-pipeline.md` | Stub |
| Create | `.github/workflows/docs.yml` | CI deploy workflow |

---

### Task 1: Add docs dependencies to pyproject.toml

**Files:**
- Modify: `pyproject.toml`

The root `pyproject.toml` already has a `[dependency-groups]` section with `dev`. Add a `docs` group.

- [ ] **Step 1: Add the docs dependency group**

Open `pyproject.toml` and add the following after the existing `[dependency-groups]` `dev` entry:

```toml
[dependency-groups]
dev = ["ipykernel>=6.30.1"]
docs = [
  "mkdocs-material>=9.5",
]
```

- [ ] **Step 2: Verify uv can resolve the group**

Run from the repo root:
```bash
uv sync --group docs
```

Expected: Dependencies resolve and install without errors. You will see `mkdocs-material` and its transitive deps installed.

---

### Task 2: Create mkdocs.yml

**Files:**
- Create: `mkdocs.yml`

- [ ] **Step 1: Create the file**

Create `mkdocs.yml` at the repo root with the following content:

```yaml
site_name: zarr-datafusion-search
repo_name: developmentseed/zarr-datafusion-search
repo_url: https://github.com/developmentseed/zarr-datafusion-search
site_description: Query Zarr metadata with DataFusion SQL
site_author: Development Seed
site_url: https://developmentseed.github.io/zarr-datafusion-search/
docs_dir: docs

extra:
  social:
    - icon: "fontawesome/brands/github"
      link: "https://github.com/developmentseed"
    - icon: "material/web"
      link: "https://developmentseed.org/"

nav:
  - index.md
  - User Guide:
      - user-guide/installation.md
      - user-guide/quickstart.md
      - user-guide/schema.md
      - user-guide/spatial-queries.md
  - Examples:
      - examples/local.md
      - examples/s3-icechunk.md
  - API Reference:
      - api/zarr-table.md
  - Architecture:
      - architecture/spatial-indexing.md
      - architecture/query-pipeline.md
  - Developer Docs: DEVELOP.md

theme:
  name: material
  icon:
    logo: material/database-search
  palette:
    - media: "(prefers-color-scheme)"
      toggle:
        icon: material/brightness-auto
        name: Switch to light mode
    - media: "(prefers-color-scheme: light)"
      scheme: default
      primary: teal
      accent: teal
      toggle:
        icon: material/brightness-7
        name: Switch to dark mode
    - media: "(prefers-color-scheme: dark)"
      scheme: slate
      primary: teal
      accent: teal
      toggle:
        icon: material/brightness-4
        name: Switch to system preference

  features:
    - content.code.copy
    - navigation.indexes
    - navigation.instant
    - navigation.tracking
    - search.suggest
    - search.share

plugins:
  - search

markdown_extensions:
  - admonition
  - attr_list
  - def_list
  - footnotes
  - pymdownx.details
  - pymdownx.superfences
  - pymdownx.highlight:
      anchor_linenums: true
  - pymdownx.inlinehilite
  - pymdownx.snippets
  - pymdownx.tasklist:
      custom_checkbox: true
  - toc:
      permalink: true
```

- [ ] **Step 2: Verify MkDocs can parse the config (before docs/ exists)**

Run:
```bash
uv run --group docs mkdocs build --strict 2>&1 | head -20
```

Expected: Errors about missing doc files — that's fine, it confirms the YAML parses. If you see a YAML parse error instead, fix the indentation in `mkdocs.yml`.

---

### Task 3: Create docs/index.md

**Files:**
- Create: `docs/index.md`

This is the landing page. Populate it from the README content rather than leaving it as a stub.

- [ ] **Step 1: Create the file**

```markdown
# zarr-datafusion-search

A prototype for querying STAC or CMR style _metadata_ about Zarr arrays and groups
using [DataFusion](https://datafusion.apache.org/), an extensible query engine written
in Rust.

This concept was conceived by the team at [Earthmover](https://www.earthmover.io/)
and is outlined in their whitepaper *Level 2 Data Collections in Zarr / Icechunk*.

## Quick Start

```python
from zarr_datafusion_search import ZarrTable
from datafusion import SessionContext

# Create a new DataFusion session context
ctx = SessionContext()

# Register a specific Zarr store as a table named "zarr_data"
ctx.register_table_provider("zarr_data", ZarrTable("zarr_store.zarr"))

# Run SQL queries against the Zarr data
df = ctx.sql("SELECT * FROM zarr_data;")
df.show()
```

!!! note
    The underlying DataFusion TableProvider ABI is not entirely stable. Use the
    same version of `datafusion-python` as the version of DataFusion used to
    compile this package.

## Navigation

- **[User Guide](user-guide/installation.md)** — Installation, quickstart, and
  how-to guides
- **[Examples](examples/local.md)** — Full worked examples against local and
  S3-backed stores
- **[API Reference](api/zarr-table.md)** — `ZarrTable` class documentation
- **[Architecture](architecture/spatial-indexing.md)** — How the query engine
  and spatial indexing work
- **[Developer Docs](DEVELOP.md)** — Contributing and running benchmarks
```

---

### Task 4: Create docs/DEVELOP.md

**Files:**
- Create: `docs/DEVELOP.md`

- [ ] **Step 1: Copy root DEVELOP.md into docs/**

```markdown
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
```

---

### Task 5: Create stub pages

**Files:**
- Create: `docs/user-guide/installation.md`
- Create: `docs/user-guide/quickstart.md`
- Create: `docs/user-guide/schema.md`
- Create: `docs/user-guide/spatial-queries.md`
- Create: `docs/examples/local.md`
- Create: `docs/examples/s3-icechunk.md`
- Create: `docs/architecture/spatial-indexing.md`
- Create: `docs/architecture/query-pipeline.md`

Each stub has the same structure: a heading and a "coming soon" admonition.

- [ ] **Step 1: Create docs/user-guide/installation.md**

```markdown
# Installation

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 2: Create docs/user-guide/quickstart.md**

```markdown
# Quickstart

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 3: Create docs/user-guide/schema.md**

```markdown
# Schema Conventions

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 4: Create docs/user-guide/spatial-queries.md**

```markdown
# Spatial Queries

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 5: Create docs/examples/local.md**

```markdown
# Local Store Example

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 6: Create docs/examples/s3-icechunk.md**

```markdown
# S3 + Icechunk Example

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 7: Create docs/architecture/spatial-indexing.md**

```markdown
# Spatial Indexing

!!! note "Coming soon"
    This page is under construction.
```

- [ ] **Step 8: Create docs/architecture/query-pipeline.md**

```markdown
# Query Pipeline

!!! note "Coming soon"
    This page is under construction.
```

---

### Task 6: Create docs/api/zarr-table.md

**Files:**
- Create: `docs/api/zarr-table.md`

Hand-written reference for `ZarrTable`. The Python package exposes one class:
`ZarrTable`, imported from `zarr_datafusion_search`.

- [ ] **Step 1: Create the file**

```markdown
# ZarrTable

::: zarr_datafusion_search.ZarrTable

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
```

---

### Task 7: Create GitHub Actions deployment workflow

**Files:**
- Create: `.github/workflows/docs.yml`

- [ ] **Step 1: Create the workflow file**

```yaml
name: Deploy docs

on:
  push:
    branches:
      - main
  workflow_dispatch:

permissions:
  contents: write

jobs:
  deploy:
    name: Deploy MkDocs to GitHub Pages
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true

      - name: Deploy docs
        env:
          GIT_COMMITTER_NAME: CI
          GIT_COMMITTER_EMAIL: ci-bot@example.com
        run: uv run --group docs mkdocs gh-deploy --force
```

---

### Task 8: Verify the full site builds locally

No files created. This is a verification step only.

- [ ] **Step 1: Build the site**

Run from the repo root:
```bash
uv run --group docs mkdocs build --strict
```

Expected output (last few lines):
```
INFO    -  Documentation built in X.XX seconds
```

No warnings or errors about missing files, broken links, or invalid nav entries.

- [ ] **Step 2: Serve locally and check the site**

```bash
uv run --group docs mkdocs serve
```

Open `http://127.0.0.1:8000` in a browser. Verify:

- Dark/light/auto toggle works in the top-right
- All nav sections expand (User Guide, Examples, API Reference, Architecture, Developer Docs)
- Each stub page renders the "Coming soon" admonition
- `index.md` shows the quick start code block with syntax highlighting
- Search bar is present and functional
- GitHub repo link in the top-right points to the correct repo

Stop the server with `Ctrl+C`.
