---
name: MkDocs Documentation Site
description: Design spec for the zarr-datafusion-search MkDocs Material docs site
type: project
---

# MkDocs Documentation Site Design

## Overview

Add a MkDocs Material documentation site to `zarr-datafusion-search`, hosted at
`https://developmentseed.github.io/zarr-datafusion-search`. The site covers both
the Python API and the architecture of the system, following the same pattern used
by obstore and geoarrow-rs from the same ecosystem.

## File Structure

```
zarr-datafusion-search/
  mkdocs.yml
  docs/
    index.md
    user-guide/
      installation.md
      quickstart.md
      schema.md
      spatial-queries.md
    examples/
      local.md
      s3-icechunk.md
    api/
      zarr-table.md
    architecture/
      spatial-indexing.md
      query-pipeline.md
    DEVELOP.md             # copy of root DEVELOP.md
  .github/workflows/
    docs.yml               # deploy on push to main
```

## Navigation

```yaml
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
```

## MkDocs Configuration

- **Theme**: Material with dark/light/auto palette toggle
- **Plugins**: `search` only (no `mkdocstrings`, no `mike`)
- **Markdown extensions**: admonition, code highlighting, superfences, toc with
  permalink — matching the geoarrow-rs/obstore baseline set
- **Site URL**: `https://developmentseed.github.io/zarr-datafusion-search`
- **Repo**: `developmentseed/zarr-datafusion-search`

## Placeholder Pages

All stub pages will contain:
1. A `# Title` heading
2. A `!!! note "Coming soon"` admonition so the site builds cleanly

The `index.md` will be populated from the existing `README.md` content
(project description + Python usage snippet) rather than left as a stub.

`docs/DEVELOP.md` will be a copy of the root `DEVELOP.md`.

## GitHub Actions Deployment

`.github/workflows/docs.yml`:

- Trigger: push to `main`
- Runner: `ubuntu-latest`
- Steps: checkout → `astral-sh/setup-uv` → `uv run mkdocs gh-deploy --force`
- The `uv` doc dependencies will be declared in a `[dependency-groups]` `docs`
  group in the root `pyproject.toml`

## Dependencies

Added to root `pyproject.toml` under `[dependency-groups] docs`:

```
mkdocs-material
mkdocs-material[imaging]   # for social plugin if added later
```

Minimal for now; `mkdocstrings` added when API surface grows.

## Out of Scope

- `mike` versioning (add when first stable release is cut)
- `mkdocstrings` auto-generated API reference (add when `ZarrTable` API stabilizes)
- Blog section
- Custom domain
