from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable

def ___version() -> str: ...

def ingest_stac_search(
    url: str,
    *,
    store: Any | None = None,
    session: Any | None = None,
    intersects: str | dict | None = None,
    ids: str | list[str] | None = None,
    collections: str | list[str] | None = None,
    max_items: int | None = None,
    limit: int | None = None,
    bbox: list[float] | None = None,
    datetime: str | None = None,
    include: str | list[str] | None = None,
    exclude: str | list[str] | None = None,
    sortby: str | list[str] | None = None,
    filter: str | dict | None = None,
    query: dict | None = None,
    chunk_size: int = 1000,
    asset_hrefs: list[str] | None = None,
) -> Awaitable[int]:
    """Ingest STAC API search results into a Zarr store.

    Queries a STAC API, converts matching items to Arrow, and writes them as
    1-D Zarr arrays under the ``/meta`` group. Supports both
    zarr group-backed stores and Icechunk sessions.

    Args:
        url: Base URL of the STAC API
            (e.g. ``"https://earth-search.aws.element84.com/v1"``).
        store: A ``zarr.Group`` (root group) to write into. The group's
            underlying store (``zarr.storage.ObjectStore`` or
            ``zarr.storage.LocalStore``) is extracted automatically.
            Mutually exclusive with ``session``.
        session: An Icechunk writable session to write into. Mutually exclusive
            with ``store``.
        intersects: GeoJSON geometry (as a string or dict) to filter items by
            spatial intersection.
        ids: One or more STAC item IDs to fetch.
        collections: One or more collection IDs to search within.
        max_items: Maximum number of items to ingest. When ``None``, all
            matching items are fetched.
        limit: Page size for the STAC API search request.
        bbox: Bounding box filter as ``[west, south, east, north]``.
        datetime: Datetime filter as a single datetime or a ``/``-separated
            range (e.g. ``"2024-01-01/2024-06-01"``).
        include: Fields to include in the response (STAC API Fields extension).
        exclude: Fields to exclude from the response (STAC API Fields extension).
        sortby: Sort order (STAC API Sort extension), e.g. ``"+datetime"`` or
            ``"-eo:cloud_cover"``.
        filter: CQL2 filter as a text string or a CQL2-JSON dict (STAC API
            Filter extension).
        query: Legacy STAC API query parameters.
        chunk_size: Number of rows per Zarr chunk for newly created arrays.
            Ignored when appending to an existing store.
        asset_hrefs: Asset keys (e.g. ``["B01", "thumbnail"]``) whose ``href``
            values should be extracted and written as ``/meta/asset_{key}``
            string arrays.

    Returns:
        An awaitable that resolves to the number of rows written.

    Example:
        ```python
        import zarr
        from zarr_datafusion_search import ingest_stac_search

        root = zarr.open_group("./my_store.zarr", mode="w")
        rows = await ingest_stac_search(
            "https://earth-search.aws.element84.com/v1",
            store=root,
            collections="sentinel-2-l2a",
            bbox=[-105, 40, -104, 41],
            max_items=100,
        )
        ```
    """
    ...

class ZarrTable:
    """A DataFusion table provider that exposes a Zarr metadata store as a SQL-queryable table.

    `ZarrTable` implements the DataFusion `TableProvider` protocol via the FFI boundary,
    so it can be registered directly with a `SessionContext` using
    `register_table_provider`.

    Use one of the async class methods to construct an instance:

    - [`from_icechunk`][zarr_datafusion_search.ZarrTable.from_icechunk] — from an Icechunk session
    - [`from_obstore`][zarr_datafusion_search.ZarrTable.from_obstore] — from an obstore object store

    Example:
        ```python
        import asyncio
        from datafusion import SessionContext
        from zarr_datafusion_search import ZarrTable

        async def main():
            table = await ZarrTable.from_obstore(store, "/meta")
            ctx = SessionContext()
            ctx.register_table_provider("items", table)
            df = ctx.sql("SELECT date, collection FROM items LIMIT 10")
            df.show()

        asyncio.run(main())
        ```
    """

    @classmethod
    def from_icechunk(
        cls,
        session: Any,
        group_path: str,
    ) -> Awaitable[ZarrTable]:
        """Create a ZarrTable from an Icechunk session.

        Args:
            session: An open `icechunk.Session` pointing to the store.
            group_path: Absolute path to the Zarr group containing the metadata
                arrays (e.g. `"/meta"`).

        Returns:
            An awaitable that resolves to a `ZarrTable` instance.

        Example:
            ```python
            table = await ZarrTable.from_icechunk(session, "/meta")
            ```
        """
        ...

    @classmethod
    def from_obstore(
        cls,
        store: Any,
        group_path: str,
    ) -> Awaitable[ZarrTable]:
        """Create a ZarrTable from an obstore object store.

        Args:
            store: Any obstore-compatible object store (e.g. `obstore.store.S3Store`,
                `obstore.store.LocalStore`).
            group_path: Absolute path to the Zarr group containing the metadata
                arrays (e.g. `"/meta"`).

        Returns:
            An awaitable that resolves to a `ZarrTable` instance.

        Example:
            ```python
            import obstore.store
            store = obstore.store.LocalStore("/path/to/data.zarr")
            table = await ZarrTable.from_obstore(store, "/meta")
            ```
        """
        ...

    def __datafusion_table_provider__(self, session: Any) -> Any:
        """Return the FFI `TableProvider` capsule for DataFusion registration.

        This is called automatically by `SessionContext.register_table_provider`.
        You do not need to call it directly.
        """
        ...
