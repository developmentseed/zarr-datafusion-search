from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable

def ___version() -> str: ...

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
