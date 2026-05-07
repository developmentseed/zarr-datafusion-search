from __future__ import annotations

from ._rust import ZarrTable, ___version, ingest_stac_search

__version__: str = ___version()

__all__ = ["ZarrTable", "ingest_stac_search", "__version__"]
