from __future__ import annotations

from ._rust import ZarrTable, ___version

__version__: str = ___version()

__all__ = ["ZarrTable", "__version__"]
