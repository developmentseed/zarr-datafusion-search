from __future__ import annotations

from ._rust import ZarrTable
from ._rust import ___version

__version__: str = ___version()

__all__ = ["ZarrTable", "__version__"]
