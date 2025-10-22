from __future__ import annotations

from ._rust import *
from ._rust import ___version

__version__: str = ___version()
