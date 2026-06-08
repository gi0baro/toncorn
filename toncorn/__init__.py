"""toncorn — a tonio-powered fork of uvicorn.

The fork keeps its source tree under the original `uvicorn` package name to
minimize the diff against upstream and make future rebases cheap. This module
is the public entry point — `import toncorn` re-exports the same names so
downstream code can pick the branded import.

The version follows the upstream uvicorn version with an extra patch segment
for toncorn-specific releases (e.g. 0.48.0.0, 0.48.0.1). See `_version.py` —
the upstream uvicorn version is the source of truth and toncorn's version is
derived from it.
"""

from toncorn._version import __version__, uvicorn_version
from uvicorn import Config, Server, main, run

__all__ = ["Config", "Server", "__version__", "main", "run", "uvicorn_version"]
