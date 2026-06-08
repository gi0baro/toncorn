"""toncorn version, derived from the upstream uvicorn version.

The upstream version is read directly from `uvicorn/__init__.py` via regex
(NOT `import uvicorn`) so this module is safe to execute at build time before
runtime dependencies are installed. Hatch's `source = "code"` reads
`__version__` from here — see `[tool.hatch.version]` in `pyproject.toml`.
"""

import pathlib
import re

_UVICORN_INIT = pathlib.Path(__file__).parent.parent / "uvicorn" / "__init__.py"
_match = re.search(r'^__version__\s*=\s*"([^"]+)"', _UVICORN_INIT.read_text(), re.MULTILINE)
if _match is None:
    raise RuntimeError(f"Could not parse uvicorn version from {_UVICORN_INIT}")

#: The upstream uvicorn version this fork tracks.
uvicorn_version: str = _match.group(1)

#: Bump this for toncorn-only patch releases against the same uvicorn baseline.
#: Resets to 0 each time the upstream uvicorn version changes.
TONCORN_PATCH: int = 0

#: Public toncorn version, e.g. "0.48.0.0".
__version__: str = f"{uvicorn_version}.{TONCORN_PATCH}"
