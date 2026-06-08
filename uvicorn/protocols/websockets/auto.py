from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

AutoWebSocketsProtocol: Callable[..., Awaitable[Any]] | None
try:
    import websockets  # noqa: F401
except ImportError:  # pragma: no cover
    try:
        import wsproto  # noqa: F401
    except ImportError:
        AutoWebSocketsProtocol = None
    else:
        from uvicorn.protocols.websockets.wsproto_impl import handle as AutoWebSocketsProtocol  # noqa: F401, I001
else:
    from uvicorn.protocols.websockets.websockets_sansio_impl import handle as AutoWebSocketsProtocol  # noqa: F401, I001
