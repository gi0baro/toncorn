from __future__ import annotations

try:
    import httptools  # noqa: F401
except ImportError:  # pragma: no cover
    from uvicorn.protocols.http.h11_impl import handle as AutoHTTPProtocol
else:
    from uvicorn.protocols.http.httptools_impl import handle as AutoHTTPProtocol

__all__ = ["AutoHTTPProtocol"]
