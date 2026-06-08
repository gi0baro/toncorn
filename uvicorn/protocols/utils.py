from __future__ import annotations

import urllib.parse

from tonio.colored.net import SocketStream
from tonio.colored.net.tls import TLSStream

from uvicorn._types import WWWScope

type Stream = SocketStream | TLSStream


class ClientDisconnected(OSError): ...


def _underlying(stream: Stream) -> SocketStream:
    """Return the SocketStream underneath an optional TLS wrapper."""
    if isinstance(stream, TLSStream):
        return stream.transport  # type: ignore[return-value]
    return stream


def get_remote_addr(stream: Stream) -> tuple[str, int] | None:
    sock = _underlying(stream).socket
    try:
        info = sock.getpeername()
    except OSError:  # pragma: no cover
        return None
    if isinstance(info, tuple) and len(info) >= 2:
        return (str(info[0]), int(info[1]))
    return None


def get_local_addr(stream: Stream) -> tuple[str, int | None] | None:
    sock = _underlying(stream).socket
    try:
        info = sock.getsockname()
    except OSError:  # pragma: no cover
        return None
    if isinstance(info, tuple) and len(info) >= 2:
        return (str(info[0]), int(info[1]))
    if isinstance(info, str):
        return (info, None)
    return None


def is_ssl(stream: Stream) -> bool:
    return isinstance(stream, TLSStream)


def get_client_addr(scope: WWWScope) -> str:
    client = scope.get("client")
    if not client:
        return ""
    return "%s:%d" % client


def get_path_with_query_string(scope: WWWScope) -> str:
    path_with_query_string = urllib.parse.quote(scope["path"])
    if scope["query_string"]:
        path_with_query_string = "{}?{}".format(path_with_query_string, scope["query_string"].decode("ascii"))
    return path_with_query_string
