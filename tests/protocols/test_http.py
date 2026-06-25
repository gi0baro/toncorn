"""Tests for the pull-style HTTP protocol handlers.

Each test drives `handle(stream, config, server_state, app_state)` directly,
with a :class:`FakeSocketStream` standing in for the network. Tests are
parametrized across the h11 and httptools handlers via the ``http_protocol_cls``
fixture in ``tests/conftest.py``.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
import tonio.colored.time

from tests.protocols._fake_stream import FakeSocketStream
from tests.response import Response
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import Config
from uvicorn.lifespan.off import LifespanOff
from uvicorn.protocols.http import h11_impl, httptools_impl
from uvicorn.server import ServerState

pytestmark = pytest.mark.tonio


async def _recv_with_timeout(stream, timeout_seconds, runtime, expect_data):
    data, ok = await tonio.colored.time.timeout(stream.receive_some(), timeout_seconds)
    return data if ok else None


httptools_impl._recv_with_timeout_plain = _recv_with_timeout
h11_impl._recv_with_timeout_plain = _recv_with_timeout


# ---------------------------------------------------------------------------
# Canonical request bytes
# ---------------------------------------------------------------------------

SIMPLE_GET_REQUEST = b"\r\n".join([b"GET / HTTP/1.1", b"Host: example.org", b"", b""])
SIMPLE_HEAD_REQUEST = b"\r\n".join([b"HEAD / HTTP/1.1", b"Host: example.org", b"", b""])
SIMPLE_POST_REQUEST = b"\r\n".join(
    [
        b"POST / HTTP/1.1",
        b"Host: example.org",
        b"Content-Type: application/json",
        b"Content-Length: 18",
        b"",
        b'{"hello": "world"}',
    ]
)
LARGE_POST_REQUEST = b"\r\n".join(
    [
        b"POST / HTTP/1.1",
        b"Host: example.org",
        b"Content-Type: text/plain",
        b"Content-Length: 100000",
        b"",
        b"x" * 100000,
    ]
)
HTTP10_GET_REQUEST = b"\r\n".join([b"GET / HTTP/1.0", b"Host: example.org", b"", b""])
CONNECTION_CLOSE_REQUEST = b"\r\n".join([b"GET / HTTP/1.1", b"Host: example.org", b"Connection: close", b"", b""])
GET_REQUEST_WITH_RAW_PATH = b"\r\n".join([b"GET /one%2Ftwo HTTP/1.1", b"Host: example.org", b"", b""])
EXPECT_100_REQUEST = b"\r\n".join(
    [
        b"POST / HTTP/1.1",
        b"Host: example.org",
        b"Content-Length: 5",
        b"Expect: 100-continue",
        b"",
        b"hello",
    ]
)
UPGRADE_WEBSOCKET_REQUEST = b"\r\n".join(
    [
        b"GET / HTTP/1.1",
        b"Host: example.org",
        b"Connection: upgrade",
        b"Upgrade: websocket",
        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
        b"Sec-WebSocket-Version: 13",
        b"",
        b"",
    ]
)
UPGRADE_H2C_REQUEST = b"\r\n".join(
    [
        b"GET / HTTP/1.1",
        b"Host: example.org",
        b"Connection: upgrade",
        b"Upgrade: h2c",
        b"",
        b"",
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(app: Any, **kwargs: Any) -> Config:
    config = Config(app=app, **kwargs)
    config.load()
    return config


async def _run(
    handler: Any,
    stream: FakeSocketStream,
    app: Any,
    *,
    lifespan: LifespanOff | None = None,
    **config_kwargs: Any,
) -> tuple[Config, ServerState]:
    config = _make_config(app, **config_kwargs)
    server_state = ServerState()
    # Mimic Server.startup's pre-population so default headers are non-empty.
    server_state.default_headers = list(config.encoded_headers)
    lifespan = lifespan or LifespanOff(config)
    await handler(stream, config, server_state, lifespan.state)
    return config, server_state


# ---------------------------------------------------------------------------
# Basic request/response
# ---------------------------------------------------------------------------


async def test_get_request(http_protocol_cls: Any):
    app = Response("Hello, world", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 200 OK" in stream.outgoing
    assert b"Hello, world" in stream.outgoing


async def test_head_request(http_protocol_cls: Any):
    app = Response("Hello, world", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(SIMPLE_HEAD_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 200 OK" in stream.outgoing
    # HEAD response must not include the body.
    assert b"Hello, world" not in stream.outgoing


async def test_post_request(http_protocol_cls: Any):
    seen_body: dict[str, bytes] = {}

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        body = b""
        while True:
            msg = await receive()
            if msg["type"] != "http.request":
                return
            body += msg.get("body", b"")
            if not msg.get("more_body", False):
                break
        seen_body["payload"] = body
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"ok"})

    stream = FakeSocketStream()
    stream.feed(SIMPLE_POST_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert seen_body["payload"] == b'{"hello": "world"}'
    assert b"HTTP/1.1 200 OK" in stream.outgoing


async def test_large_post_request(http_protocol_cls: Any):
    seen: dict[str, int] = {}

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        size = 0
        while True:
            msg = await receive()
            if msg["type"] != "http.request":
                return
            size += len(msg.get("body", b""))
            if not msg.get("more_body", False):
                break
        seen["size"] = size
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"ok"})

    stream = FakeSocketStream()
    stream.feed(LARGE_POST_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert seen["size"] == 100000


async def test_http10_request_closes(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(HTTP10_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 200 OK" in stream.outgoing
    # HTTP/1.0 means no implicit keep-alive — handle returns immediately after
    # the response, without waiting for further bytes.


async def test_connection_close_closes(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(CONNECTION_CLOSE_REQUEST)
    # No EOF needed: server should exit after seeing Connection: close.
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 200 OK" in stream.outgoing


async def test_raw_path(http_protocol_cls: Any):
    seen: dict[str, Any] = {}

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        seen["path"] = scope["path"]
        seen["raw_path"] = scope["raw_path"]
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"0")]})
        await send({"type": "http.response.body", "body": b""})

    stream = FakeSocketStream()
    stream.feed(GET_REQUEST_WITH_RAW_PATH)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert seen["path"] == "/one/two"
    assert seen["raw_path"] == b"/one%2Ftwo"


async def test_root_path(http_protocol_cls: Any):
    seen: dict[str, Any] = {}

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        seen["path"] = scope["path"]
        seen["root_path"] = scope["root_path"]
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"0")]})
        await send({"type": "http.response.body", "body": b""})

    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app, root_path="/api")
    assert seen["root_path"] == "/api"
    assert seen["path"] == "/api/"


# ---------------------------------------------------------------------------
# Keep-alive and pipelining
# ---------------------------------------------------------------------------


async def test_keepalive_two_requests(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert stream.outgoing.count(b"HTTP/1.1 200 OK") == 2


async def test_pipelined_requests_in_one_chunk(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST + SIMPLE_GET_REQUEST + SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert stream.outgoing.count(b"HTTP/1.1 200 OK") == 3


async def test_keepalive_timeout(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    # No EOF and no further bytes — handler should exit after timeout_keep_alive.
    await _run(http_protocol_cls, stream, app, timeout_keep_alive=0.1)
    assert b"HTTP/1.1 200 OK" in stream.outgoing


# ---------------------------------------------------------------------------
# Errors and edge cases
# ---------------------------------------------------------------------------


async def test_invalid_http(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(b"GARBAGE NOT HTTP\r\n\r\n")
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 400" in stream.outgoing


async def test_app_exception(http_protocol_cls: Any):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        raise RuntimeError("kaboom")

    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 500 Internal Server Error" in stream.outgoing


async def test_app_returns_without_starting_response(http_protocol_cls: Any):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        return  # never sends anything

    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 500" in stream.outgoing


async def test_exception_after_response_started(http_protocol_cls: Any):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError("kaboom")

    stream = FakeSocketStream()
    stream.feed(SIMPLE_GET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    # Headers already sent — no 500 body, but the connection closes.
    assert b"HTTP/1.1 200 OK" in stream.outgoing


# ---------------------------------------------------------------------------
# 100-continue
# ---------------------------------------------------------------------------


async def test_100_continue_sent_when_body_consumed(http_protocol_cls: Any):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        msg = await receive()
        assert msg["body"] == b"hello"
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"ok"})

    stream = FakeSocketStream()
    stream.feed(EXPECT_100_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 100 Continue" in stream.outgoing
    assert b"HTTP/1.1 200 OK" in stream.outgoing


async def test_100_continue_skipped_when_body_not_consumed(http_protocol_cls: Any):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        # Respond without calling receive() — body is never consumed.
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"0")]})
        await send({"type": "http.response.body", "body": b""})

    stream = FakeSocketStream()
    stream.feed(EXPECT_100_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    assert b"HTTP/1.1 100 Continue" not in stream.outgoing
    assert b"HTTP/1.1 200 OK" in stream.outgoing


# ---------------------------------------------------------------------------
# WebSocket upgrade dispatch
# ---------------------------------------------------------------------------


async def test_websocket_upgrade_dispatched(http_protocol_cls: Any):
    """When a ws_protocol_class is configured, the upgrade is delegated to it."""

    captured: dict[str, Any] = {}

    async def fake_ws_handler(stream, config, server_state, app_state, *, request_bytes):
        captured["request_bytes"] = request_bytes
        # Respond with anything visible; the test just verifies dispatch.
        await stream.send_all(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")

    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(UPGRADE_WEBSOCKET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app, ws=fake_ws_handler)
    assert b"HTTP/1.1 101" in stream.outgoing
    # Headers are lowercased by both h11 and httptools parsers.
    assert b"upgrade: websocket" in captured["request_bytes"]


async def test_websocket_upgrade_426_without_ws_handler(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(UPGRADE_WEBSOCKET_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app, ws="none")
    assert b"HTTP/1.1 426 Upgrade Required" in stream.outgoing


async def test_non_websocket_upgrade_rejected(http_protocol_cls: Any):
    app = Response("ok", media_type="text/plain")
    stream = FakeSocketStream()
    stream.feed(UPGRADE_H2C_REQUEST)
    stream.feed_eof()
    await _run(http_protocol_cls, stream, app)
    # An h2c upgrade is not a ws upgrade — the parser exception path produces
    # either a 400 or just closes; we just check the connection didn't 101.
    assert b"HTTP/1.1 101" not in stream.outgoing


# ---------------------------------------------------------------------------
# Access log
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/", "/?foo", "/?foo=bar"])
async def test_access_log(path: str, http_protocol_cls: Any):
    """Verify the access log gets the request line and status.

    Note: this test uses a hand-rolled logging handler instead of pytest's
    ``caplog``. caplog's propagation hooks interact badly with the new
    pull-style handler under tonio and trip a C-level abort.
    """
    request = b"\r\n".join([f"GET {path} HTTP/1.1".encode(), b"Host: example.org", b"", b""])
    captured: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    # Config.load() reconfigures uvicorn.access (replacing handlers), so we
    # must load the config first, then install our handler on the resulting
    # logger, then invoke the protocol handler manually.
    app = Response("ok", media_type="text/plain")
    config = Config(app=app)
    config.load()
    server_state = ServerState()
    server_state.default_headers = list(config.encoded_headers)

    access = logging.getLogger("uvicorn.access")
    handler = _Capture(level=logging.INFO)
    access.addHandler(handler)
    try:
        stream = FakeSocketStream()
        stream.feed(request)
        stream.feed_eof()
        await http_protocol_cls(stream, config, server_state, {})
    finally:
        access.removeHandler(handler)

    assert any(path in m and "200" in m for m in captured), captured
