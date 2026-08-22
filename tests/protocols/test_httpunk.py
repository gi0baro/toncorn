from __future__ import annotations

import asyncio
import importlib.util
from typing import Any

import httpx
import pytest

from tests.utils import run_server
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import Config
from uvicorn.server import ServerState

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(not importlib.util.find_spec("httpunk"), reason="httpunk not installed."),
]

# Behaviour shared with other HTTP/2 implementations lives in the protocol-agnostic
# `test_http2_server.py` (and `test_http.py` for HTTP/1). Only httpunk-specific corners
# that cannot be expressed against a generic protocol remain here.


async def test_start_only_response(unused_tcp_port: int):
    """An app that starts a response but never sends a body still flushes an empty one."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})

    config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert response.status_code == 204
    assert response.text == ""


async def test_body_handoff_abort_releases_parked_consumer():
    """abort() wakes a consumer parked in __anext__, which then raises _StreamAborted
    (truncating the wire response); later puts from the producer are silent no-ops."""
    from uvicorn.protocols.http.httpunk_impl import _BodyHandoff, _StreamAborted

    handoff = _BodyHandoff(asyncio.get_event_loop())
    consumer = asyncio.ensure_future(handoff.__anext__())
    await asyncio.sleep(0)  # let the consumer park in its get-waiter
    handoff.abort()
    with pytest.raises(_StreamAborted):
        await consumer
    await handoff.put(b"late", True)  # producer outlives the abort: dropped, no park


async def test_body_handoff_abort_releases_parked_producer():
    """A non-empty chunk behind an unconsumed one parks the producer (backpressure);
    abort() releases it without delivering the chunk. Empty non-final puts are no-ops."""
    from uvicorn.protocols.http.httpunk_impl import _BodyHandoff

    handoff = _BodyHandoff(asyncio.get_event_loop())
    await handoff.put(b"first", True)  # slot free: returns without parking
    await handoff.put(b"", True)  # empty non-final chunk: nothing to hand over
    producer = asyncio.ensure_future(handoff.put(b"second", True))
    await asyncio.sleep(0)  # let the producer park on the occupied slot
    assert not producer.done()
    handoff.abort()
    await producer  # released by the abort, the parked chunk is dropped


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"connection": b"Upgrade", "upgrade": b"websocket"}, True),
        ({"connection": b"keep-alive, Upgrade", "upgrade": b"WebSocket"}, True),
        ({"connection": b"keep-alive"}, False),
        ({"connection": b"Upgrade", "upgrade": b"h2c"}, False),
    ],
)
def test_is_ws_upgrade(headers: dict[str, bytes], expected: bool):
    from uvicorn.protocols.http.httpunk_impl import _is_ws_upgrade

    class _Headers:
        def __init__(self, data: dict[str, bytes]) -> None:
            self._data = data

        def items(self):
            return self._data.items()

    class _Request:
        def __init__(self, data: dict[str, bytes]) -> None:
            self.headers = _Headers(data)

    assert _is_ws_upgrade(_Request(headers)) is expected


async def test_init_loads_config():
    """Constructing a protocol with an unloaded config loads it (config.load())."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(
        scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
    ) -> None: ...  # pragma: no cover - never invoked, just a valid ASGI target

    config = Config(app=app)
    assert not config.loaded
    protocol = HTTPunkH1Protocol(config=config, server_state=ServerState(), app_state={})
    assert config.loaded
    assert protocol.app is not None


async def test_access_log(unused_tcp_port: int):
    """With the access log enabled, each request is logged."""
    import logging

    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    records: list[logging.LogRecord] = []

    class _RecordingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _RecordingHandler()
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.addHandler(handler)
    config: Config = Config(
        app=app,
        loop="asyncio",
        port=unused_tcp_port,
        http=HTTPunkH1Protocol,
        access_log=True,
        log_level="info",
        log_config=None,
    )
    try:
        async with run_server(config):
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    finally:
        access_logger.removeHandler(handler)
    assert response.status_code == 200
    assert any('"GET / HTTP/1.1" 200' in record.getMessage() for record in records)


async def test_keepalive_and_total_requests(unused_tcp_port: int):
    """Sequential requests reuse the connection and are counted in `total_requests`."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    config: Config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config) as server:
        async with httpx.AsyncClient() as client:
            await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
            await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert server.server_state.total_requests == 2


async def test_h1_post_request_body(unused_tcp_port: int):
    """httpunk drives its own serve loop over a real transport, so it can't run through
    `test_http.py`'s synchronous MockTransport harness; exercise its HTTP/1 body path here."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        body = b""
        more_body = True
        while more_body:
            message = await receive()
            assert message["type"] == "http.request"
            body += message.get("body", b"")
            more_body = message.get("more_body", False)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})

    config: Config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"http://127.0.0.1:{unused_tcp_port}/", content=b"request-payload")
    assert response.status_code == 200
    assert response.text == "request-payload"


async def test_h1_streaming_response(unused_tcp_port: int):
    """The HTTP/1 chunked streaming path over a real transport."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"tick;", "more_body": True})
        await asyncio.sleep(0.05)
        await send({"type": "http.response.body", "body": b"tock", "more_body": False})

    config: Config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert response.status_code == 200
    assert response.text == "tick;tock"
    assert response.headers.get("transfer-encoding") == "chunked"


async def test_h1_app_exception_returns_500(unused_tcp_port: int):
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        raise RuntimeError("boom")

    config: Config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert response.headers.get("server") == "uvicorn"


async def test_auto_protocol_serves_h1_and_h2(unused_tcp_port: int):
    """`--http httpunk` sniffs the protocol per connection: an HTTP/1 request and an
    h2c prior-knowledge HTTP/2 request are both served on the same port."""
    from httpunk.asyncio import H2ClientProtocol

    from uvicorn.protocols.http.httpunk_impl import HTTPunkAutoProtocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        version: Any = scope.get("http_version")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": f"http/{version}".encode()})

    config: Config = Config(
        app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkAutoProtocol, log_level="warning"
    )
    async with run_server(config):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
        assert response.text == "http/1.1"

        loop = asyncio.get_event_loop()
        _transport, proto = await loop.create_connection(
            lambda: H2ClientProtocol(authority=f"127.0.0.1:{unused_tcp_port}", scheme="http"),
            "127.0.0.1",
            unused_tcp_port,
        )
        try:
            conn = await proto.ready()
            h2_response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await h2_response.read()
        finally:
            await proto.aclose()
    assert body == b"http/2"
