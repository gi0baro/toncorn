from __future__ import annotations

import asyncio
import contextlib
import contextvars
import importlib.util
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest

from tests.utils import run_server
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import Config

if TYPE_CHECKING:
    from uvicorn._types import HTTPScope

pytestmark = [
    pytest.mark.anyio,
    # The client half of every test is httpunk's own HTTP/2 client: httpx only speaks
    # HTTP/2 through the `h2` package, which is not a dependency, and these are
    # plaintext (h2c prior-knowledge) connections.
    pytest.mark.skipif(not importlib.util.find_spec("httpunk"), reason="httpunk not installed."),
]


async def _run(app: object, port: int, http_protocol_cls: type[asyncio.Protocol], **config_kwargs: object):
    config = Config(app=app, loop="asyncio", port=port, http=http_protocol_cls, log_level="warning", **config_kwargs)  # type: ignore[arg-type]
    return run_server(config)


async def _ok_app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:  # pragma: no cover
    # Only used where uvicorn's concurrency limit replaces the app with its own 503 response.
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


@asynccontextmanager
async def _h2_connection(port: int) -> AsyncIterator[Any]:
    from httpunk.asyncio import H2ClientProtocol

    loop = asyncio.get_event_loop()
    _transport, proto = await loop.create_connection(
        lambda: H2ClientProtocol(authority=f"127.0.0.1:{port}", scheme="http"), "127.0.0.1", port
    )
    try:
        yield await proto.ready()
    finally:
        await proto.aclose()


async def test_get_request(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        assert scope["type"] == "http"
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"Hello, world"})

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 200
    assert body == b"Hello, world"
    assert dict(response.headers.items()).get("server") == b"uvicorn"


async def test_request_scope(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        http_scope = cast("HTTPScope", scope)
        body = "|".join(
            [
                http_scope["http_version"],
                http_scope["method"],
                http_scope["root_path"],
                http_scope["path"],
                http_scope["query_string"].decode(),
            ]
        ).encode()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})

    async with await _run(app, unused_tcp_port, http2_protocol_cls, root_path="/api"):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/items?a=1&b=2", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 200
    assert body == b"2|GET|/api|/api/items|a=1&b=2"


async def test_post_request_body(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
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

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request(
                "POST", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"}, body=b"request-payload"
            )
            body = await response.read()
    assert response.status == 200
    assert body == b"request-payload"


async def test_streaming_response(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"chunk-1", "more_body": True})
        await asyncio.sleep(0.01)
        await send({"type": "http.response.body", "body": b"chunk-2", "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 200
    assert body == b"chunk-1chunk-2"


async def test_destreamed_response(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    """A multi-part body completed without suspending may collapse into a single response."""

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"part-1;", "more_body": True})
        await send({"type": "http.response.body", "body": b"part-2", "more_body": False})

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 200
    assert body == b"part-1;part-2"


async def test_streaming_response_backpressure(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    """Several chunks emitted within one loop tick, then more after suspending."""

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"a" * 1024, "more_body": True})
        await send({"type": "http.response.body", "body": b"b" * 1024, "more_body": True})
        await send({"type": "http.response.body", "body": b"c" * 1024, "more_body": True})
        await asyncio.sleep(0.01)
        await send({"type": "http.response.body", "body": b"d" * 1024, "more_body": False})

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 200
    assert body == b"a" * 1024 + b"b" * 1024 + b"c" * 1024 + b"d" * 1024


async def test_client_disconnect_mid_stream(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    """A client vanishing mid-streaming-response must not take the server down."""
    sending = asyncio.Event()
    gone = asyncio.Event()

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"x" * 1024, "more_body": True})
        sending.set()
        await gone.wait()
        await send({"type": "http.response.body", "body": b"y" * 1024, "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        loop = asyncio.get_event_loop()
        from httpunk.asyncio import H2ClientProtocol

        transport, proto = await loop.create_connection(
            lambda: H2ClientProtocol(authority=f"127.0.0.1:{unused_tcp_port}", scheme="http"),
            "127.0.0.1",
            unused_tcp_port,
        )
        conn = await proto.ready()
        request = conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
        task = asyncio.ensure_future(request)
        await sending.wait()
        transport.abort()
        gone.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, OSError):
            await task
        await asyncio.sleep(0.05)

        async with _h2_connection(unused_tcp_port) as conn2:
            response = await conn2.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            sending.clear()
            gone.set()
            body = await response.read()
    assert response.status == 200
    assert body == b"x" * 1024 + b"y" * 1024


async def test_concurrent_streams(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        http_scope = cast("HTTPScope", scope)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": http_scope["path"].encode()})

    async with await _run(app, unused_tcp_port, http2_protocol_cls) as server:
        async with _h2_connection(unused_tcp_port) as conn:

            async def one(i: int) -> bytes:
                response = await conn.request("GET", f"/{i}", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
                return await response.read()

            results = await asyncio.gather(*(one(i) for i in range(6)))
    assert results == [f"/{i}".encode() for i in range(6)]
    assert server.server_state.total_requests == 6


async def test_app_exception_returns_500(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        raise RuntimeError("boom")

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 500
    assert body == b"Internal Server Error"
    assert dict(response.headers.items()).get("server") == b"uvicorn"


async def test_no_response_returns_500(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        return

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            await response.read()
    assert response.status == 500


async def test_connection_specific_response_headers_are_stripped(
    http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int
):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"Connection", b"close"),
                    (b"Keep-Alive", b"timeout=5"),
                    (b"X-Custom", b"kept"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            await response.read()
    headers = dict(response.headers.items())
    assert response.status == 200
    assert "connection" not in headers
    assert "keep-alive" not in headers
    assert headers.get("x-custom") == b"kept"


async def test_limit_concurrency_returns_503(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async with await _run(_ok_app, unused_tcp_port, http2_protocol_cls, limit_concurrency=1):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 503
    assert body == b"Service Unavailable"


async def test_reset_contextvars(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    var: contextvars.ContextVar[str] = contextvars.ContextVar("test_http2_ctx", default="default")
    var.set("outer")
    seen: dict[str, str] = {}

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        seen["value"] = var.get()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    async with await _run(app, unused_tcp_port, http2_protocol_cls, reset_contextvars=True):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            await response.read()
    assert response.status == 200
    assert seen["value"] == "default"
