from __future__ import annotations

import asyncio
import contextlib
import contextvars
import importlib.util
import logging
import ssl
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, cast

import httpx2
import pytest

from tests.response import Response
from tests.utils import run_server
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import Config
from uvicorn.lifespan.off import LifespanOff
from uvicorn.lifespan.on import LifespanOn
from uvicorn.protocols.http.flow_control import HIGH_WATER_LIMIT
from uvicorn.server import ServerState

if TYPE_CHECKING:
    from uvicorn._types import HTTPScope

try:
    import zttp

    from uvicorn.protocols.http.auto_zttp_impl import AutoZttpProtocol
    from uvicorn.protocols.http.zttp_h2_impl import ZttpH2Protocol
    from uvicorn.protocols.http.zttp_impl import ZttpProtocol

    skip_if_no_zttp_h2 = pytest.mark.skipif(
        not hasattr(zttp, "HTTP2"), reason="zttp with HTTP/2 support is not installed"
    )
except ModuleNotFoundError:  # pragma: no cover
    skip_if_no_zttp_h2 = pytest.mark.skipif(True, reason="zttp is not installed")

skip_if_no_httpunk = pytest.mark.skipif(not importlib.util.find_spec("httpunk"), reason="httpunk not installed.")

pytestmark = pytest.mark.anyio


# --- Implementation-agnostic tests (run against every HTTP/2 protocol) ---------


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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
async def test_no_response_returns_500(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        return

    async with await _run(app, unused_tcp_port, http2_protocol_cls):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            await response.read()
    assert response.status == 500


@skip_if_no_httpunk
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
                    (b"TE", b"gzip"),
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
    assert "te" not in headers


@skip_if_no_httpunk
async def test_limit_concurrency_returns_503(http2_protocol_cls: type[asyncio.Protocol], unused_tcp_port: int):
    async with await _run(_ok_app, unused_tcp_port, http2_protocol_cls, limit_concurrency=1):
        async with _h2_connection(unused_tcp_port) as conn:
            response = await conn.request("GET", "/", headers={"host": f"127.0.0.1:{unused_tcp_port}"})
            body = await response.read()
    assert response.status == 503
    assert body == b"Service Unavailable"


@skip_if_no_httpunk
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


# --- zttp-specific internals ----------------------------------------------------


class MockSSLObject:
    def __init__(self, alpn_protocol: str | None):
        self._alpn_protocol = alpn_protocol

    def selected_alpn_protocol(self) -> str | None:
        return self._alpn_protocol


class MockTransport:
    def __init__(
        self,
        sockname: tuple[str, int] | None = None,
        peername: tuple[str, int] | None = None,
        sslcontext: bool = False,
        alpn_protocol: str | None = None,
    ):
        self.sockname = ("127.0.0.1", 8000) if sockname is None else sockname
        self.peername = ("127.0.0.1", 8001) if peername is None else peername
        self.sslcontext = sslcontext
        self.ssl_object = MockSSLObject(alpn_protocol) if sslcontext else None
        self.closed = False
        self.buffer = b""
        self.read_paused = False
        self.protocol: asyncio.Protocol | None = None

    def get_extra_info(self, key: Any):
        return {
            "sockname": self.sockname,
            "peername": self.peername,
            "sslcontext": self.sslcontext,
            "ssl_object": self.ssl_object,
        }.get(key)

    def write(self, data: bytes):
        assert not self.closed
        self.buffer += data

    def close(self):
        assert not self.closed
        self.closed = True

    def pause_reading(self):
        self.read_paused = True

    def resume_reading(self):
        self.read_paused = False

    def is_closing(self):
        return self.closed

    def clear_buffer(self):
        self.buffer = b""

    def set_protocol(self, protocol: asyncio.Protocol):
        self.protocol = protocol

    def get_protocol(self) -> asyncio.Protocol | None:
        return self.protocol


class MockTimerHandle:
    def __init__(
        self, loop_later_list: list[MockTimerHandle], delay: float, callback: Callable[[], None], args: tuple[Any, ...]
    ):
        self.loop_later_list = loop_later_list
        self.delay = delay
        self.callback = callback
        self.args = args
        self.cancelled = False

    def cancel(self):
        if not self.cancelled:
            self.cancelled = True
            self.loop_later_list.remove(self)


class MockTask:
    def add_done_callback(self, callback: Callable[[], None]):
        pass


class MockLoop:
    def __init__(self):
        self._tasks: list[Any] = []
        self._later: list[MockTimerHandle] = []

    def create_task(self, coroutine: Any, **kwargs: Any) -> Any:
        self._tasks.insert(0, coroutine)
        return MockTask()

    def call_later(self, delay: float, callback: Callable[[], None], *args: Any) -> MockTimerHandle:
        handle = MockTimerHandle(self._later, delay, callback, args)
        self._later.insert(0, handle)
        return handle

    async def run_one(self):
        return await self._tasks.pop()

    def run_later(self, with_delay: float) -> None:
        later: list[MockTimerHandle] = []
        for timer_handle in self._later:
            if with_delay >= timer_handle.delay:
                timer_handle.callback(*timer_handle.args)
            else:
                later.append(timer_handle)
        self._later = later


class MockProtocol(asyncio.Protocol):
    loop: MockLoop
    transport: MockTransport
    conn: Any
    flow: Any
    cycles: dict[int, Any]
    timeout_keep_alive_task: Any

    def shutdown(self) -> None: ...

    def resume_reading_if_idle(self) -> None: ...


def get_connected_protocol(
    app: Callable[..., Any],
    lifespan: LifespanOff | LifespanOn | None = None,
    **kwargs: Any,
) -> MockProtocol:
    loop = MockLoop()
    transport = MockTransport(sslcontext=True)
    config = Config(app=app, http="zttp2", **kwargs)
    lifespan = lifespan or LifespanOff(config)
    server_state = ServerState()
    protocol = ZttpH2Protocol(config=config, server_state=server_state, app_state=lifespan.state, _loop=loop)  # type: ignore[arg-type]
    protocol.connection_made(transport)  # type: ignore[arg-type]
    return protocol  # type: ignore[return-value]


def frame(ftype: int, flags: int, stream_id: int, payload: bytes) -> bytes:
    header = len(payload).to_bytes(3, "big") + bytes([ftype, flags]) + stream_id.to_bytes(4, "big")
    return header + payload


class H2Client:
    """Drives the client half of the wire with zttp's own client connection."""

    def __init__(self) -> None:
        self.conn = zttp.Connection(zttp.CLIENT, protocol=zttp.HTTP2)

    def request(
        self,
        method: bytes = b"GET",
        target: bytes = b"/",
        headers: list[tuple[bytes, bytes]] | None = None,
        end: bool = True,
    ) -> zttp.Stream:
        headers = [(b"host", b"example.org")] if headers is None else headers
        stream = self.conn.send_request(method, target, b"2", headers)
        if end:
            stream.end_message()
        return stream

    def data_to_send(self) -> bytes:
        return self.conn.data_to_send()

    def events(self, data: bytes) -> list[Any]:
        self.conn.receive_data(data)
        events = []
        while (event := self.conn.next_event()) is not zttp.NEED_DATA:
            events.append(event)
        return events

    def parse_responses(self, data: bytes) -> dict[int, tuple[int, list[tuple[bytes, bytes]], bytes, bool]]:
        responses: dict[int, tuple[int, list[tuple[bytes, bytes]], bytes, bool]] = {}
        for event in self.events(data):
            if isinstance(event, zttp.Response):
                headers = event.headers.to_list() if isinstance(event.headers, zttp.HeaderBlock) else event.headers
                responses[event.stream_id] = (event.status_code, headers, b"", False)
            elif isinstance(event, zttp.Data):
                status, headers, body, ended = responses[event.stream_id]
                responses[event.stream_id] = (status, headers, body + event.data, ended)
            elif isinstance(event, zttp.EndOfMessage):
                status, headers, body, _ = responses[event.stream_id]
                responses[event.stream_id] = (status, headers, body, True)
        return responses

    def parse_response(self, data: bytes, stream_id: int = 1) -> tuple[int, list[tuple[bytes, bytes]], bytes, bool]:
        return self.parse_responses(data)[stream_id]


# --- Protocol negotiation ------------------------------------------------------


def get_negotiator(
    app: Callable[..., Any],
    alpn_protocol: str | None = None,
    sslcontext: bool = False,
    **kwargs: Any,
) -> tuple[AutoZttpProtocol, MockTransport, MockLoop]:
    loop = MockLoop()
    transport = MockTransport(sslcontext=sslcontext, alpn_protocol=alpn_protocol)
    config = Config(app=app, http="zttp", **kwargs)
    lifespan = LifespanOff(config)
    server_state = ServerState()
    negotiator = AutoZttpProtocol(config=config, server_state=server_state, app_state=lifespan.state, _loop=loop)  # type: ignore[arg-type]
    negotiator.connection_made(transport)  # type: ignore[arg-type]
    return negotiator, transport, loop


# --- Configuration -------------------------------------------------------------


class CustomH2Protocol(asyncio.Protocol):
    alpn_protocols: ClassVar[list[str]] = ["h2", "http/1.1"]


@skip_if_no_zttp_h2
async def test_head_request_has_no_body():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"Hello, world", "more_body": False})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"HEAD", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    status, headers, body, ended = client.parse_response(protocol.transport.buffer)
    assert status == 200
    assert (b"content-type", b"text/plain") in headers
    assert body == b""
    assert ended


@skip_if_no_zttp_h2
async def test_204_response_has_no_body():
    app = Response(b"", status_code=204)
    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    status, _, body, ended = client.parse_response(protocol.transport.buffer)
    assert status == 204
    assert body == b""
    assert ended


@skip_if_no_zttp_h2
async def test_partial_response_resets_stream():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": []})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert not protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.RstStream) for event in events)


@skip_if_no_zttp_h2
async def test_partial_response_after_transport_close_is_dropped():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        protocol.transport.close()

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_response_shorter_than_content_length_resets_stream():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"10")]})
        await send({"type": "http.response.body", "body": b"short", "more_body": False})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert not protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.RstStream) for event in events)


@skip_if_no_zttp_h2
async def test_response_longer_than_content_length_resets_stream():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"too long", "more_body": False})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert not protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.RstStream) for event in events)


@skip_if_no_zttp_h2
async def test_rst_stream_disconnects_the_app():
    received_disconnect = False

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        nonlocal received_disconnect
        message = await receive()
        received_disconnect = message["type"] == "http.disconnect"

    protocol = get_connected_protocol(app)
    client = H2Client()

    stream = client.request(b"POST", b"/", end=False)
    protocol.data_received(client.data_to_send())
    # RST_STREAM with CANCEL (0x8) aborts the stream before the body arrived.
    protocol.data_received(frame(0x03, 0, stream.stream_id, (0x8).to_bytes(4, "big")))
    await protocol.loop.run_one()

    assert received_disconnect
    assert not protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_connection_lost_disconnects_the_app():
    received_disconnect = False

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        nonlocal received_disconnect
        message = await receive()
        received_disconnect = message["type"] == "http.disconnect"

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"POST", b"/", end=False)
    protocol.data_received(client.data_to_send())
    protocol.connection_lost(None)
    await protocol.loop.run_one()

    assert received_disconnect


@skip_if_no_zttp_h2
async def test_keep_alive_timeout_closes_idle_connection():
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app, timeout_keep_alive=5)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()
    assert not protocol.transport.is_closing()

    protocol.loop.run_later(with_delay=1)
    assert not protocol.transport.is_closing()
    protocol.loop.run_later(with_delay=5)
    assert protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_idle_frames_rearm_keep_alive_timer():
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app, timeout_keep_alive=5)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()
    armed = protocol.timeout_keep_alive_task
    assert armed is not None

    protocol.data_received(frame(0x06, 0, 0, b"\x00" * 8))  # PING
    rearmed = protocol.timeout_keep_alive_task
    assert rearmed is not None
    assert rearmed is not armed


@skip_if_no_zttp_h2
async def test_shutdown_when_idle_closes_connection():
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    protocol.shutdown()
    assert protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.GoAway) for event in events)


@skip_if_no_zttp_h2
async def test_shutdown_twice_is_a_no_op():
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app)

    protocol.shutdown()
    assert protocol.transport.is_closing()
    protocol.shutdown()
    assert protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_shutdown_refuses_new_streams_and_closes_after_last_response():
    waiting = asyncio.Event()
    release = asyncio.Event()

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        waiting.set()
        await release.wait()
        response = Response("done", media_type="text/plain")
        await response(scope, receive, send)

    protocol = get_connected_protocol(app)
    client = H2Client()

    first = client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    task = asyncio.get_running_loop().create_task(protocol.loop.run_one())
    await waiting.wait()

    protocol.shutdown()
    assert not protocol.transport.is_closing()

    second = client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()
    status, _, body, _ = client.parse_response(protocol.transport.buffer, stream_id=second.stream_id)
    assert status == 503
    assert body == b"Service Unavailable"

    protocol.transport.clear_buffer()
    release.set()
    await task
    status, _, body, _ = client.parse_response(protocol.transport.buffer, stream_id=first.stream_id)
    assert status == 200
    assert body == b"done"
    assert protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_goaway_closes_idle_connection():
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    protocol.data_received(frame(0x07, 0, 0, (0).to_bytes(4, "big") + (0).to_bytes(4, "big")))
    assert protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_invalid_frames_close_the_connection(caplog: pytest.LogCaptureFixture):
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app)

    protocol.data_received(b"NOT A VALID HTTP/2 PREFACE!!!!!!")

    assert protocol.transport.is_closing()
    assert any("Invalid HTTP/2 frame received" in record.getMessage() for record in caplog.records)


@skip_if_no_zttp_h2
async def test_resume_reading_waits_for_other_buffered_streams():
    """Ending or consuming one stream must not release transport backpressure
    while another stream's body buffer is still over the high-water mark."""
    protocol = get_connected_protocol(Response("ok", media_type="text/plain"))
    client = H2Client()

    stream = client.request(b"POST", b"/", end=False)
    protocol.data_received(client.data_to_send())

    cycle = protocol.cycles[stream.stream_id]
    cycle.body += b"x" * (HIGH_WATER_LIMIT + 1)
    protocol.flow.pause_reading()
    assert protocol.transport.read_paused

    protocol.resume_reading_if_idle()
    assert protocol.transport.read_paused

    cycle.body = bytearray()
    protocol.resume_reading_if_idle()
    assert not protocol.transport.read_paused

    protocol.data_received(frame(0x03, 0, stream.stream_id, (0x8).to_bytes(4, "big")))
    await protocol.loop.run_one()


@skip_if_no_zttp_h2
async def test_early_response_ignores_late_request_frames():
    """If the app responds before consuming the request body, frames the
    client keeps sending on that stream must be dropped, not crash."""
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app)
    client = H2Client()

    stream = client.request(b"POST", b"/", end=False)
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    status, _, _, _ = client.parse_response(protocol.transport.buffer)
    assert status == 200

    protocol.data_received(frame(0x00, 0, stream.stream_id, b"late data"))
    protocol.data_received(frame(0x00, 0x01, stream.stream_id, b"the end"))
    protocol.data_received(frame(0x03, 0, stream.stream_id, (0x8).to_bytes(4, "big")))
    assert not protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_window_update_flushes_pending_response_data():
    """A response larger than the peer's flow-control window is parked inside
    zttp and must flush once WINDOW_UPDATE frames arrive."""
    body = b"x" * 100_000

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body, "more_body": False})

    protocol = get_connected_protocol(app)
    client = H2Client()

    stream = client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    written = len(protocol.transport.buffer)
    increment = (100_000).to_bytes(4, "big")
    protocol.data_received(frame(0x08, 0, 0, increment))
    protocol.data_received(frame(0x08, 0, stream.stream_id, increment))
    assert len(protocol.transport.buffer) > written

    # Count the DATA payload on the wire directly: zttp's client cannot read a
    # body larger than its own 64 KiB receive window.
    i, received, ended = 0, 0, False
    buffer = protocol.transport.buffer
    while i + 9 <= len(buffer):
        length = int.from_bytes(buffer[i : i + 3], "big")
        if buffer[i + 3] == 0x00:
            received += length
            ended = ended or bool(buffer[i + 4] & 0x01)
        i += 9 + length
    assert received == len(body)
    assert ended


@skip_if_no_zttp_h2
async def test_app_returning_value_resets_stream():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        response = Response("Hello, world", media_type="text/plain")
        await response(scope, receive, send)
        return 123

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert not protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.RstStream) for event in events)


@skip_if_no_zttp_h2
async def test_send_after_rst_stream_is_dropped():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        message = await receive()
        assert message["type"] == "http.disconnect"
        await send({"type": "http.response.start", "status": 200, "headers": []})

    protocol = get_connected_protocol(app)
    client = H2Client()

    stream = client.request(b"POST", b"/", end=False)
    protocol.data_received(client.data_to_send())
    protocol.data_received(frame(0x03, 0, stream.stream_id, (0x8).to_bytes(4, "big")))
    protocol.transport.clear_buffer()
    await protocol.loop.run_one()

    assert protocol.transport.buffer == b""
    assert not protocol.transport.is_closing()


@skip_if_no_zttp_h2
async def test_response_body_before_start_returns_500():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.body", "body": b"oops", "more_body": False})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    status, _, body, _ = client.parse_response(protocol.transport.buffer)
    assert status == 500
    assert body == b"Internal Server Error"


@skip_if_no_zttp_h2
async def test_unexpected_message_after_start_resets_stream():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.start", "status": 200, "headers": []})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert not protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.RstStream) for event in events)


@skip_if_no_zttp_h2
async def test_unexpected_message_after_completion_resets_stream():
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        response = Response("Hello, world", media_type="text/plain")
        await response(scope, receive, send)
        await send({"type": "http.response.body", "body": b"extra", "more_body": False})

    protocol = get_connected_protocol(app)
    client = H2Client()

    client.request(b"GET", b"/")
    protocol.data_received(client.data_to_send())
    await protocol.loop.run_one()

    assert not protocol.transport.is_closing()
    events = client.events(protocol.transport.buffer)
    assert any(isinstance(event, zttp.RstStream) for event in events)


@skip_if_no_zttp_h2
async def test_eof_received_is_a_no_op():
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app)
    assert protocol.eof_received() is None


@skip_if_no_zttp_h2
async def test_trace_logging(caplog: pytest.LogCaptureFixture, logging_config: dict[str, Any]):
    app = Response("Hello, world", media_type="text/plain")
    protocol = get_connected_protocol(app, log_level="trace", log_config=logging_config)
    protocol.connection_lost(None)

    messages = [record.message for record in caplog.records if record.name == "uvicorn.error"]
    assert any("HTTP/2 connection made" in message for message in messages)
    assert any("HTTP/2 connection lost" in message for message in messages)


@skip_if_no_zttp_h2
async def test_alpn_h2_selects_http2():
    app = Response("Hello, world", media_type="text/plain")
    _, transport, _ = get_negotiator(app, alpn_protocol="h2", sslcontext=True)
    assert isinstance(transport.get_protocol(), ZttpH2Protocol)


@skip_if_no_zttp_h2
async def test_alpn_http11_selects_http1():
    app = Response("Hello, world", media_type="text/plain")
    _, transport, _ = get_negotiator(app, alpn_protocol="http/1.1", sslcontext=True)
    assert isinstance(transport.get_protocol(), ZttpProtocol)


@skip_if_no_zttp_h2
async def test_prior_knowledge_preface_selects_http2():
    app = Response("Hello, world", media_type="text/plain")
    negotiator, transport, _ = get_negotiator(app)
    client = H2Client()

    client.request(b"GET", b"/")
    negotiator.data_received(client.data_to_send())

    h2_protocol = transport.get_protocol()
    assert isinstance(h2_protocol, ZttpH2Protocol)
    await h2_protocol.loop.run_one()

    status, _, body, _ = client.parse_response(transport.buffer)
    assert status == 200
    assert body == b"Hello, world"


@skip_if_no_zttp_h2
async def test_prior_knowledge_preface_split_across_packets():
    app = Response("Hello, world", media_type="text/plain")
    negotiator, transport, _ = get_negotiator(app)
    client = H2Client()

    client.request(b"GET", b"/")
    wire = client.data_to_send()
    negotiator.data_received(wire[:10])
    assert transport.get_protocol() is None
    negotiator.data_received(wire[10:])

    h2_protocol = transport.get_protocol()
    assert isinstance(h2_protocol, ZttpH2Protocol)
    await h2_protocol.loop.run_one()

    status, _, body, _ = client.parse_response(transport.buffer)
    assert status == 200
    assert body == b"Hello, world"


@skip_if_no_zttp_h2
async def test_http1_request_selects_http1():
    app = Response("Hello, world", media_type="text/plain")
    negotiator, transport, loop = get_negotiator(app)

    negotiator.data_received(b"GET / HTTP/1.1\r\nHost: example.org\r\n\r\n")
    await loop.run_one()

    assert isinstance(transport.get_protocol(), ZttpProtocol)
    assert b"HTTP/1.1 200 OK" in transport.buffer
    assert b"Hello, world" in transport.buffer


@skip_if_no_zttp_h2
async def test_tls_without_alpn_selects_http1():
    app = Response("Hello, world", media_type="text/plain")
    _, transport, loop = get_negotiator(app, sslcontext=True)

    protocol = transport.get_protocol()
    assert isinstance(protocol, ZttpProtocol)
    protocol.data_received(b"GET / HTTP/1.1\r\nHost: example.org\r\n\r\n")
    await loop.run_one()

    assert b"HTTP/1.1 200 OK" in transport.buffer


@skip_if_no_zttp_h2
async def test_negotiator_times_out_silent_connection():
    app = Response("Hello, world", media_type="text/plain")
    negotiator, transport, loop = get_negotiator(app)

    loop.run_later(with_delay=5)
    assert transport.closed
    negotiator.connection_lost(None)


@skip_if_no_zttp_h2
async def test_negotiator_shutdown_closes_connection():
    app = Response("Hello, world", media_type="text/plain")
    negotiator, transport, _ = get_negotiator(app)

    negotiator.shutdown()
    assert transport.closed
    negotiator.eof_received()
    negotiator.connection_lost(None)


@skip_if_no_zttp_h2
async def test_config_http_zttp_loads_negotiator():
    config = Config(app=Response("ok"), http="zttp")
    config.load()
    assert config.http_protocol_class is AutoZttpProtocol


@skip_if_no_zttp_h2
async def test_config_http_zttp1_loads_http1_protocol():
    config = Config(app=Response("ok"), http="zttp1")
    config.load()
    assert config.http_protocol_class is ZttpProtocol


@skip_if_no_zttp_h2
async def test_config_http_zttp2_loads_http2_protocol():
    config = Config(app=Response("ok"), http="zttp2")
    config.load()
    assert config.http_protocol_class is ZttpH2Protocol


@skip_if_no_zttp_h2
@pytest.mark.parametrize("http", ["zttp", CustomH2Protocol], ids=["zttp", "custom"])
@skip_if_no_zttp_h2
async def test_config_http_protocol_offers_alpn_protocols(
    http: str | type[asyncio.Protocol],
    tls_ca_certificate_pem_path: str,
    tls_ca_certificate_private_key_path: str,
):
    config = Config(
        app=Response("ok"),
        http=http,
        ssl_certfile=tls_ca_certificate_pem_path,
        ssl_keyfile=tls_ca_certificate_private_key_path,
    )
    config.load()
    assert config.ssl is not None

    client_context = ssl.create_default_context()
    client_context.check_hostname = False
    client_context.verify_mode = ssl.CERT_NONE
    client_context.set_alpn_protocols(["h2"])

    server_incoming = ssl.MemoryBIO()
    server_outgoing = ssl.MemoryBIO()
    client_incoming = ssl.MemoryBIO()
    client_outgoing = ssl.MemoryBIO()
    server = config.ssl.wrap_bio(server_incoming, server_outgoing, server_side=True)
    client = client_context.wrap_bio(
        client_incoming,
        client_outgoing,
        server_side=False,
        server_hostname="localhost",
    )

    client_done = False
    server_done = False
    for _ in range(10):
        if not client_done:
            try:
                client.do_handshake()
                client_done = True
            except ssl.SSLWantReadError:
                pass
        server_incoming.write(client_outgoing.read())

        if not server_done:
            try:
                server.do_handshake()
                server_done = True
            except ssl.SSLWantReadError:
                pass
        client_incoming.write(server_outgoing.read())
        if client_done and server_done:
            break

    assert client_done and server_done
    assert client.selected_alpn_protocol() == "h2"
    assert server.selected_alpn_protocol() == "h2"


# --- httpunk-specific internals ---------------------------------------------------


@skip_if_no_httpunk
async def test_start_only_response(unused_tcp_port: int):
    """An app that starts a response but never sends a body still flushes an empty one."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})

    config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config):
        async with httpx2.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert response.status_code == 204
    assert response.text == ""


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"connection": b"Upgrade", "upgrade": b"websocket"}, True),
        ({"connection": b"keep-alive, Upgrade", "upgrade": b"WebSocket"}, True),
        ({"connection": b"keep-alive"}, False),
        ({"connection": b"Upgrade", "upgrade": b"h2c"}, False),
    ],
)
@skip_if_no_httpunk
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


@skip_if_no_httpunk
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


@skip_if_no_httpunk
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
            async with httpx2.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    finally:
        access_logger.removeHandler(handler)
    assert response.status_code == 200
    assert any('"GET / HTTP/1.1" 200' in record.getMessage() for record in records)


@skip_if_no_httpunk
async def test_keepalive_and_total_requests(unused_tcp_port: int):
    """Sequential requests reuse the connection and are counted in `total_requests`."""
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    config: Config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config) as server:
        async with httpx2.AsyncClient() as client:
            await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
            await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert server.server_state.total_requests == 2


@skip_if_no_httpunk
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
        async with httpx2.AsyncClient() as client:
            response = await client.post(f"http://127.0.0.1:{unused_tcp_port}/", content=b"request-payload")
    assert response.status_code == 200
    assert response.text == "request-payload"


@skip_if_no_httpunk
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
        async with httpx2.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert response.status_code == 200
    assert response.text == "tick;tock"
    assert response.headers.get("transfer-encoding") == "chunked"


@skip_if_no_httpunk
async def test_h1_app_exception_returns_500(unused_tcp_port: int):
    from uvicorn.protocols.http.httpunk_impl import HTTPunkH1Protocol

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        raise RuntimeError("boom")

    config: Config = Config(app=app, loop="asyncio", port=unused_tcp_port, http=HTTPunkH1Protocol, log_level="warning")
    async with run_server(config):
        async with httpx2.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{unused_tcp_port}/")
    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert response.headers.get("server") == "uvicorn"


@skip_if_no_httpunk
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
        async with httpx2.AsyncClient() as client:
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
