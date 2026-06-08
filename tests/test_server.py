from __future__ import annotations

import contextvars
import json
import logging
import socket
from typing import Any

import httpx
import pytest
import tonio.colored
import tonio.colored.time

from tests.utils import run_server
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import Config
from uvicorn.protocols.http.flow_control import HIGH_WATER_LIMIT

pytestmark = pytest.mark.tonio


SIMPLE_GET_REQUEST = b"\r\n".join([b"GET / HTTP/1.1", b"Host: example.org", b"", b""])


async def _ok_app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
    assert scope["type"] == "http"
    await receive()
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def test_shutdown_on_early_exit_during_startup(unused_tcp_port: int):
    """`lifespan.shutdown` runs even if should_exit flips during startup."""
    seen: dict[str, bool] = {"startup": False, "shutdown": False}

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        if scope["type"] != "lifespan":
            return
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await tonio.colored.time.sleep(0.5)
                await send({"type": "lifespan.startup.complete"})
                seen["startup"] = True
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                seen["shutdown"] = True
                return

    config = Config(app=app, lifespan="on", port=unused_tcp_port)

    async with run_server(config) as server:
        # Flip should_exit during the 500ms startup sleep.
        async def trip():
            await tonio.colored.time.sleep(0.2)
            server.should_exit = True

        tonio.colored.spawn.without_tracking(trip())
        # run_server will await server.serve until it returns.

    assert seen["startup"]
    assert seen["shutdown"], "lifespan.shutdown should run even on early exit"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def test_request_than_limit_max_requests_warn_log(unused_tcp_port: int, http_protocol_cls: Any):
    config = Config(app=_ok_app, limit_max_requests=1, port=unused_tcp_port, http=http_protocol_cls)
    config.load()
    err = logging.getLogger("uvicorn.error")
    cap = _Capture()
    cap.setLevel(logging.INFO)
    err.addHandler(cap)
    try:
        async with run_server(config) as server:
            with httpx.Client() as client:
                client.get(f"http://127.0.0.1:{unused_tcp_port}")
            # Wait for main_loop's on_tick to observe total_requests >= 1 and
            # trigger self-termination. on_tick fires every 100ms.
            for _ in range(20):
                if server.should_exit:
                    break
                await tonio.colored.time.sleep(0.05)
    finally:
        err.removeHandler(cap)
    messages = [r.getMessage() for r in cap.records]
    assert any("Maximum request limit of 1 exceeded" in m for m in messages), messages


async def test_limit_max_requests_jitter(unused_tcp_port: int, http_protocol_cls: Any):
    config = Config(
        app=_ok_app,
        limit_max_requests=1,
        limit_max_requests_jitter=2,
        port=unused_tcp_port,
        http=http_protocol_cls,
    )
    config.load()
    err = logging.getLogger("uvicorn.error")
    cap = _Capture()
    cap.setLevel(logging.INFO)
    err.addHandler(cap)
    try:
        async with run_server(config) as server:
            limit = server.limit_max_requests
            assert limit is not None
            assert 1 <= limit <= 3
            with httpx.Client() as client:
                for _ in range(limit):
                    client.get(f"http://127.0.0.1:{unused_tcp_port}")
            for _ in range(20):
                if server.should_exit:
                    break
                await tonio.colored.time.sleep(0.05)
    finally:
        err.removeHandler(cap)
    messages = [r.getMessage() for r in cap.records]
    assert any(f"Maximum request limit of {limit} exceeded" in m for m in messages), messages


def _raw_request(port: int, request: bytes) -> dict[str, Any]:
    """Send a raw HTTP/1.1 request via a sync socket and parse the JSON body."""
    s = socket.socket()
    s.settimeout(5.0)
    s.connect(("127.0.0.1", port))
    try:
        s.sendall(request)
        buf = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
            head_end = buf.find(b"\r\n\r\n")
            if head_end != -1:
                head = buf[:head_end]
                body_start = head_end + 4
                content_length = 0
                for line in head.split(b"\r\n"):
                    if line.lower().startswith(b"content-length:"):
                        content_length = int(line.split(b":", 1)[1].strip())
                if len(buf) - body_start >= content_length:
                    return json.loads(buf[body_start : body_start + content_length])
        raise RuntimeError(f"unexpected end of response: {buf!r}")
    finally:
        s.close()


async def test_contextvars_preserved_by_default(http_protocol_cls: Any, unused_tcp_port: int):
    """By default, context set outside the ASGI task is visible inside it."""
    ctx: contextvars.ContextVar[str] = contextvars.ContextVar("ctx")
    ctx.set("outer-value")

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        assert scope["type"] == "http"
        while True:
            message = await receive()
            assert message["type"] == "http.request"
            if not message["more_body"]:
                break
        body = json.dumps({"ctx": ctx.get("MISSING")}).encode("utf-8")
        headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    config = Config(app=app, port=unused_tcp_port, http=http_protocol_cls)
    async with run_server(config):
        assert _raw_request(unused_tcp_port, SIMPLE_GET_REQUEST) == {"ctx": "outer-value"}


async def test_reset_contextvars(http_protocol_cls: Any, unused_tcp_port: int):
    """`reset_contextvars=True` gives each ASGI run a fresh context."""
    default_contextvars = {c.name for c in contextvars.copy_context().keys()}
    ctx: contextvars.ContextVar[str] = contextvars.ContextVar("ctx")

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        assert scope["type"] == "http"
        initial_context = {
            n: v for c, v in contextvars.copy_context().items() if (n := c.name) not in default_contextvars
        }
        ctx.set(scope["path"])
        while True:
            message = await receive()
            assert message["type"] == "http.request"
            if not message["more_body"]:
                break
        body = json.dumps(initial_context).encode("utf-8")
        headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    # body larger than HIGH_WATER_LIMIT used to expose contextvar pollution under
    # asyncio. Keep the coverage even though the new pull-style impl doesn't have
    # the same code path.
    large_body = b"a" * (HIGH_WATER_LIMIT + 1)
    large_request = b"\r\n".join(
        [
            b"POST /large-body HTTP/1.1",
            b"Host: example.org",
            b"Content-Type: application/octet-stream",
            f"Content-Length: {len(large_body)}".encode(),
            b"",
            large_body,
        ]
    )

    config = Config(app=app, port=unused_tcp_port, http=http_protocol_cls, reset_contextvars=True)
    async with run_server(config):
        assert _raw_request(unused_tcp_port, large_request) == {}
        assert _raw_request(unused_tcp_port, SIMPLE_GET_REQUEST) == {}
