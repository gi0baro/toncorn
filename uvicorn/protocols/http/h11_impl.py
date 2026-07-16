from __future__ import annotations

import contextlib
import http
import logging
import socket as _stdlib_socket
import time
from typing import Any, Literal
from urllib.parse import unquote

import h11
import tonio
import tonio.colored
import tonio.colored.time
from h11._connection import DEFAULT_MAX_INCOMPLETE_EVENT_SIZE
from tonio.colored.net.tls import TLSStream

from uvicorn._types import (
    ASGI3Application,
    ASGIReceiveEvent,
    ASGISendEvent,
    HTTPRequestEvent,
    HTTPResponseBodyEvent,
    HTTPResponseStartEvent,
    HTTPScope,
)
from uvicorn.config import Config
from uvicorn.protocols.http.flow_control import CLOSE_HEADER, service_unavailable
from uvicorn.protocols.utils import (
    Stream,
    get_client_addr,
    get_local_addr,
    get_path_with_query_string,
    get_remote_addr,
    is_ssl,
)
from uvicorn.server import Connection, ServerState


def _get_status_phrase(status_code: int) -> bytes:
    try:
        return http.HTTPStatus(status_code).phrase.encode()
    except ValueError:
        return b""


STATUS_PHRASES = {status_code: _get_status_phrase(status_code) for status_code in range(100, 600)}

logger = logging.getLogger("uvicorn.error")
access_logger = logging.getLogger("uvicorn.access")


class _Upgrade:
    """Marker returned from `_read_request_event` for websocket upgrades."""

    __slots__ = ("request_bytes",)

    def __init__(self, request_bytes: bytes) -> None:
        self.request_bytes = request_bytes


async def _recv_plain(
    stream: Stream, timeout_seconds: float, conn_ref: Connection | None, expect_data: bool
) -> bytes | None:
    """Receive bytes on a plain socket, parking on its own IO registration.

    There is no per-read timeout here (``timeout_seconds`` is unused): while
    parked, ``conn_ref.idle_since`` is set so the server's keep-alive watchdog
    can reap the connection by shutting the socket down, which wakes us with
    an EOF (empty bytes) — never None.
    """
    sock = stream.socket
    if expect_data:
        try:
            return sock._sock.recv(65536, 0)
        except BlockingIOError, InterruptedError:
            pass

    if conn_ref is not None:
        conn_ref.idle_since = time.monotonic()
    try:
        while True:
            # Same arm/clear discipline as tonio's own recv: arm returns None
            # once readiness is flagged; the flag is only cleared after the
            # kernel buffer is proven drained (tick-guarded, so an edge that
            # raced in stays intact).
            if (waiter := sock._io_arm_r()) is not None:
                await waiter
                continue
            try:
                data = sock._sock.recv(65536, 0)
            except BlockingIOError, InterruptedError:
                sock._io_clear_r()
                continue
            if len(data) < 65536:
                # The buffer was fully drained by this recv. Proactively clear
                # readiness so the next call parks directly instead of paying
                # a wasted recv syscall against an empty buffer.
                sock._io_clear_r()
            return data
    finally:
        if conn_ref is not None:
            conn_ref.idle_since = None


async def _recv_tls(
    stream: TLSStream, timeout_seconds: float, conn_ref: Connection | None, expect_data: bool
) -> bytes | None:
    data, ok = await tonio.colored.time.timeout(stream.receive_some(), timeout_seconds)
    return data if ok else None


async def handle(
    stream: Stream,
    config: Config,
    server_state: ServerState,
    app_state: dict[str, Any],
) -> None:
    if isinstance(stream, TLSStream):
        _recv = _recv_tls
        try:
            stream.transport.socket.setsockopt(_stdlib_socket.IPPROTO_TCP, _stdlib_socket.TCP_NODELAY, 1)
        except OSError, NameError:
            pass
    else:
        _recv = _recv_plain
        try:
            stream.socket.setsockopt(_stdlib_socket.IPPROTO_TCP, _stdlib_socket.TCP_NODELAY, 1)
        except OSError, NameError:
            pass

    max_event_size = (
        config.h11_max_incomplete_event_size
        if config.h11_max_incomplete_event_size is not None
        else DEFAULT_MAX_INCOMPLETE_EVENT_SIZE
    )
    conn = h11.Connection(h11.SERVER, max_event_size)

    server_addr = get_local_addr(stream)
    client_addr = get_remote_addr(stream)
    scheme: Literal["http", "https"] = "https" if is_ssl(stream) else "http"
    access_log = access_logger.hasHandlers()
    # The server registers the raw (pre-TLS-wrap) stream, so this is None for
    # TLS connections and direct handler invocations (tests) — both of which
    # don't rely on the keep-alive watchdog.
    conn_ref = server_state.connections.get(id(stream))
    expect_data = True

    while True:
        request = await _read_request_event(stream, conn, config, server_state, conn_ref, _recv, expect_data)
        expect_data = False
        if request is None:
            return
        if isinstance(request, _Upgrade):
            ws_handler = config.ws_protocol_class
            if ws_handler is None:
                logger.warning(
                    "No supported WebSocket library detected. "
                    "Please use \"pip install 'uvicorn[standard]'\", or install 'websockets' or 'wsproto' manually."
                )
                await _send_simple(stream, conn, 426, b"Upgrade Required")
                return
            await ws_handler(stream, config, server_state, app_state, request_bytes=request.request_bytes)
            return

        headers = [(key.lower(), value) for key, value in request.headers]
        raw_path, _, query_string = request.target.partition(b"?")
        path = unquote(raw_path.decode("ascii"))
        full_path = config.root_path + path
        full_raw_path = config.root_path.encode("ascii") + raw_path
        http_version = request.http_version.decode("ascii")
        scope: HTTPScope = {
            "type": "http",
            "asgi": {"version": config.asgi_version, "spec_version": "2.3"},
            "http_version": http_version,
            "server": server_addr,
            "client": client_addr,
            "scheme": scheme,
            "method": request.method.decode("ascii"),
            "root_path": config.root_path,
            "path": full_path,
            "raw_path": full_raw_path,
            "query_string": query_string,
            "headers": headers,
            "state": app_state.copy(),
        }

        if config.limit_concurrency is not None and (len(server_state.connections) >= config.limit_concurrency):
            logger.warning("Exceeded concurrency limit.")
            app: Any = service_unavailable
        else:
            app = config.loaded_app

        cycle = _Cycle(
            scope=scope,
            stream=stream,
            conn=conn,
            app=app,
            default_headers=server_state.default_headers,
            access_log=access_log,
        )

        await cycle.run_asgi()
        await cycle.drain_pending_body()
        server_state.total_requests += 1

        if cycle.disconnected or not cycle.keep_alive:
            return
        if conn.our_state is h11.MUST_CLOSE or conn.their_state is h11.MUST_CLOSE:
            return
        try:
            conn.start_next_cycle()
        except h11.LocalProtocolError:  # pragma: no cover
            return


async def _read_request_event(
    stream: Stream,
    conn: h11.Connection,
    config: Config,
    server_state: ServerState,
    conn_ref: Connection | None,
    _recv: Any,
    expect_data: bool,
) -> h11.Request | _Upgrade | None:
    """Drive `conn` until a Request event is available.

    Returns the Request, an `_Upgrade` marker for ws upgrades, or `None` if
    the connection should be torn down (idle timeout, EOF, or protocol error).

    ``expect_data`` is True only on the initial call for a fresh connection
    (the request may already be in the kernel buffer from accept). On
    subsequent calls, and after any recv inside this function, we know the
    kernel buffer was just drained and there's no point trying a
    speculative non-blocking read.
    """
    while True:
        try:
            event = conn.next_event()
        except h11.RemoteProtocolError:
            logger.warning("Invalid HTTP request received.")
            await _send_simple(stream, conn, 400, b"Invalid HTTP request received.")
            return None

        if event is h11.NEED_DATA:
            try:
                data = await _recv(stream, config.timeout_keep_alive, conn_ref, expect_data)
            except OSError:
                return None
            if data is None:  # TLS keep-alive timeout
                return None
            try:
                conn.receive_data(data or b"")
            except h11.RemoteProtocolError:
                logger.warning("Invalid HTTP request received.")
                await _send_simple(stream, conn, 400, b"Invalid HTTP request received.")
                return None
            if not data:
                return None
            expect_data = False  # any further recv in this call goes through the slow path
            continue

        if isinstance(event, h11.Request):
            headers = [(key.lower(), value) for key, value in event.headers]
            if _is_websocket_upgrade(headers):
                return _Upgrade(_rebuild_request_bytes(event, headers))
            return event

        # PAUSED at start, ConnectionClosed, or unknown — connection is done.
        return None


async def _send_simple(stream: Stream, conn: h11.Connection, status: int, body: bytes) -> None:
    reason = STATUS_PHRASES[status]
    headers = [
        (b"content-type", b"text/plain; charset=utf-8"),
        (b"content-length", str(len(body)).encode("ascii")),
        (b"connection", b"close"),
    ]
    out: list[bytes] = []
    try:
        for evt in (
            h11.Response(status_code=status, headers=headers, reason=reason),
            h11.Data(data=body),
            h11.EndOfMessage(),
        ):
            chunk = conn.send(evt)
            if chunk:
                out.append(chunk)
    except h11.LocalProtocolError:  # pragma: no cover
        return
    if out:
        with contextlib.suppress(OSError):
            await stream.send_all(b"".join(out))


def _rebuild_request_bytes(request: h11.Request, headers: list[tuple[bytes, bytes]]) -> bytes:
    parts: list[bytes] = [
        request.method,
        b" ",
        request.target,
        b" HTTP/",
        request.http_version,
        b"\r\n",
    ]
    for name, value in headers:
        parts.extend([name, b": ", value, b"\r\n"])
    parts.append(b"\r\n")
    return b"".join(parts)


def _is_websocket_upgrade(headers: list[tuple[bytes, bytes]]) -> bool:
    connection_tokens: list[bytes] = []
    upgrade: bytes | None = None
    for name, value in headers:
        if name == b"connection":
            connection_tokens = [t.lower().strip() for t in value.split(b",")]
        elif name == b"upgrade":
            upgrade = value.lower()
    return b"upgrade" in connection_tokens and upgrade == b"websocket"


class _Cycle:
    """One request/response exchange.

    Single task: pulls h11 events from `conn` inline within `receive()` and
    `drain_pending_body()`. Writes response bytes directly to the stream via
    `conn.send(...)` (which both formats and advances h11's send-side state).
    """

    def __init__(
        self,
        scope: HTTPScope,
        stream: Stream,
        conn: h11.Connection,
        app: ASGI3Application,
        default_headers: list[tuple[bytes, bytes]],
        access_log: bool,
    ) -> None:
        self.scope = scope
        self.stream = stream
        self.conn = conn
        self.app = app
        self.default_headers = default_headers
        self.access_log = access_log

        self.disconnected = False
        self.keep_alive = True
        self.waiting_for_100_continue = conn.they_are_waiting_for_100_continue
        self.body_finished = False
        self.response_started = False
        self.response_complete = False

    async def run_asgi(self) -> None:
        try:
            result = await self.app(self.scope, self.receive, self.send)  # type: ignore[func-returns-value]
        except BaseException as exc:
            logger.error("Exception in ASGI application\n", exc_info=exc)
            if not self.response_started:
                await self._send_500_response()
            else:
                self.keep_alive = False
        else:
            if result is not None:
                logger.error("ASGI callable should return None, but returned '%s'.", result)
                self.keep_alive = False
            elif not self.response_started and not self.disconnected:
                logger.error("ASGI callable returned without starting response.")
                await self._send_500_response()
            elif not self.response_complete and not self.disconnected:
                logger.error("ASGI callable returned without completing response.")
                self.keep_alive = False

    async def drain_pending_body(self) -> None:
        """Consume any unread body events from the conn so the parser is
        positioned at the next request (or end-of-stream).
        """
        while not self.body_finished and not self.disconnected:
            try:
                event = self.conn.next_event()
            except h11.RemoteProtocolError:
                self.disconnected = True
                return
            if event is h11.NEED_DATA:
                try:
                    data = await self.stream.receive_some()
                except OSError:
                    self.disconnected = True
                    return
                if not data:
                    self.disconnected = True
                    return
                try:
                    self.conn.receive_data(data)
                except h11.RemoteProtocolError:
                    self.disconnected = True
                    return
                continue
            if isinstance(event, h11.EndOfMessage):
                self.body_finished = True
                return
            if isinstance(event, h11.Data):
                continue
            if event is h11.PAUSED:
                self.body_finished = True
                return
            # ConnectionClosed or unknown.
            self.disconnected = True
            return

    async def _send_500_response(self) -> None:
        response_start_event: HTTPResponseStartEvent = {
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"connection", b"close"),
            ],
        }
        await self.send(response_start_event)
        response_body_event: HTTPResponseBodyEvent = {
            "type": "http.response.body",
            "body": b"Internal Server Error",
            "more_body": False,
        }
        await self.send(response_body_event)

    async def send(self, message: ASGISendEvent) -> None:
        if self.disconnected:
            return

        if not self.response_started:
            if message["type"] != "http.response.start":
                raise RuntimeError(f"Expected ASGI message 'http.response.start', but got '{message['type']}'.")
            self.response_started = True
            self.waiting_for_100_continue = False

            status = message["status"]
            headers = self.default_headers + list(message.get("headers", []))

            if CLOSE_HEADER in self.scope["headers"] and CLOSE_HEADER not in headers:
                headers = headers + [CLOSE_HEADER]
            if any(name.lower() == b"connection" and value.lower() == b"close" for name, value in headers):
                self.keep_alive = False

            if self.access_log:
                access_logger.info(
                    '%s - "%s %s HTTP/%s" %d',
                    get_client_addr(self.scope),
                    self.scope["method"],
                    get_path_with_query_string(self.scope),
                    self.scope["http_version"],
                    status,
                )

            response = h11.Response(status_code=status, headers=headers, reason=STATUS_PHRASES[status])
            output = self.conn.send(event=response)
            try:
                await self.stream.send_all(output)
            except OSError:
                self.disconnected = True
                return

        elif not self.response_complete:
            if message["type"] != "http.response.body":
                raise RuntimeError(f"Expected ASGI message 'http.response.body', but got '{message['type']}'.")
            body = message.get("body", b"")
            more_body = message.get("more_body", False)

            data = b"" if self.scope["method"] == "HEAD" else body
            output = self.conn.send(event=h11.Data(data=data))
            try:
                await self.stream.send_all(output)
            except OSError:
                self.disconnected = True
                return

            if not more_body:
                self.response_complete = True
                output = self.conn.send(event=h11.EndOfMessage())
                try:
                    await self.stream.send_all(output)
                except OSError:
                    self.disconnected = True
                    return

        else:
            raise RuntimeError(f"Unexpected ASGI message '{message['type']}' sent, after response already completed.")

        if self.response_complete and (self.conn.our_state is h11.MUST_CLOSE or not self.keep_alive):
            with contextlib.suppress(h11.LocalProtocolError):
                self.conn.send(event=h11.ConnectionClosed())

    async def receive(self) -> ASGIReceiveEvent:
        if self.waiting_for_100_continue:
            informational = h11.InformationalResponse(status_code=100, headers=[], reason="Continue")
            try:
                await self.stream.send_all(self.conn.send(event=informational))
            except OSError:
                self.disconnected = True
            self.waiting_for_100_continue = False

        if self.body_finished or self.disconnected:
            return {"type": "http.disconnect"}

        # Drive `conn` inline until we have body data to return or the body
        # is fully consumed.
        while True:
            try:
                event = self.conn.next_event()
            except h11.RemoteProtocolError:
                self.disconnected = True
                return {"type": "http.disconnect"}

            if event is h11.NEED_DATA:
                try:
                    data = await self.stream.receive_some()
                except OSError:
                    self.disconnected = True
                    return {"type": "http.disconnect"}
                if not data:
                    with contextlib.suppress(h11.RemoteProtocolError):
                        self.conn.receive_data(b"")
                    self.disconnected = True
                    return {"type": "http.disconnect"}
                try:
                    self.conn.receive_data(data)
                except h11.RemoteProtocolError:
                    self.disconnected = True
                    return {"type": "http.disconnect"}
                continue

            if isinstance(event, h11.Data):
                message: HTTPRequestEvent = {
                    "type": "http.request",
                    "body": bytes(event.data),
                    "more_body": True,
                }
                return message

            if isinstance(event, h11.EndOfMessage):
                self.body_finished = True
                return {"type": "http.request", "body": b"", "more_body": False}

            if event is h11.PAUSED:
                # Body fully delivered; we just haven't emitted EndOfMessage
                # for some edge case. Treat as end-of-body.
                self.body_finished = True
                return {"type": "http.request", "body": b"", "more_body": False}

            # ConnectionClosed or unknown.
            self.disconnected = True
            return {"type": "http.disconnect"}
