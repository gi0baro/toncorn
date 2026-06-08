from __future__ import annotations

import collections
import contextlib
import http
import logging
import re
import urllib.parse
from typing import Any, Literal

import httptools
import tonio
import tonio.colored
import tonio.colored.time

from uvicorn._types import (
    ASGI3Application,
    ASGIReceiveEvent,
    ASGISendEvent,
    HTTPRequestEvent,
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
from uvicorn.server import ServerState

HEADER_RE = re.compile(b'[\x00-\x1f\x7f()<>@,;:\\[\\]={} \t\\\\"]')
HEADER_VALUE_RE = re.compile(b"[\x00-\x08\x0a-\x1f\x7f]")


def _get_status_line(status_code: int) -> bytes:
    try:
        phrase = http.HTTPStatus(status_code).phrase.encode()
    except ValueError:
        phrase = b""
    return b"".join([b"HTTP/1.1 ", str(status_code).encode(), b" ", phrase, b"\r\n"])


STATUS_LINE = {status_code: _get_status_line(status_code) for status_code in range(100, 600)}

logger = logging.getLogger("uvicorn.error")
access_logger = logging.getLogger("uvicorn.access")


async def handle(
    stream: Stream,
    config: Config,
    server_state: ServerState,
    app_state: dict[str, Any],
) -> None:
    cb = _Callbacks(config, server_state, app_state, stream)
    parser = httptools.HttpRequestParser(cb)
    try:
        parser.set_dangerous_leniencies(lenient_data_after_close=True)
    except AttributeError:  # pragma: no cover - httptools < 0.6.3
        pass
    cb.parser = parser

    access_log = access_logger.hasHandlers()

    while True:
        # Read bytes from the stream until at least one cycle is dispatched.
        # Pipelined requests parsed during a previous cycle's body read may
        # have already populated dispatch_queue — in that case we skip the
        # read entirely.
        while not cb.dispatch_queue:
            data, ok = await tonio.colored.time.timeout(stream.receive_some(), config.timeout_keep_alive)
            if not ok:
                return
            if not data:
                return
            try:
                parser.feed_data(data)
            except httptools.HttpParserError:
                logger.warning("Invalid HTTP request received.")
                await _send_simple(stream, 400, b"Invalid HTTP request received.", server_state)
                return
            except httptools.HttpParserUpgrade:
                await _handle_upgrade(stream, parser, cb, config, server_state, app_state)
                return

        cycle = cb.dispatch_queue.popleft()
        cycle.stream = stream
        cycle.parser = parser
        cycle.access_log = access_log
        cycle.default_headers = server_state.default_headers

        try:
            await cycle.run_asgi()
            await cycle.drain_pending_body()
        except httptools.HttpParserUpgrade:
            # Triggered if feed_data inside cycle.receive / drain encounters an
            # upgrade — extremely unlikely (pipelined upgrade after a normal
            # request) but handled for completeness.
            await _handle_upgrade(stream, parser, cb, config, server_state, app_state)
            return
        except httptools.HttpParserError:
            return

        server_state.total_requests += 1
        if cycle.disconnected or not cycle.keep_alive:
            return


async def _handle_upgrade(
    stream: Stream,
    parser: httptools.HttpRequestParser,
    cb: _Callbacks,
    config: Config,
    server_state: ServerState,
    app_state: dict[str, Any],
) -> None:
    if not _is_websocket_upgrade(cb.headers):
        await _send_simple(stream, 400, b"Invalid HTTP request received.", server_state)
        return
    ws_handler = config.ws_protocol_class
    if ws_handler is None:
        logger.warning(
            "No supported WebSocket library detected. "
            "Please use \"pip install 'uvicorn[standard]'\", or install 'websockets' or 'wsproto' manually."
        )
        await _send_simple(stream, 426, b"Upgrade Required", server_state)
        return
    request_bytes = _rebuild_request_bytes(parser, cb)
    await ws_handler(stream, config, server_state, app_state, request_bytes=request_bytes)


async def _send_simple(stream: Stream, status: int, body: bytes, server_state: ServerState) -> None:
    content = [STATUS_LINE[status]]
    for name, value in server_state.default_headers:
        content.extend([name, b": ", value, b"\r\n"])
    content.extend(
        [
            b"content-type: text/plain; charset=utf-8\r\n",
            b"content-length: " + str(len(body)).encode("ascii") + b"\r\n",
            b"connection: close\r\n",
            b"\r\n",
            body,
        ]
    )
    with contextlib.suppress(OSError):
        await stream.send_all(b"".join(content))


def _rebuild_request_bytes(parser: httptools.HttpRequestParser, cb: _Callbacks) -> bytes:
    method = parser.get_method()
    http_version = parser.get_http_version()
    parts: list[bytes] = [method, b" ", cb.url, b" HTTP/", http_version.encode("ascii"), b"\r\n"]
    for name, value in cb.headers:
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


class _Callbacks:
    """httptools parser callbacks that mutate cycle state synchronously.

    Single-task design: callbacks fire inside `parser.feed_data(...)` which is
    called from the same coroutine that processes cycles. No channel, no
    inter-task synchronization — cycle.body and cycle.more_body are updated
    in-place and visible to `cycle.receive` immediately after feed_data returns.
    """

    def __init__(
        self,
        config: Config,
        server_state: ServerState,
        app_state: dict[str, Any],
        stream: Stream,
    ) -> None:
        self.config = config
        self.server_state = server_state
        self.app_state = app_state
        self.parser: httptools.HttpRequestParser | None = None

        self.server_addr = get_local_addr(stream)
        self.client_addr = get_remote_addr(stream)
        self.scheme: Literal["http", "https"] = "https" if is_ssl(stream) else "http"

        # Per-request scratch state populated before on_headers_complete.
        self.url = b""
        self.headers: list[tuple[bytes, bytes]] = []
        self.expect_100_continue = False
        self.scope: HTTPScope | None = None

        # Cycle currently being filled by body/end callbacks. Set in
        # on_headers_complete; persists until the next on_headers_complete
        # replaces it.
        self.parsing: _Cycle | None = None
        # Cycles built and not yet handed to the outer handle loop.
        self.dispatch_queue: collections.deque[_Cycle] = collections.deque()

    def on_message_begin(self) -> None:
        self.url = b""
        self.headers = []
        self.expect_100_continue = False
        self.scope = {  # type: ignore[typeddict-item]
            "type": "http",
            "asgi": {"version": self.config.asgi_version, "spec_version": "2.3"},
            "http_version": "1.1",
            "server": self.server_addr,
            "client": self.client_addr,
            "scheme": self.scheme,
            "root_path": self.config.root_path,
            "headers": self.headers,
            "state": self.app_state.copy(),
        }

    def on_url(self, url: bytes) -> None:
        self.url += url

    def on_header(self, name: bytes, value: bytes) -> None:
        name = name.lower()
        if name == b"expect" and value.lower() == b"100-continue":
            self.expect_100_continue = True
        self.headers.append((name, value))

    def on_headers_complete(self) -> None:
        assert self.parser is not None
        assert self.scope is not None

        http_version = self.parser.get_http_version()
        method = self.parser.get_method()
        self.scope["method"] = method.decode("ascii")
        if http_version != "1.1":
            self.scope["http_version"] = http_version

        # WS upgrades: feed_data will raise HttpParserUpgrade right after this
        # callback. Don't build a cycle for it; the outer handle loop catches
        # the exception and dispatches.
        if self.parser.should_upgrade() and _is_websocket_upgrade(self.headers):
            return

        parsed_url = httptools.parse_url(self.url)
        raw_path = parsed_url.path
        path = raw_path.decode("ascii")
        if "%" in path:
            path = urllib.parse.unquote(path)
        full_path = self.config.root_path + path
        full_raw_path = self.config.root_path.encode("ascii") + raw_path
        self.scope["path"] = full_path
        self.scope["raw_path"] = full_raw_path
        self.scope["query_string"] = parsed_url.query or b""

        if self.config.limit_concurrency is not None and (
            len(self.server_state.connections) >= self.config.limit_concurrency
        ):
            logger.warning("Exceeded concurrency limit.")
            app: Any = service_unavailable
        else:
            app = self.config.loaded_app

        cycle = _Cycle(
            scope=self.scope,
            app=app,
            expect_100_continue=self.expect_100_continue,
            keep_alive=http_version != "1.0",
        )
        self.parsing = cycle
        self.dispatch_queue.append(cycle)

    def on_body(self, body: bytes) -> None:
        cycle = self.parsing
        if cycle is None or cycle.response_complete:
            return
        cycle.body.extend(body)

    def on_message_complete(self) -> None:
        cycle = self.parsing
        if cycle is None or cycle.response_complete:
            return
        cycle.more_body = False


class _Cycle:
    """One request/response exchange.

    Sole owner of the stream during run_asgi. `receive()` pulls bytes from
    the stream and feeds the parser inline; callbacks update `body` and
    `more_body` synchronously, so we never need to await on a separate event.
    """

    def __init__(
        self,
        scope: HTTPScope,
        app: ASGI3Application,
        expect_100_continue: bool,
        keep_alive: bool,
    ) -> None:
        self.scope = scope
        self.app = app

        # Wired in by handle() before run_asgi.
        self.stream: Stream = None  # type: ignore[assignment]
        self.parser: httptools.HttpRequestParser = None  # type: ignore[assignment]
        self.access_log: bool = False
        self.default_headers: list[tuple[bytes, bytes]] = []

        self.disconnected = False
        self.keep_alive = keep_alive
        self.waiting_for_100_continue = expect_100_continue

        self.body = bytearray()
        self.more_body = True
        self.response_started = False
        self.response_complete = False
        self.chunked_encoding: bool | None = None
        self.expected_content_length = 0

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
        """If the app returned without reading the full body, drive the parser
        until on_message_complete fires so the parser is positioned at the
        next request.
        """
        while self.more_body and not self.disconnected:
            try:
                data = await self.stream.receive_some()
            except OSError:
                self.disconnected = True
                return
            if not data:
                self.disconnected = True
                return
            try:
                self.parser.feed_data(data)
            except httptools.HttpParserError:
                self.disconnected = True
                return
            # HttpParserUpgrade is intentionally left to propagate; the outer
            # handle loop wraps run_asgi/drain_pending_body in a try.

    async def _send_500_response(self) -> None:
        await self.send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [
                    (b"content-type", b"text/plain; charset=utf-8"),
                    (b"content-length", b"21"),
                    (b"connection", b"close"),
                ],
            }
        )
        await self.send({"type": "http.response.body", "body": b"Internal Server Error", "more_body": False})

    async def send(self, message: ASGISendEvent) -> None:
        if self.disconnected:
            return

        if not self.response_started:
            if message["type"] != "http.response.start":
                raise RuntimeError(f"Expected ASGI message 'http.response.start', but got '{message['type']}'.")
            self.response_started = True
            self.waiting_for_100_continue = False

            status_code = message["status"]
            headers = self.default_headers + list(message.get("headers", []))

            if CLOSE_HEADER in self.scope["headers"] and CLOSE_HEADER not in headers:
                headers = headers + [CLOSE_HEADER]

            if self.access_log:
                access_logger.info(
                    '%s - "%s %s HTTP/%s" %d',
                    get_client_addr(self.scope),
                    self.scope["method"],
                    get_path_with_query_string(self.scope),
                    self.scope["http_version"],
                    status_code,
                )

            content = [STATUS_LINE[status_code]]
            for name, value in headers:
                if HEADER_RE.search(name):
                    raise RuntimeError("Invalid HTTP header name.")  # pragma: no cover
                if HEADER_VALUE_RE.search(value):
                    raise RuntimeError("Invalid HTTP header value.")

                name = name.lower()
                if name == b"content-length" and self.chunked_encoding is None:
                    self.expected_content_length = int(value.decode())
                    self.chunked_encoding = False
                elif name == b"transfer-encoding" and value.lower() == b"chunked":
                    self.expected_content_length = 0
                    self.chunked_encoding = True
                elif name == b"connection" and value.lower() == b"close":
                    self.keep_alive = False
                content.extend([name, b": ", value, b"\r\n"])

            if self.chunked_encoding is None and self.scope["method"] != "HEAD" and status_code not in (204, 304):
                self.chunked_encoding = True
                content.append(b"transfer-encoding: chunked\r\n")

            content.append(b"\r\n")
            try:
                await self.stream.send_all(b"".join(content))
            except OSError:
                self.disconnected = True
                return

        elif not self.response_complete:
            if message["type"] != "http.response.body":
                raise RuntimeError(f"Expected ASGI message 'http.response.body', but got '{message['type']}'.")

            body = message.get("body", b"")
            more_body = message.get("more_body", False)

            if self.scope["method"] == "HEAD":
                self.expected_content_length = 0
                out: bytes = b""
            elif self.chunked_encoding:
                if body:
                    out = b"%x\r\n%s\r\n" % (len(body), body)
                else:
                    out = b""
                if not more_body:
                    out = out + b"0\r\n\r\n"
            else:
                num_bytes = len(body)
                if num_bytes > self.expected_content_length:
                    raise RuntimeError("Response content longer than Content-Length")
                self.expected_content_length -= num_bytes
                out = body

            if out:
                try:
                    await self.stream.send_all(out)
                except OSError:
                    self.disconnected = True
                    return

            if not more_body:
                if self.expected_content_length != 0:
                    raise RuntimeError("Response content shorter than Content-Length")
                self.response_complete = True

        else:
            raise RuntimeError(f"Unexpected ASGI message '{message['type']}' sent, after response already completed.")

    async def receive(self) -> ASGIReceiveEvent:
        if self.waiting_for_100_continue:
            try:
                await self.stream.send_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            except OSError:
                self.disconnected = True
            self.waiting_for_100_continue = False

        # Read bytes and feed parser inline until we have body data to return
        # or the body is fully consumed.
        while not self.body and self.more_body and not self.disconnected:
            try:
                data = await self.stream.receive_some()
            except OSError:
                self.disconnected = True
                break
            if not data:
                self.disconnected = True
                break
            try:
                self.parser.feed_data(data)
            except httptools.HttpParserError:
                self.disconnected = True
                break
            # HttpParserUpgrade propagates to handle().

        if self.disconnected and not self.body:
            return {"type": "http.disconnect"}

        chunk = bytes(self.body)
        self.body = bytearray()
        message: HTTPRequestEvent = {
            "type": "http.request",
            "body": chunk,
            "more_body": self.more_body,
        }
        return message
