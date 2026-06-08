from __future__ import annotations

import logging
import random
import struct
import time
from io import BytesIO, StringIO
from typing import Any, Literal, cast
from urllib.parse import unquote

import tonio
import tonio.colored
import tonio.colored.sync
import tonio.colored.time
import wsproto
from tonio.colored.sync.channel import unbounded as _unbounded_channel
from wsproto import ConnectionType, events
from wsproto.connection import ConnectionState
from wsproto.extensions import Extension, PerMessageDeflate
from wsproto.utilities import LocalProtocolError, RemoteProtocolError

from uvicorn._types import ASGI3Application, ASGISendEvent, WebSocketEvent, WebSocketReceiveEvent, WebSocketScope
from uvicorn.config import Config
from uvicorn.logging import TRACE_LOG_LEVEL
from uvicorn.protocols.utils import (
    ClientDisconnected,
    Stream,
    get_client_addr,
    get_local_addr,
    get_path_with_query_string,
    get_remote_addr,
    is_ssl,
)
from uvicorn.server import ServerState

logger = logging.getLogger("uvicorn.error")


class FrameTooLargeError(Exception):
    """Raised when accumulated websocket message bytes exceed `ws_max_size`."""


class WebsocketBuffer:
    def __init__(self, max_length: int) -> None:
        self.value: BytesIO | StringIO | None = None
        self.length = 0
        self.max_length = max_length

    def extend(self, event: events.TextMessage | events.BytesMessage) -> None:
        if self.value is None:
            self.value = StringIO() if isinstance(event, events.TextMessage) else BytesIO()
        self.value.write(event.data)  # type: ignore[arg-type]
        self.length += len(event.data.encode()) if isinstance(event, events.TextMessage) else len(event.data)
        if self.length > self.max_length:
            raise FrameTooLargeError

    def clear(self) -> None:
        self.value = None
        self.length = 0

    def to_message(self) -> WebSocketReceiveEvent:
        if isinstance(self.value, StringIO):
            return {"type": "websocket.receive", "text": self.value.getvalue()}
        assert isinstance(self.value, BytesIO)
        return {"type": "websocket.receive", "bytes": self.value.getvalue()}


async def handle(
    stream: Stream,
    config: Config,
    server_state: ServerState,
    app_state: dict[str, Any],
    *,
    request_bytes: bytes,
) -> None:
    app = cast(ASGI3Application, config.loaded_app)

    server_addr = get_local_addr(stream)
    client_addr = get_remote_addr(stream)
    scheme: Literal["ws", "wss"] = "wss" if is_ssl(stream) else "ws"

    conn = wsproto.WSConnection(connection_type=ConnectionType.SERVER)
    try:
        conn.receive_data(request_bytes)
    except RemoteProtocolError as err:
        try:
            await stream.send_all(conn.send(err.event_hint))  # type: ignore[arg-type]
        except OSError:
            pass
        return

    request: events.Request | None = None
    for event in conn.events():
        if isinstance(event, events.Request):
            request = event
            break
    if request is None:
        return

    headers: list[tuple[bytes, bytes]] = [(b"host", request.host.encode())]
    headers += [(key.lower(), value) for key, value in request.extra_headers]
    raw_path, _, query_string = request.target.partition("?")
    path = unquote(raw_path)
    full_path = config.root_path + path
    full_raw_path = config.root_path.encode("ascii") + raw_path.encode("ascii")
    scope: WebSocketScope = {
        "type": "websocket",
        "asgi": {"version": config.asgi_version, "spec_version": "2.4"},
        "http_version": "1.1",
        "scheme": scheme,
        "server": server_addr,
        "client": client_addr,
        "root_path": config.root_path,
        "path": full_path,
        "raw_path": full_raw_path,
        "query_string": query_string.encode("ascii"),
        "headers": headers,
        "subprotocols": request.subprotocols,
        "state": app_state.copy(),
        "extensions": {"websocket.http.response": {}},
    }

    state = _State(
        config=config,
        scope=scope,
        conn=conn,
        stream=stream,
        server_state=server_state,
    )
    state.queue_send.send({"type": "websocket.connect"})

    try:
        await tonio.colored.select(state.run_asgi(app), state.run_reader(), state.run_keepalive())
    except ExceptionGroup as eg:  # pragma: no cover
        first = eg.exceptions[0]
        if not isinstance(first, ClientDisconnected):
            raise first


class _State:
    def __init__(
        self,
        config: Config,
        scope: WebSocketScope,
        conn: wsproto.WSConnection,
        stream: Stream,
        server_state: ServerState,
    ) -> None:
        self.config = config
        self.scope = scope
        self.conn = conn
        self.stream = stream
        self.default_headers = server_state.default_headers

        self.write_lock = tonio.colored.sync.Lock()
        sender, receiver = _unbounded_channel()
        self.queue_send = sender
        self.queue_recv = receiver

        self.handshake_complete = False
        self.close_sent = False
        self.response_started = False
        self.client_gone = False
        self.done = tonio.colored.Event()

        # keepalive
        self.ping_interval = config.ws_ping_interval
        self.ping_timeout = config.ws_ping_timeout
        self.pending_ping_payload: bytes | None = None
        self.ping_sent_at: float = 0.0
        self.last_ping_rtt: float = 0.0

    async def _write(self, data: bytes) -> None:
        if not data:
            return
        async with self.write_lock:
            try:
                await self.stream.send_all(data)
            except OSError as exc:
                self.client_gone = True
                raise ClientDisconnected() from exc

    def _enqueue_disconnect(self, code: int, reason: str | None = None) -> None:
        msg: dict[str, Any] = {"type": "websocket.disconnect", "code": code}
        if reason is not None:
            msg["reason"] = reason
        self.queue_send.send(msg)

    async def run_reader(self) -> None:
        buffer = WebsocketBuffer(self.config.ws_max_size)
        try:
            while not self.close_sent and not self.client_gone:
                try:
                    data = await self.stream.receive_some()
                except OSError:
                    self._enqueue_disconnect(1006)
                    self.client_gone = True
                    return
                if not data:
                    code = 1005 if self.handshake_complete else 1006
                    self._enqueue_disconnect(code)
                    self.client_gone = True
                    return
                try:
                    self.conn.receive_data(data)
                except RemoteProtocolError as err:
                    try:
                        await self._write(self.conn.send(err.event_hint))  # type: ignore[arg-type]
                    except ClientDisconnected:
                        pass
                    self.client_gone = True
                    return
                for event in self.conn.events():
                    if self.close_sent:
                        return
                    if isinstance(event, (events.TextMessage, events.BytesMessage)):
                        try:
                            buffer.extend(event)
                        except FrameTooLargeError:
                            reason = f"Message exceeds the maximum size ({self.config.ws_max_size} bytes)"
                            self._enqueue_disconnect(1009, reason)
                            try:
                                await self._write(self.conn.send(events.CloseConnection(code=1009, reason=reason)))
                            except ClientDisconnected:
                                pass
                            self.close_sent = True
                            return
                        if event.message_finished:
                            self.queue_send.send(buffer.to_message())
                            buffer.clear()
                    elif isinstance(event, events.CloseConnection):
                        self._enqueue_disconnect(event.code, event.reason)
                        if self.conn.state == ConnectionState.REMOTE_CLOSING:
                            try:
                                await self._write(self.conn.send(event.response()))
                            except ClientDisconnected:
                                pass
                        self.close_sent = True
                        return
                    elif isinstance(event, events.Ping):
                        try:
                            await self._write(self.conn.send(event.response()))
                        except ClientDisconnected:
                            return
                    elif isinstance(event, events.Pong):
                        self._handle_pong(bytes(event.payload))
        finally:
            self.done.set()

    def _handle_pong(self, payload: bytes) -> None:
        if self.pending_ping_payload is None or payload != self.pending_ping_payload:
            return
        self.last_ping_rtt = time.monotonic() - self.ping_sent_at
        self.pending_ping_payload = None

    async def run_keepalive(self) -> None:
        if not self.ping_interval or self.ping_interval <= 0:
            return
        try:
            while not self.close_sent and not self.client_gone:
                delay = max(0.0, self.ping_interval - self.last_ping_rtt)
                # Race the sleep against the reader's "done" signal so the
                # task exits immediately when the connection terminates.
                _, ok = await tonio.colored.time.timeout(self.done.wait(), delay)
                if ok or self.close_sent or self.client_gone:
                    return
                if not self.handshake_complete:
                    continue
                self.pending_ping_payload = struct.pack("!I", random.getrandbits(32))
                self.ping_sent_at = time.monotonic()
                try:
                    await self._write(self.conn.send(events.Ping(payload=self.pending_ping_payload)))
                except ClientDisconnected:
                    return
                if self.ping_timeout is None:
                    continue
                # Wait up to ping_timeout for a pong (or for the connection to
                # terminate, whichever comes first).
                await tonio.colored.time.timeout(self.done.wait(), self.ping_timeout)
                if self.pending_ping_payload is not None and not self.close_sent and not self.client_gone:
                    if logger.isEnabledFor(TRACE_LOG_LEVEL):
                        logger.log(TRACE_LOG_LEVEL, "WebSocket keepalive ping timeout")
                    reason = "keepalive ping timeout"
                    try:
                        await self._write(self.conn.send(events.CloseConnection(code=1011, reason=reason)))
                    except ClientDisconnected:
                        pass
                    self.close_sent = True
                    return
        except Exception:  # pragma: no cover
            logger.exception("keepalive task error")

    async def run_asgi(self, app: ASGI3Application) -> None:
        try:
            result = await app(self.scope, self.receive, self.send)  # type: ignore[func-returns-value]
        except ClientDisconnected:
            pass
        except BaseException:
            logger.exception("Exception in ASGI application\n")
            await self._send_500_response()
        else:
            if not self.handshake_complete:
                logger.error("ASGI callable returned without completing handshake.")
                await self._send_500_response()
            elif result is not None:
                logger.error("ASGI callable should return None, but returned '%s'.", result)
        # Force shutdown of remaining tasks
        self.close_sent = True
        self.client_gone = True

    async def _send_500_response(self) -> None:
        if self.response_started or self.handshake_complete:
            return
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"text/plain; charset=utf-8"),
            (b"connection", b"close"),
            (b"content-length", b"21"),
        ]
        try:
            out = self.conn.send(events.RejectConnection(status_code=500, headers=headers, has_body=True))
            out += self.conn.send(events.RejectData(data=b"Internal Server Error"))
            await self._write(out)
        except ClientDisconnected, LocalProtocolError:
            pass

    async def send(self, message: ASGISendEvent) -> None:
        if not self.handshake_complete and not self.response_started:
            mtype = message["type"]
            if mtype == "websocket.accept":
                logger.info(
                    '%s - "WebSocket %s" [accepted]',
                    get_client_addr(self.scope),
                    get_path_with_query_string(self.scope),
                )
                subprotocol = message.get("subprotocol")
                extra_headers = self.default_headers + list(message.get("headers", []))
                extensions: list[Extension] = []
                if self.config.ws_per_message_deflate:
                    extensions.append(PerMessageDeflate())
                self.handshake_complete = True
                out = self.conn.send(
                    events.AcceptConnection(
                        subprotocol=subprotocol,
                        extensions=extensions,
                        extra_headers=extra_headers,
                    )
                )
                await self._write(out)

            elif mtype == "websocket.close":
                self._enqueue_disconnect(1006)
                logger.info(
                    '%s - "WebSocket %s" 403',
                    get_client_addr(self.scope),
                    get_path_with_query_string(self.scope),
                )
                self.handshake_complete = True
                self.close_sent = True
                out = self.conn.send(events.RejectConnection(status_code=403, headers=[]))
                try:
                    await self._write(out)
                except ClientDisconnected:
                    pass

            elif mtype == "websocket.http.response.start":
                if not (100 <= message["status"] < 600):
                    raise RuntimeError("Invalid HTTP status code '%d' in response." % message["status"])
                logger.info(
                    '%s - "WebSocket %s" %d',
                    get_client_addr(self.scope),
                    get_path_with_query_string(self.scope),
                    message["status"],
                )
                self.handshake_complete = True
                self.response_started = True
                out = self.conn.send(
                    events.RejectConnection(
                        status_code=message["status"],
                        headers=list(message["headers"]),
                        has_body=True,
                    )
                )
                await self._write(out)
            else:
                raise RuntimeError(
                    "Expected ASGI message 'websocket.accept', 'websocket.close' "
                    f"or 'websocket.http.response.start' but got '{message['type']}'."
                )

        elif self.response_started:
            if message["type"] == "websocket.http.response.body":
                body_finished = not message.get("more_body", False)
                try:
                    out = self.conn.send(events.RejectData(data=message["body"], body_finished=body_finished))
                    await self._write(out)
                except (ClientDisconnected, LocalProtocolError) as exc:
                    raise ClientDisconnected from exc
                if body_finished:
                    self._enqueue_disconnect(1006)
                    self.close_sent = True
            else:
                raise RuntimeError(f"Expected ASGI message 'websocket.http.response.body' but got '{message['type']}'.")

        elif not self.close_sent:
            mtype = message["type"]
            try:
                if mtype == "websocket.send":
                    bytes_data = message.get("bytes")
                    text_data = message.get("text")
                    data = text_data if bytes_data is None else bytes_data
                    out = self.conn.send(events.Message(data=data))  # type: ignore[arg-type]
                    await self._write(out)

                elif mtype == "websocket.close":
                    self.close_sent = True
                    code = message.get("code", 1000)
                    reason = message.get("reason", "") or ""
                    self._enqueue_disconnect(code, reason)
                    out = self.conn.send(events.CloseConnection(code=code, reason=reason))
                    try:
                        await self._write(out)
                    except ClientDisconnected:
                        pass
                else:
                    raise RuntimeError(
                        f"Expected ASGI message 'websocket.send' or 'websocket.close', but got '{message['type']}'."
                    )
            except LocalProtocolError as exc:
                raise ClientDisconnected from exc

        else:
            raise RuntimeError(f"Unexpected ASGI message '{message['type']}', after sending 'websocket.close'.")

    async def receive(self) -> WebSocketEvent:
        message = await self.queue_recv.receive()
        return message
