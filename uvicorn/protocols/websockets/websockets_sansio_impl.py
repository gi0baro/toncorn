from __future__ import annotations

import logging
import random
import struct
import time
from http import HTTPStatus
from typing import Any, Literal, cast
from urllib.parse import unquote

import tonio
import tonio.colored
import tonio.colored.sync
import tonio.colored.time
from tonio.colored.sync.channel import unbounded as _unbounded_channel
from websockets.exceptions import InvalidState
from websockets.extensions.permessage_deflate import ServerPerMessageDeflateFactory
from websockets.frames import Frame, Opcode
from websockets.http11 import Request
from websockets.server import ServerProtocol

from uvicorn._types import (
    ASGIReceiveEvent,
    ASGISendEvent,
    WebSocketScope,
)
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


async def handle(
    stream: Stream,
    config: Config,
    server_state: ServerState,
    app_state: dict[str, Any],
    *,
    request_bytes: bytes,
) -> None:
    app = config.loaded_app
    server_addr = get_local_addr(stream)
    client_addr = get_remote_addr(stream)
    scheme: Literal["ws", "wss"] = "wss" if is_ssl(stream) else "ws"

    extensions = []
    if config.ws_per_message_deflate:
        extensions = [
            ServerPerMessageDeflateFactory(
                server_max_window_bits=12,
                client_max_window_bits=12,
                compress_settings={"memLevel": 5},
            )
        ]
    conn = ServerProtocol(
        extensions=extensions,
        max_size=config.ws_max_size,
        logger=logger,
    )

    try:
        conn.receive_data(request_bytes)
    except Exception:
        return
    if conn.parser_exc is not None:  # pragma: no cover
        return

    request: Request | None = None
    for event in conn.events_received():
        if isinstance(event, Request):
            request = event
            break
    if request is None:
        return

    response = conn.accept(request)
    headers = [
        (key.encode("ascii"), value.encode("ascii", errors="surrogateescape"))
        for key, value in request.headers.raw_items()
    ]
    raw_path, _, query_string = request.path.partition("?")
    scope: WebSocketScope = {
        "type": "websocket",
        "asgi": {"version": config.asgi_version, "spec_version": "2.4"},
        "http_version": "1.1",
        "scheme": scheme,
        "server": server_addr,
        "client": client_addr,
        "root_path": config.root_path,
        "path": config.root_path + unquote(raw_path),
        "raw_path": config.root_path.encode("ascii") + raw_path.encode("ascii"),
        "query_string": query_string.encode("ascii"),
        "headers": headers,
        "subprotocols": request.headers.get_all("Sec-WebSocket-Protocol"),
        "state": app_state.copy(),
        "extensions": {"websocket.http.response": {}},
    }

    if response.status_code != 101:
        # accept() refused the upgrade outright
        conn.send_response(response)
        out = b"".join(conn.data_to_send())
        try:
            if out:
                await stream.send_all(out)
        except OSError:
            pass
        return

    state = _State(
        config=config,
        scope=scope,
        conn=conn,
        response=response,
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
        conn: ServerProtocol,
        response: Any,
        stream: Stream,
        server_state: ServerState,
    ) -> None:
        self.config = config
        self.scope = scope
        self.conn = conn
        self.response = response
        self.stream = stream
        self.default_headers = server_state.default_headers

        self.write_lock = tonio.colored.sync.Lock()
        sender, receiver = _unbounded_channel()
        self.queue_send = sender
        self.queue_recv = receiver

        self.handshake_complete = False
        self.close_sent = False
        self.client_gone = False
        self.done = tonio.colored.Event()
        self.initial_response: tuple[int, list[tuple[str, str]], bytes] | None = None

        self.bytes = bytearray()
        self.curr_msg_data_type: Literal["text", "bytes"] = "text"

        self.ping_interval = config.ws_ping_interval
        self.ping_timeout = config.ws_ping_timeout
        self.pending_ping_payload: bytes | None = None
        self.ping_sent_at: float = 0.0
        self.last_ping_rtt: float = 0.0

    async def _flush(self) -> None:
        out = b"".join(self.conn.data_to_send())
        if not out:
            return
        async with self.write_lock:
            try:
                await self.stream.send_all(out)
            except OSError as exc:
                self.client_gone = True
                raise ClientDisconnected() from exc

    def _enqueue_disconnect(self, code: int, reason: str | None = None) -> None:
        msg: dict[str, Any] = {"type": "websocket.disconnect", "code": code}
        if reason is not None:
            msg["reason"] = reason
        self.queue_send.send(msg)

    async def run_reader(self) -> None:
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
                self.conn.receive_data(data)
                if self.conn.parser_exc is not None:  # pragma: no cover
                    self._handle_parser_error()
                    return
                for event in self.conn.events_received():
                    if self.close_sent:
                        return
                    if isinstance(event, Frame):
                        if event.opcode == Opcode.CONT:
                            self.bytes.extend(event.data)
                            if event.fin:
                                self._dispatch_message()
                        elif event.opcode == Opcode.TEXT:
                            self.bytes = bytearray(event.data)
                            self.curr_msg_data_type = "text"
                            if event.fin:
                                self._dispatch_message()
                        elif event.opcode == Opcode.BINARY:
                            self.bytes = bytearray(event.data)
                            self.curr_msg_data_type = "bytes"
                            if event.fin:
                                self._dispatch_message()
                        elif event.opcode == Opcode.PING:
                            try:
                                await self._flush()
                            except ClientDisconnected:
                                return
                        elif event.opcode == Opcode.PONG:
                            self._handle_pong(bytes(event.data))
                        elif event.opcode == Opcode.CLOSE:
                            if self.conn.close_rcvd is not None:
                                self._enqueue_disconnect(
                                    self.conn.close_rcvd.code,
                                    self.conn.close_rcvd.reason,
                                )
                            try:
                                await self._flush()
                            except ClientDisconnected:
                                pass
                            self.close_sent = True
                            return
        finally:
            self.done.set()

    def _dispatch_message(self) -> None:
        if self.curr_msg_data_type == "text":
            try:
                self.queue_send.send({"type": "websocket.receive", "text": self.bytes.decode()})
            except UnicodeDecodeError:  # pragma: no cover
                logger.exception("Invalid UTF-8 sequence received from client.")
                self.conn.send_close(1007)
                self._handle_parser_error()
                return
        else:
            self.queue_send.send({"type": "websocket.receive", "bytes": bytes(self.bytes)})

    def _handle_parser_error(self) -> None:  # pragma: no cover
        if self.conn.close_sent is not None:
            self._enqueue_disconnect(self.conn.close_sent.code, self.conn.close_sent.reason)
        self.close_sent = True
        try:
            out = b"".join(self.conn.data_to_send())
            if out:
                # best-effort write outside the lock (parser-error path is fatal anyway)
                # but use the lock to stay consistent
                # we can't await here in a non-async context; this is a fallback
                pass
        except Exception:
            pass

    def _handle_pong(self, payload: bytes) -> None:
        if self.pending_ping_payload is None or payload != self.pending_ping_payload:
            return
        self.last_ping_rtt = time.monotonic() - self.ping_sent_at
        self.pending_ping_payload = None

    async def run_keepalive(self) -> None:
        if not self.ping_interval or self.ping_interval <= 0:
            return
        while not self.close_sent and not self.client_gone:
            delay = max(0.0, self.ping_interval - self.last_ping_rtt)
            # Race the sleep against the reader's "done" signal so this task
            # exits immediately when the connection terminates.
            _, ok = await tonio.colored.time.timeout(self.done.wait(), delay)
            if ok or self.close_sent or self.client_gone:
                return
            if not self.handshake_complete:
                continue
            self.pending_ping_payload = struct.pack("!I", random.getrandbits(32))
            self.ping_sent_at = time.monotonic()
            self.conn.send_ping(self.pending_ping_payload)
            try:
                await self._flush()
            except ClientDisconnected:
                return
            if self.ping_timeout is None:
                continue
            await tonio.colored.time.timeout(self.done.wait(), self.ping_timeout)
            if self.pending_ping_payload is not None and not self.close_sent and not self.client_gone:
                if logger.isEnabledFor(TRACE_LOG_LEVEL):
                    logger.log(TRACE_LOG_LEVEL, "WebSocket keepalive ping timeout")
                self.conn.fail(1011, "keepalive ping timeout")
                try:
                    await self._flush()
                except ClientDisconnected:
                    pass
                self.close_sent = True
                return

    async def run_asgi(self, app: Any) -> None:
        try:
            result = await app(self.scope, self.receive, self.send)
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
        self.close_sent = True
        self.client_gone = True

    async def _send_500_response(self) -> None:
        if self.initial_response or self.handshake_complete:
            return
        response = self.conn.reject(500, "Internal Server Error")
        self.conn.send_response(response)
        try:
            await self._flush()
        except ClientDisconnected:
            pass

    async def send(self, message: ASGISendEvent) -> None:
        if not self.handshake_complete and self.initial_response is None:
            mtype = message["type"]
            if mtype == "websocket.accept":
                logger.info(
                    '%s - "WebSocket %s" [accepted]',
                    get_client_addr(self.scope),
                    get_path_with_query_string(self.scope),
                )
                headers = [
                    (name.decode("latin-1").lower(), value.decode("latin-1"))
                    for name, value in (self.default_headers + list(message.get("headers", [])))
                ]
                accepted_subprotocol = message.get("subprotocol")
                if accepted_subprotocol:
                    headers.append(("Sec-WebSocket-Protocol", accepted_subprotocol))
                self.response.headers.update(headers)
                self.handshake_complete = True
                self.conn.send_response(self.response)
                await self._flush()

            elif mtype == "websocket.close":
                self._enqueue_disconnect(1006)
                logger.info(
                    '%s - "WebSocket %s" 403',
                    get_client_addr(self.scope),
                    get_path_with_query_string(self.scope),
                )
                response = self.conn.reject(HTTPStatus.FORBIDDEN, "")
                self.conn.send_response(response)
                self.close_sent = True
                self.handshake_complete = True
                try:
                    await self._flush()
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
                headers = [
                    (name.decode("latin-1"), value.decode("latin-1"))
                    for name, value in list(message.get("headers", []))
                ]
                self.initial_response = (message["status"], headers, b"")
            else:
                raise RuntimeError(
                    "Expected ASGI message 'websocket.accept', 'websocket.close' "
                    f"or 'websocket.http.response.start' but got '{message['type']}'."
                )

        elif self.initial_response is not None:
            if message["type"] == "websocket.http.response.body":
                body = self.initial_response[2] + message["body"]
                self.initial_response = self.initial_response[:2] + (body,)
                if not message.get("more_body", False):
                    response = self.conn.reject(self.initial_response[0], body.decode())
                    response.headers.update(self.initial_response[1])
                    self._enqueue_disconnect(1006)
                    self.conn.send_response(response)
                    self.close_sent = True
                    try:
                        await self._flush()
                    except ClientDisconnected:
                        pass
            else:  # pragma: no cover
                raise RuntimeError(f"Expected ASGI message 'websocket.http.response.body' but got '{message['type']}'.")

        elif not self.close_sent:
            mtype = message["type"]
            try:
                if mtype == "websocket.send":
                    bytes_data = message.get("bytes")
                    text_data = message.get("text")
                    if bytes_data is not None:
                        self.conn.send_binary(bytes_data)
                    elif text_data is not None:
                        self.conn.send_text(text_data.encode())
                    await self._flush()
                elif mtype == "websocket.close":
                    code = message.get("code", 1000)
                    reason = message.get("reason", "") or ""
                    self._enqueue_disconnect(code, reason)
                    self.conn.send_close(code, reason)
                    self.close_sent = True
                    try:
                        await self._flush()
                    except ClientDisconnected:
                        pass
                else:
                    raise RuntimeError(
                        f"Expected ASGI message 'websocket.send' or 'websocket.close', but got '{message['type']}'."
                    )
            except InvalidState as exc:
                raise ClientDisconnected() from exc

        else:
            raise RuntimeError(f"Unexpected ASGI message '{message['type']}', after sending 'websocket.close'.")

    async def receive(self) -> ASGIReceiveEvent:
        message = await self.queue_recv.receive()
        return cast(ASGIReceiveEvent, message)
