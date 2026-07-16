from __future__ import annotations

import contextlib
import functools
import logging
import os
import random
import signal
import socket
import ssl as _ssl
import sys
import threading
import time
from collections.abc import Sequence
from email.utils import formatdate

import tonio
import tonio.colored
import tonio.colored.net
import tonio.colored.signals
import tonio.colored.time
from tonio._colored._net._socket import from_stdlib_socket
from tonio.colored.net import SocketListener, SocketStream
from tonio.colored.net.tls import TLSStream

from uvicorn._ansi import style
from uvicorn.config import STARTUP_FAILURE, Config

HANDLED_SIGNALS: tuple[int, ...] = (
    signal.SIGINT,
    signal.SIGTERM,
)

logger = logging.getLogger("uvicorn.error")


class Connection:
    """Bookkeeping for one in-flight connection."""

    __slots__ = ("sock", "idle_since")

    def __init__(self, sock: socket.socket) -> None:
        #: The underlying stdlib socket.
        self.sock = sock
        #: Monotonic timestamp since the handler has been parked waiting for
        #: the next request head; None while a request is in flight. Written
        #: by the HTTP handlers, read by the server's keep-alive watchdog.
        self.idle_since: float | None = None


class ServerState:
    """Shared server state available to all connection handlers."""

    def __init__(self) -> None:
        self.total_requests = 0
        # id(stream) -> Connection for every in-flight connection.
        # dict add/del is atomic under free-threaded CPython, so no lock is
        # needed. Used by handlers for `limit_concurrency` and idle tracking,
        # and by Server's keep-alive watchdog and graceful-shutdown drain.
        self.connections: dict[int, Connection] = {}
        self.default_headers: list[tuple[bytes, bytes]] = []


class Server:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.server_state = ServerState()

        self.started = False
        self.should_exit = False
        self.force_exit = False
        self.last_notified = 0.0

        self._captured_signals: list[int] = []
        self.listeners: list[SocketListener] = []
        self._ssl_context: _ssl.SSLContext | None = None
        self._exit_ev = tonio.colored.Event()

        # Set when server_state.connections becomes empty; cleared on add.
        self._all_handlers_done = tonio.colored.Event()
        self._all_handlers_done.set()

    @functools.cached_property
    def limit_max_requests(self) -> int | None:
        if self.config.limit_max_requests is None:
            return None
        return self.config.limit_max_requests + random.randint(0, self.config.limit_max_requests_jitter)

    def run(self, sockets: list[socket.socket] | None = None) -> None:
        threads = self.config.threads if self.config.threads and self.config.threads > 0 else None
        # Tonio's runtime installs signal handlers via signal.set_wakeup_fd, which
        # requires the main thread. When Server.run is invoked from a worker
        # thread (e.g. test harnesses), skip signal registration.
        signals = list(HANDLED_SIGNALS) if threading.current_thread() is threading.main_thread() else []
        try:
            return tonio.run(
                self.serve(sockets=sockets),
                threads=threads,
                signals=signals,
            )
        finally:
            # Re-raise captured signals after the runtime has restored the default
            # handlers, so a wrapping shell sees e.g. KeyboardInterrupt on Ctrl-C.
            for captured_signal in reversed(self._captured_signals):
                signal.raise_signal(captured_signal)

    async def serve(self, sockets: list[socket.socket] | None = None) -> None:
        await self._serve(sockets)

    async def _serve(self, sockets: list[socket.socket] | None = None) -> None:
        process_id = os.getpid()

        config = self.config
        if not config.loaded:
            config.load()

        self.lifespan = config.lifespan_class(config)

        # toncorn banner: identify the fork + the upstream uvicorn version it tracks.
        import toncorn as _toncorn

        banner = "toncorn %s (uvicorn %s)" % (_toncorn.__version__, _toncorn.uvicorn_version)
        color_banner = style(banner, bold=True)
        logger.info(banner, extra={"color_message": color_banner})

        message = "Started server process [%d]"
        color_message = "Started server process [" + style("%d", fg="cyan") + "]"
        logger.info(message, process_id, extra={"color_message": color_message})

        await self.startup(sockets=sockets)

        async def main_work() -> None:
            if not self.should_exit:
                try:
                    await tonio.colored.select(self._run_accept_loops(), self.main_loop())
                finally:
                    for listener in self.listeners:
                        with contextlib.suppress(Exception):
                            listener.close()

            if self.started:
                await self.shutdown()
                finished = "Finished server process [%d]"
                color_finished = "Finished server process [" + style("%d", fg="cyan") + "]"
                logger.info(finished, process_id, extra={"color_message": color_finished})

        # The signal watcher runs concurrently with main_work so a second SIGINT
        # received during graceful shutdown can flip `force_exit` and skip the
        # lifespan shutdown.
        await tonio.colored.select(main_work(), self._watch_signals())

    async def _watch_signals(self) -> None:
        """Consume HANDLED_SIGNALS via tonio and update should_exit/force_exit.

        If the runtime wasn't initialized with these signals (e.g. when serve()
        is driven by the tonio pytest plugin), we fall back to an idle wait so
        the surrounding `select` keeps waiting on main_work to finish.
        """
        try:
            ctx = tonio.colored.signals.signal_receiver(*HANDLED_SIGNALS)
        except Exception:
            await _idle_forever()
            return

        try:
            with ctx as recv:
                async for sig in recv:
                    self._captured_signals.append(sig)
                    if self.should_exit and sig == signal.SIGINT:
                        self.force_exit = True  # pragma: full coverage
                    else:
                        self.should_exit = True
                        self._exit_ev.set()
        except Exception:
            await _idle_forever()

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await self.lifespan.startup()
        if self.lifespan.should_exit:
            sys.exit(STARTUP_FAILURE)

        config = self.config
        self._ssl_context = config.ssl

        listener_sockets: Sequence[socket.SocketType]
        if sockets is not None:
            for sock in sockets:
                sock.setblocking(False)
                self.listeners.append(SocketListener(from_stdlib_socket(sock)))
            listener_sockets = sockets

        elif config.uds is not None:
            uds_perms = 0o666
            if os.path.exists(config.uds):
                uds_perms = os.stat(config.uds).st_mode  # pragma: full coverage
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.setblocking(False)
                sock.bind(config.uds)
                sock.listen(config.backlog)
                os.chmod(config.uds, uds_perms)
            except BaseException:
                sock.close()
                raise
            self.listeners.append(SocketListener(from_stdlib_socket(sock)))
            listener_sockets = [sock]

        else:
            try:
                self.listeners = await tonio.colored.net.open_tcp_listeners(
                    config.port,
                    host=config.host,
                    backlog=config.backlog,
                )
            except OSError as exc:
                logger.error(exc)
                await self.lifespan.shutdown()
                sys.exit(STARTUP_FAILURE)
            listener_sockets = [listener.socket._sock for listener in self.listeners]

        if sockets is None:
            self._log_started_message(listener_sockets)

        # Pre-populate default headers so the first request doesn't race the
        # main loop's first on_tick.
        date_header: list[tuple[bytes, bytes]] = []
        if self.config.date_header:
            date_header = [(b"date", formatdate(time.time(), usegmt=True).encode())]
        self.server_state.default_headers = date_header + list(self.config.encoded_headers)

        self.started = True

    def _log_started_message(self, listeners: Sequence[socket.SocketType]) -> None:
        config = self.config

        if config.uds is not None:
            logger.info("Uvicorn running on unix socket %s (Press CTRL+C to quit)", config.uds)

        else:
            addr_format = "%s://%s:%d"
            host = "0.0.0.0" if config.host is None else config.host
            if ":" in host:
                addr_format = "%s://[%s]:%d"

            port = config.port
            if port == 0:
                port = listeners[0].getsockname()[1]

            protocol_name = "https" if self._ssl_context is not None else "http"
            message = f"Uvicorn running on {addr_format} (Press CTRL+C to quit)"
            color_message = "Uvicorn running on " + style(addr_format, bold=True) + " (Press CTRL+C to quit)"
            logger.info(
                message,
                protocol_name,
                host,
                port,
                extra={"color_message": color_message},
            )

    async def _run_accept_loops(self) -> None:
        async with tonio.colored.scope() as sc:
            for listener in self.listeners:
                sc.spawn(self._accept_loop(listener))
            await self._exit_ev.wait()
            sc.cancel()

    async def _accept_loop(self, listener: SocketListener) -> None:
        # while True:
        while not self.should_exit:
            try:
                stream = await listener.accept()
            except OSError:
                return
            tonio.colored.spawn.without_tracking(self._handle_connection(stream))

    async def _handle_connection(self, stream: SocketStream) -> None:
        handler = self.config.http_protocol_class

        handler_id = id(stream)
        self.server_state.connections[handler_id] = Connection(stream.socket._sock)
        self._all_handlers_done.clear()

        wrapped: SocketStream | TLSStream = stream
        try:
            if self._ssl_context is not None:
                wrapped = TLSStream(stream, self._ssl_context, server_side=True, https_compatible=True)
                try:
                    await wrapped.handshake()
                except Exception:
                    logger.exception("TLS handshake failed")
                    return
            await handler(wrapped, self.config, self.server_state, self.lifespan.state)
        except Exception:
            logger.exception("Error while handling connection")
        finally:
            with contextlib.suppress(Exception):
                if isinstance(wrapped, TLSStream):
                    await wrapped.close()
                else:
                    wrapped.close()
            self.server_state.connections.pop(handler_id, None)
            if not self.server_state.connections:
                self._all_handlers_done.set()

    async def main_loop(self) -> None:
        counter = 0
        # if await self.on_tick(counter):
        #    return
        ticker = tonio.colored.time.interval(0.1)
        while True:
            await ticker.tick()
            counter = (counter + 1) % 864000
            if await self.on_tick(counter):
                return

    async def on_tick(self, counter: int) -> bool:
        # Keep-alive watchdog: reap connections that have been idle (parked
        # waiting for the next request head) longer than timeout_keep_alive.
        # shutdown(), unlike close(), delivers a READ_CLOSED edge to the
        # poller, so the parked reader wakes up, sees EOF, and unwinds the
        # connection through its normal cleanup path — close() would silently
        # drop the fd from the poller's interest set and leak the parked task.
        timeout_keep_alive = self.config.timeout_keep_alive
        if timeout_keep_alive and self.server_state.connections:
            now = time.monotonic()
            for connection in list(self.server_state.connections.values()):
                idle_since = connection.idle_since
                if idle_since is not None and now - idle_since > timeout_keep_alive:
                    with contextlib.suppress(OSError):
                        connection.sock.shutdown(socket.SHUT_RDWR)

        if counter % 10 == 0:
            current_time = time.time()
            current_date = formatdate(current_time, usegmt=True).encode()

            if self.config.date_header:
                date_header = [(b"date", current_date)]
            else:
                date_header = []

            self.server_state.default_headers = date_header + self.config.encoded_headers

            if self.config.callback_notify is not None:
                if current_time - self.last_notified > self.config.timeout_notify:  # pragma: full coverage
                    self.last_notified = current_time
                    await self.config.callback_notify()

        if self.should_exit:
            return True

        max_requests = self.limit_max_requests
        if max_requests is not None and self.server_state.total_requests >= max_requests:
            logger.info("Maximum request limit of %d exceeded. Terminating process.", max_requests)
            return True

        return False

    async def shutdown(self) -> None:
        logger.info("Shutting down")
        if not self.force_exit:
            await self._drain_connections()
        if not self.force_exit:
            await self.lifespan.shutdown()

    async def _drain_connections(self) -> None:
        if not self.server_state.connections:
            return

        timeout = self.config.timeout_graceful_shutdown
        in_flight = len(self.server_state.connections)

        if timeout is None:
            logger.info("Waiting for %d connection(s) to close.", in_flight)
            await self._all_handlers_done.wait()
            return

        logger.info(
            "Waiting up to %ss for %d connection(s) to close.",
            timeout,
            in_flight,
        )
        _, ok = await tonio.colored.time.timeout(self._all_handlers_done.wait(), timeout)
        if ok:
            return

        remaining = len(self.server_state.connections)
        if remaining == 0:
            return
        logger.warning(
            "Graceful shutdown timeout exceeded; force-closing %d connection(s).",
            remaining,
        )
        # shutdown() on the underlying stdlib socket makes any blocked
        # stream.receive_some / send_all return immediately (EOF or OSError),
        # so the handler's existing error path unwinds the connection and
        # closes the socket itself. close() would instead silently remove the
        # fd from the poller's interest set, leaving parked readers waiting
        # forever.
        for connection in list(self.server_state.connections.values()):
            with contextlib.suppress(Exception):
                connection.sock.shutdown(socket.SHUT_RDWR)
        # Give handlers a moment to observe the closed socket and run their
        # finally blocks. Anything still pending after this is abandoned when
        # tonio.run returns.
        await tonio.colored.time.timeout(self._all_handlers_done.wait(), 1.0)


async def _idle_forever() -> None:
    while True:
        await tonio.colored.time.sleep(3600)
