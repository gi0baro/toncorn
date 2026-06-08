"""In-memory stand-in for tonio.colored.net.SocketStream.

Lets tests drive a protocol handler (``await handle(stream, config, …)``)
without spinning up a real listener. Feed inbound bytes with :meth:`feed` and
:meth:`feed_eof`; inspect outbound bytes via :attr:`outgoing`.
"""

from __future__ import annotations

import collections
import socket as _stdlib_socket

import tonio.colored


class _FakeSocket:
    """Mimics enough of tonio's :class:`_Socket` for utils.get_local_addr/_remote_addr."""

    family = _stdlib_socket.AF_INET

    def __init__(
        self,
        peername: tuple[str, int] | None = ("127.0.0.1", 12345),
        sockname: tuple[str, int] | None = ("127.0.0.1", 8000),
    ) -> None:
        self._peername = peername
        self._sockname = sockname
        # Server._handle_connection accesses stream.socket._sock for
        # graceful-shutdown force-close bookkeeping. Tests don't exercise
        # that path (they call handle() directly), but make it safe anyway.
        self._sock = self

    def getpeername(self) -> tuple[str, int]:
        if self._peername is None:
            raise OSError("no peer")
        return self._peername

    def getsockname(self) -> tuple[str, int]:
        if self._sockname is None:
            raise OSError("no name")
        return self._sockname

    def close(self) -> None:
        pass


class FakeSocketStream:
    """In-process stream for driving HTTP/WS handlers in tests.

    Usage::

        stream = FakeSocketStream()
        stream.feed(b"GET / HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n")
        stream.feed_eof()
        await handle(stream, config, server_state, app_state)
        assert b"HTTP/1.1 200" in stream.outgoing
    """

    def __init__(
        self,
        *,
        peername: tuple[str, int] | None = ("127.0.0.1", 12345),
        sockname: tuple[str, int] | None = ("127.0.0.1", 8000),
    ) -> None:
        self.socket = _FakeSocket(peername=peername, sockname=sockname)
        self._incoming: collections.deque[bytes] = collections.deque()
        self._eof = False
        self._closed = False
        self._can_read = tonio.colored.Event()
        self.outgoing = bytearray()

    # ----- test-side controls -----

    def feed(self, data: bytes) -> None:
        """Queue inbound bytes the server should ``receive_some``."""
        if not data:
            return
        self._incoming.append(data)
        self._can_read.set()

    def feed_eof(self) -> None:
        """Signal the peer has closed the write side; next ``receive_some`` returns ``b""``."""
        self._eof = True
        self._can_read.set()

    def break_connection(self) -> None:
        """Simulate the peer hanging up abruptly; next stream op raises ``OSError``."""
        self._closed = True
        self._can_read.set()

    # ----- stream interface (tonio.colored.net.SocketStream shape) -----

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        while not self._incoming and not self._eof and not self._closed:
            await self._can_read.wait()
            self._can_read.clear()
        if self._closed:
            raise OSError("stream closed")
        if not self._incoming:
            return b""
        data = self._incoming.popleft()
        if max_bytes is not None and len(data) > max_bytes:
            self._incoming.appendleft(data[max_bytes:])
            return data[:max_bytes]
        return data

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        if self._closed:
            raise OSError("stream closed")
        self.outgoing.extend(bytes(data))

    def send_eof(self) -> None:
        pass

    def close(self) -> None:
        self._closed = True
        self._can_read.set()
