from __future__ import annotations

import socket
from typing import Any

import pytest

from uvicorn.protocols.utils import get_client_addr, get_local_addr, get_remote_addr


class FakeSocket:
    def __init__(
        self,
        family: socket.AddressFamily = socket.AF_INET,
        peername: tuple[str, int] | str | None = None,
        sockname: tuple[str, int] | str | None = None,
    ):
        self.family = family
        self._peername = peername
        self._sockname = sockname

    def getpeername(self) -> Any:
        if self._peername is None:
            raise OSError
        return self._peername

    def getsockname(self) -> Any:
        if self._sockname is None:
            raise OSError
        return self._sockname


class FakeStream:
    def __init__(self, sock: FakeSocket) -> None:
        self.socket = sock


def test_get_local_addr_ipv4():
    stream = FakeStream(FakeSocket(sockname=("127.0.0.1", 80)))
    assert get_local_addr(stream) == ("127.0.0.1", 80)


def test_get_local_addr_ipv6():
    stream = FakeStream(FakeSocket(family=socket.AF_INET6, sockname=("::1", 80)))
    assert get_local_addr(stream) == ("::1", 80)


def test_get_local_addr_unix():
    stream = FakeStream(FakeSocket(sockname="/tmp/test.sock"))
    assert get_local_addr(stream) == ("/tmp/test.sock", None)


def test_get_local_addr_returns_none_on_oserror():
    stream = FakeStream(FakeSocket())
    assert get_local_addr(stream) is None


def test_get_remote_addr_ipv4():
    stream = FakeStream(FakeSocket(peername=("123.45.6.7", 123)))
    assert get_remote_addr(stream) == ("123.45.6.7", 123)


def test_get_remote_addr_ipv6():
    stream = FakeStream(FakeSocket(family=socket.AF_INET6, peername=("::1", 80)))
    assert get_remote_addr(stream) == ("::1", 80)


def test_get_remote_addr_returns_none_on_oserror():
    stream = FakeStream(FakeSocket())
    assert get_remote_addr(stream) is None


@pytest.mark.parametrize(
    "scope, expected_client",
    [({"client": ("127.0.0.1", 36000)}, "127.0.0.1:36000"), ({"client": None}, "")],
    ids=["ip:port client", "None client"],
)
def test_get_client_addr(scope: Any, expected_client: str):
    assert get_client_addr(scope) == expected_client
