"""Logging/access-log tests against a live server.

Original suite tested asyncio.Protocol-level connection-made/lost messages
(no longer applicable to the pull-style handlers) and used pytest's `caplog`
fixture (which has been observed to trip the tonio runtime under some paths).
This rewrite keeps the still-applicable scenarios and uses a hand-rolled
logging.Handler instead of caplog.
"""

from __future__ import annotations

import contextlib
import logging
import socket
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.utils import run_server
from uvicorn import Config
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope

pytestmark = pytest.mark.tonio


async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
    if scope["type"] != "http":
        return
    await receive()
    await send({"type": "http.response.start", "status": 204, "headers": []})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextlib.contextmanager
def _attach(logger_name: str, level: int = logging.DEBUG) -> Iterator[_Capture]:
    """Attach a hand-rolled handler to the given logger.

    The handler must be attached *after* ``Config.load()`` has run, because
    ``configure_logging`` replaces the handler list. Tests using this helper
    construct a Config first, load it, then enter this context manager.
    """
    logger = logging.getLogger(logger_name)
    handler = _Capture()
    handler.setLevel(level)
    prev_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(level)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------


@pytest.mark.parametrize("use_colors", [True, False, None])
async def test_access_logging(use_colors: bool | None, logging_config: dict[str, Any]):
    port = _free_port()
    config = Config(app=app, use_colors=use_colors, log_config=logging_config, port=port, lifespan="off")
    config.load()
    with _attach("uvicorn.access", logging.INFO) as cap:
        async with run_server(config):
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}")
        assert response.status_code == 204
    messages = [r.getMessage() for r in cap.records]
    assert any('"GET / HTTP/1.1" 204' in m for m in messages), messages


async def test_unknown_status_code():
    async def app_599(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        if scope["type"] != "http":
            return
        await receive()
        await send({"type": "http.response.start", "status": 599, "headers": [(b"content-length", b"0")]})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    port = _free_port()
    config = Config(app=app_599, port=port, lifespan="off")
    config.load()
    with _attach("uvicorn.access", logging.INFO) as cap:
        async with run_server(config):
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}")
        assert response.status_code == 599
    messages = [r.getMessage() for r in cap.records]
    assert any('"GET / HTTP/1.1" 599' in m for m in messages), messages


async def test_server_lifecycle_messages():
    """The Server logs the start/stop banner, the running URL, and per-request access."""
    port = _free_port()
    config = Config(app=app, port=port, lifespan="off")
    config.load()
    with _attach("uvicorn.error", logging.INFO) as err_cap, _attach("uvicorn.access", logging.INFO) as acc_cap:
        async with run_server(config):
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}")
        assert response.status_code == 204

    err_messages = [r.getMessage() for r in err_cap.records]
    assert any("Started server process" in m for m in err_messages), err_messages
    assert any("Uvicorn running on http://127.0.0.1" in m for m in err_messages), err_messages
    assert any("Shutting down" in m for m in err_messages), err_messages

    acc_messages = [r.getMessage() for r in acc_cap.records]
    assert any('"GET / HTTP/1.1" 204' in m for m in acc_messages), acc_messages


async def test_running_log_using_uds():
    # AF_UNIX paths must stay under ~100 bytes. Bind under /tmp so the resulting
    # path stays short on macOS where pytest's tmp_path lives under a long
    # /var/folders/... prefix. (Avoid a yield-style fixture: those trip the tonio
    # runtime — see notes in tests/test_ssl.py).
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="uvicorn-uds-") as tmpd:
        sock_path = str(Path(tmpd) / "my.sock")
        config = Config(app=app, uds=sock_path, lifespan="off")
        config.load()
        with _attach("uvicorn.error", logging.INFO) as cap:
            async with run_server(config):
                pass
        messages = [r.getMessage() for r in cap.records]
        assert any(f"Uvicorn running on unix socket {sock_path}" in m for m in messages), messages
