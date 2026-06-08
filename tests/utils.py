from __future__ import annotations

import os
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from socket import socket

import tonio.colored
import tonio.colored.time

from uvicorn import Config, Server


@asynccontextmanager
async def run_server(config: Config, sockets: list[socket] | None = None) -> AsyncIterator[Server]:
    """Spawn `server.serve(...)` inside the active tonio runtime.

    Used by tests that need a live HTTP server. Requires the surrounding test
    to be marked with `@pytest.mark.tonio` so a runtime is in place.
    """
    server = Server(config=config)
    done = tonio.colored.Event()

    async def runner() -> None:
        try:
            await server.serve(sockets=sockets)
        finally:
            done.set()

    tonio.colored.spawn.without_tracking(runner())
    while not server.started and not done.is_set():
        await tonio.colored.time.sleep(0.05)
    try:
        yield server
    finally:
        server.should_exit = True
        await done.wait()


@contextmanager
def assert_signal(sig: signal.Signals):
    """Check that a signal was received and handled in a block"""
    seen: set[int] = set()
    prev_handler = signal.signal(sig, lambda num, frame: seen.add(num))
    try:
        yield
        assert sig in seen, f"process signal {signal.Signals(sig)!r} was not received or handled"
    finally:
        signal.signal(sig, prev_handler)


@contextmanager
def as_cwd(path: Path):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)
