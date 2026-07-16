import importlib
import inspect
import os
import socket
import sys
from pathlib import Path

import httpx
import pytest

import uvicorn.server
from tests.utils import run_server
from uvicorn import Server
from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import STARTUP_FAILURE, Config
from uvicorn.main import run

pytestmark = pytest.mark.tonio


async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
    assert scope["type"] == "http"
    await send({"type": "http.response.start", "status": 204, "headers": []})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


def _has_ipv6(host: str):
    sock = None
    has_ipv6 = False
    if socket.has_ipv6:
        try:
            sock = socket.socket(socket.AF_INET6)
            sock.bind((host, 0))
            has_ipv6 = True
        except Exception:  # pragma: no cover
            pass
    if sock:
        sock.close()
    return has_ipv6


@pytest.mark.parametrize(
    "host, url",
    [
        pytest.param(None, "http://127.0.0.1", id="default"),
        pytest.param("localhost", "http://127.0.0.1", id="hostname"),
        pytest.param(
            "::1",
            "http://[::1]",
            id="ipv6",
            marks=pytest.mark.skipif(not _has_ipv6("::1"), reason="IPV6 not enabled"),
        ),
    ],
)
async def test_run(host, url: str, unused_tcp_port: int):
    config = Config(app=app, host=host, limit_max_requests=1, port=unused_tcp_port)
    async with run_server(config):
        with httpx.Client() as client:
            response = client.get(f"{url}:{unused_tcp_port}")
    assert response.status_code == 204


def test_run_imports_app_before_starting_event_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`uvicorn.run()` imports the app before `Server.run` opens the event loop.

    Regression for https://github.com/encode/uvicorn/issues/941: an app whose
    module body calls `asyncio.run(...)` crashes with "loop already running"
    if Uvicorn imports it inside the server's event loop. The parent must
    import the app synchronously, before `Server.run` enters `asyncio.run`.
    """
    module = tmp_path / "eager_async_app.py"
    module.write_text(
        "import asyncio\n"
        "async def _build():\n"
        "    async def app(scope, receive, send):\n"
        "        pass\n"
        "    return app\n"
        "app = asyncio.run(_build())\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    imported_before_server_run: list[bool] = []

    def tracking_run(self: Server, sockets: object = None) -> None:
        imported_before_server_run.append("eager_async_app" in sys.modules)
        self.started = True

    monkeypatch.setattr(Server, "run", tracking_run)

    # The import side effect (`eager_async_app` lands in `sys.modules`) must
    # happen before `Server.run`, which is where the event loop opens.
    run("eager_async_app:app")

    assert imported_before_server_run == [True]


def test_run_startup_failure(tmp_path: Path) -> None:
    """Run in a subprocess: `run()` calls tonio.run which can't re-init the
    runtime owned by the pytest plugin."""
    import subprocess

    script = tmp_path / "boot.py"
    script.write_text(
        "from uvicorn.main import run\n"
        "async def app(scope, receive, send):\n"
        "    msg = await receive()\n"
        "    if msg['type'] == 'lifespan.startup':\n"
        "        raise RuntimeError('Startup failed')\n"
        "run(app, lifespan='on', port=0)\n"
    )
    env = dict(os.environ)
    env.setdefault("PYTHON_GIL", "0")
    result = subprocess.run(
        [sys.executable, str(script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 3, (result.returncode, result.stdout, result.stderr)


def test_run_match_config_params() -> None:
    config_params = {
        key: repr(value)
        for key, value in inspect.signature(Config.__init__).parameters.items()
        if key not in ("self", "timeout_notify", "callback_notify")
    }
    run_params = {
        key: repr(value) for key, value in inspect.signature(run).parameters.items() if key not in ("app_dir",)
    }
    assert config_params == run_params


async def test_exit_on_create_server_with_invalid_host() -> None:
    with pytest.raises(SystemExit) as exc_info:
        config = Config(app=app, host="illegal_host")
        server = Server(config=config)
        await server.serve()
    assert exc_info.value.code == STARTUP_FAILURE


def test_deprecated_server_state_from_main() -> None:
    with pytest.deprecated_call(
        match="uvicorn.main.ServerState is deprecated, use uvicorn.server.ServerState instead."
    ):
        main = importlib.import_module("uvicorn.main")
        server_state_cls = getattr(main, "ServerState")
    assert server_state_cls is uvicorn.server.ServerState
