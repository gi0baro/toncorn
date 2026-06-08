import pytest

from uvicorn.config import Config
from uvicorn.lifespan.off import LifespanOff
from uvicorn.lifespan.on import LifespanOn

pytestmark = pytest.mark.tonio


async def test_lifespan_on():
    startup_complete = False
    shutdown_complete = False

    async def app(scope, receive, send):
        nonlocal startup_complete, shutdown_complete
        message = await receive()
        assert message["type"] == "lifespan.startup"
        startup_complete = True
        await send({"type": "lifespan.startup.complete"})
        message = await receive()
        assert message["type"] == "lifespan.shutdown"
        shutdown_complete = True
        await send({"type": "lifespan.shutdown.complete"})

    config = Config(app=app, lifespan="on")
    lifespan = LifespanOn(config)

    assert not startup_complete
    assert not shutdown_complete
    await lifespan.startup()
    assert startup_complete
    assert not shutdown_complete
    await lifespan.shutdown()
    assert startup_complete
    assert shutdown_complete


async def test_lifespan_off():
    async def app(scope, receive, send):
        pass  # pragma: no cover

    config = Config(app=app, lifespan="off")
    lifespan = LifespanOff(config)

    await lifespan.startup()
    await lifespan.shutdown()


async def test_lifespan_auto():
    startup_complete = False
    shutdown_complete = False

    async def app(scope, receive, send):
        nonlocal startup_complete, shutdown_complete
        message = await receive()
        assert message["type"] == "lifespan.startup"
        startup_complete = True
        await send({"type": "lifespan.startup.complete"})
        message = await receive()
        assert message["type"] == "lifespan.shutdown"
        shutdown_complete = True
        await send({"type": "lifespan.shutdown.complete"})

    config = Config(app=app, lifespan="auto")
    lifespan = LifespanOn(config)

    assert not startup_complete
    assert not shutdown_complete
    await lifespan.startup()
    assert startup_complete
    assert not shutdown_complete
    await lifespan.shutdown()
    assert startup_complete
    assert shutdown_complete


async def test_lifespan_auto_with_error():
    async def app(scope, receive, send):
        assert scope["type"] == "http"

    config = Config(app=app, lifespan="auto")
    lifespan = LifespanOn(config)

    await lifespan.startup()
    assert lifespan.error_occurred
    assert not lifespan.should_exit
    await lifespan.shutdown()


async def test_lifespan_on_with_error():
    async def app(scope, receive, send):
        if scope["type"] != "http":
            raise RuntimeError()

    config = Config(app=app, lifespan="on")
    lifespan = LifespanOn(config)

    await lifespan.startup()
    assert lifespan.error_occurred
    assert lifespan.should_exit
    await lifespan.shutdown()


@pytest.mark.parametrize("mode", ("auto", "on"))
@pytest.mark.parametrize("raise_exception", (True, False))
async def test_lifespan_with_failed_startup(mode, raise_exception):
    import logging

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    async def app(scope, receive, send):
        message = await receive()
        assert message["type"] == "lifespan.startup"
        await send({"type": "lifespan.startup.failed", "message": "the lifespan event failed"})
        if raise_exception:
            raise RuntimeError()

    config = Config(app=app, lifespan=mode)
    lifespan = LifespanOn(config)

    err_logger = logging.getLogger("uvicorn.error")
    h = _Capture(level=logging.ERROR)
    err_logger.addHandler(h)
    try:
        await lifespan.startup()
        assert lifespan.startup_failed
        assert lifespan.error_occurred is raise_exception
        assert lifespan.should_exit
        await lifespan.shutdown()
    finally:
        err_logger.removeHandler(h)

    error_messages = [r.getMessage() for r in captured if r.levelname == "ERROR"]
    # Two messages, order may differ between asyncio and tonio (two tasks racing).
    assert any("the lifespan event failed" in m for m in error_messages), error_messages
    assert any("Application startup failed. Exiting." in m for m in error_messages), error_messages


async def test_lifespan_scope_asgi3app():
    async def asgi3app(scope, receive, send):
        assert scope == {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": {},
        }

    config = Config(app=asgi3app, lifespan="on")
    lifespan = LifespanOn(config)

    await lifespan.startup()
    assert not lifespan.startup_failed
    assert not lifespan.error_occurred
    assert not lifespan.should_exit
    await lifespan.shutdown()


async def test_lifespan_scope_asgi2app():
    def asgi2app(scope):
        assert scope == {
            "type": "lifespan",
            "asgi": {"version": "2.0", "spec_version": "2.0"},
            "state": {},
        }

        async def asgi(receive, send):
            pass

        return asgi

    config = Config(app=asgi2app, lifespan="on")
    lifespan = LifespanOn(config)

    await lifespan.startup()
    await lifespan.shutdown()


@pytest.mark.parametrize("mode", ("auto", "on"))
@pytest.mark.parametrize("raise_exception", (True, False))
async def test_lifespan_with_failed_shutdown(mode, raise_exception):
    import logging

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    async def app(scope, receive, send):
        message = await receive()
        assert message["type"] == "lifespan.startup"
        await send({"type": "lifespan.startup.complete"})
        message = await receive()
        assert message["type"] == "lifespan.shutdown"
        await send({"type": "lifespan.shutdown.failed", "message": "the lifespan event failed"})

        if raise_exception:
            raise RuntimeError()

    config = Config(app=app, lifespan=mode)
    lifespan = LifespanOn(config)

    err_logger = logging.getLogger("uvicorn.error")
    h = _Capture(level=logging.ERROR)
    err_logger.addHandler(h)
    try:
        await lifespan.startup()
        assert not lifespan.startup_failed
        await lifespan.shutdown()
        assert lifespan.shutdown_failed
        assert lifespan.error_occurred is raise_exception
        assert lifespan.should_exit
    finally:
        err_logger.removeHandler(h)

    error_messages = [r.getMessage() for r in captured if r.levelname == "ERROR"]
    assert any("the lifespan event failed" in m for m in error_messages), error_messages
    assert any("Application shutdown failed. Exiting." in m for m in error_messages), error_messages


async def test_lifespan_state():
    async def app(scope, receive, send):
        message = await receive()
        assert message["type"] == "lifespan.startup"
        await send({"type": "lifespan.startup.complete"})
        scope["state"]["foo"] = 123
        message = await receive()
        assert message["type"] == "lifespan.shutdown"
        await send({"type": "lifespan.shutdown.complete"})

    config = Config(app=app, lifespan="on")
    lifespan = LifespanOn(config)

    await lifespan.startup()
    assert lifespan.state == {"foo": 123}
    await lifespan.shutdown()
