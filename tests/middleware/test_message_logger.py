import contextlib
import logging
from collections.abc import Iterator

import httpx
import pytest

from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.logging import TRACE_LOG_LEVEL
from uvicorn.middleware.message_logger import MessageLoggerMiddleware


@contextlib.contextmanager
def caplog_for_logger(caplog: pytest.LogCaptureFixture, logger_name: str) -> Iterator[pytest.LogCaptureFixture]:
    logger = logging.getLogger(logger_name)
    logger.propagate, old_propagate = False, logger.propagate
    logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)
        logger.propagate = old_propagate


@pytest.mark.anyio
async def test_message_logger(caplog: pytest.LogCaptureFixture) -> None:
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    with caplog_for_logger(caplog, "uvicorn.asgi"):
        caplog.set_level(TRACE_LOG_LEVEL, logger="uvicorn.asgi")
        caplog.set_level(TRACE_LOG_LEVEL)

        transport = httpx.ASGITransport(MessageLoggerMiddleware(app))  # type: ignore
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/")
        assert response.status_code == 200
        messages = [record.msg % record.args for record in caplog.records]
        assert sum(["ASGI [1] Started" in message for message in messages]) == 1
        assert sum(["ASGI [1] Send" in message for message in messages]) == 2
        assert sum(["ASGI [1] Receive" in message for message in messages]) == 1
        assert sum(["ASGI [1] Completed" in message for message in messages]) == 1
        assert sum(["ASGI [1] Raised exception" in message for message in messages]) == 0


@pytest.mark.anyio
async def test_message_logger_exc(caplog: pytest.LogCaptureFixture) -> None:
    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        raise RuntimeError()

    with caplog_for_logger(caplog, "uvicorn.asgi"):
        caplog.set_level(TRACE_LOG_LEVEL, logger="uvicorn.asgi")
        caplog.set_level(TRACE_LOG_LEVEL)
        transport = httpx.ASGITransport(MessageLoggerMiddleware(app))  # type: ignore
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            with pytest.raises(RuntimeError):
                await client.get("/")
        messages = [record.msg % record.args for record in caplog.records]
        assert sum(["ASGI [1] Started" in message for message in messages]) == 1
        assert sum(["ASGI [1] Send" in message for message in messages]) == 0
        assert sum(["ASGI [1] Receive" in message for message in messages]) == 0
        assert sum(["ASGI [1] Completed" in message for message in messages]) == 0
        assert sum(["ASGI [1] Raised exception" in message for message in messages]) == 1
