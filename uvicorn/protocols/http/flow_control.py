from uvicorn._types import ASGIReceiveCallable, ASGISendCallable, Scope

CLOSE_HEADER = (b"connection", b"close")

HIGH_WATER_LIMIT = 65536


async def service_unavailable(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 503,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", b"19"),
                (b"connection", b"close"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": b"Service Unavailable", "more_body": False})
