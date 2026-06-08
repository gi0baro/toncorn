from __future__ import annotations

import inspect
import json
import logging
import logging.config
import os
import ssl
import sys
from collections.abc import Awaitable, Callable
from configparser import RawConfigParser
from typing import IO, Any, Literal

from uvicorn._types import ASGIApplication
from uvicorn.importer import ImportFromStringError, import_from_string
from uvicorn.logging import TRACE_LOG_LEVEL
from uvicorn.middleware.asgi2 import ASGI2Middleware
from uvicorn.middleware.message_logger import MessageLoggerMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

HTTPProtocolType = Literal["auto", "h11", "httptools"]
WSProtocolType = Literal["auto", "none", "websockets", "websockets-sansio", "wsproto"]
LifespanType = Literal["auto", "on", "off"]
InterfaceType = Literal["auto", "asgi3", "asgi2"]

LOG_LEVELS: dict[str, int] = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "trace": TRACE_LOG_LEVEL,
}
HTTP_PROTOCOLS: dict[str, str] = {
    "auto": "uvicorn.protocols.http.auto:AutoHTTPProtocol",
    "h11": "uvicorn.protocols.http.h11_impl:handle",
    "httptools": "uvicorn.protocols.http.httptools_impl:handle",
}
WS_PROTOCOLS: dict[str, str | None] = {
    "auto": "uvicorn.protocols.websockets.auto:AutoWebSocketsProtocol",
    "none": None,
    "websockets": "uvicorn.protocols.websockets.websockets_sansio_impl:handle",
    "websockets-sansio": "uvicorn.protocols.websockets.websockets_sansio_impl:handle",
    "wsproto": "uvicorn.protocols.websockets.wsproto_impl:handle",
}
LIFESPAN: dict[str, str] = {
    "auto": "uvicorn.lifespan.on:LifespanOn",
    "on": "uvicorn.lifespan.on:LifespanOn",
    "off": "uvicorn.lifespan.off:LifespanOff",
}
INTERFACES: list[InterfaceType] = ["auto", "asgi3", "asgi2"]

SSL_PROTOCOL_VERSION: int = ssl.PROTOCOL_TLS_SERVER

LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(message)s",
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
}

logger = logging.getLogger("uvicorn.error")


def create_ssl_context(
    certfile: str | os.PathLike[str],
    keyfile: str | os.PathLike[str] | None,
    password: str | None,
    ssl_version: int,
    cert_reqs: int,
    ca_certs: str | os.PathLike[str] | None,
    ciphers: str | None,
) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl_version)
    get_password = (lambda: password) if password else None
    ctx.load_cert_chain(certfile, keyfile, get_password)
    ctx.verify_mode = ssl.VerifyMode(cert_reqs)
    if ca_certs:
        ctx.load_verify_locations(ca_certs)
    if ciphers:
        ctx.set_ciphers(ciphers)
    return ctx


class Config:
    def __init__(
        self,
        app: ASGIApplication | Callable[..., Any] | str,
        host: str = "127.0.0.1",
        port: int = 8000,
        uds: str | None = None,
        http: HTTPProtocolType | str | Callable[..., Awaitable[None]] = "auto",
        ws: WSProtocolType | str | Callable[..., Awaitable[None]] | None = "auto",
        ws_max_size: int = 16 * 1024 * 1024,
        ws_max_queue: int = 32,
        ws_ping_interval: float | None = 20.0,
        ws_ping_timeout: float | None = 20.0,
        ws_per_message_deflate: bool = True,
        lifespan: LifespanType = "auto",
        env_file: str | os.PathLike[str] | None = None,
        log_config: dict[str, Any] | str | os.PathLike[str] | RawConfigParser | IO[Any] | None = LOGGING_CONFIG,
        log_level: str | int | None = None,
        access_log: bool = True,
        use_colors: bool | None = None,
        interface: InterfaceType = "auto",
        threads: int | None = None,
        proxy_headers: bool = True,
        server_header: bool = True,
        date_header: bool = True,
        forwarded_allow_ips: list[str] | str | None = None,
        root_path: str = "",
        limit_concurrency: int | None = None,
        limit_max_requests: int | None = None,
        limit_max_requests_jitter: int = 0,
        backlog: int = 2048,
        timeout_keep_alive: int = 5,
        timeout_notify: int = 30,
        timeout_graceful_shutdown: int | None = None,
        callback_notify: Callable[..., Awaitable[None]] | None = None,
        ssl_keyfile: str | os.PathLike[str] | None = None,
        ssl_certfile: str | os.PathLike[str] | None = None,
        ssl_keyfile_password: str | None = None,
        ssl_version: int = SSL_PROTOCOL_VERSION,
        ssl_cert_reqs: int = ssl.CERT_NONE,
        ssl_ca_certs: str | os.PathLike[str] | None = None,
        ssl_ciphers: str | None = None,
        ssl_context_factory: Callable[[Config, Callable[[], ssl.SSLContext]], ssl.SSLContext] | None = None,
        headers: list[tuple[str, str]] | None = None,
        factory: bool = False,
        h11_max_incomplete_event_size: int | None = None,
        reset_contextvars: bool = False,
    ):
        self.app = app
        self.host = host
        self.port = port
        self.uds = uds
        self.http = http
        self.ws = ws
        self.ws_max_size = ws_max_size
        self.ws_max_queue = ws_max_queue
        self.ws_ping_interval = ws_ping_interval
        self.ws_ping_timeout = ws_ping_timeout
        self.ws_per_message_deflate = ws_per_message_deflate
        self.lifespan = lifespan
        self.log_config = log_config
        self.log_level = log_level
        self.access_log = access_log
        self.use_colors = use_colors
        self.interface = interface
        self.threads = threads
        self.proxy_headers = proxy_headers
        self.server_header = server_header
        self.date_header = date_header
        self.root_path = root_path
        self.limit_concurrency = limit_concurrency
        self.limit_max_requests = limit_max_requests
        self.limit_max_requests_jitter = limit_max_requests_jitter
        self.backlog = backlog
        self.timeout_keep_alive = timeout_keep_alive
        self.timeout_notify = timeout_notify
        self.timeout_graceful_shutdown = timeout_graceful_shutdown
        self.callback_notify = callback_notify
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile_password = ssl_keyfile_password
        self.ssl_version = ssl_version
        self.ssl_cert_reqs = ssl_cert_reqs
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_ciphers = ssl_ciphers
        self.ssl_context_factory = ssl_context_factory
        self.headers: list[tuple[str, str]] = headers or []
        self.encoded_headers: list[tuple[bytes, bytes]] = []
        self.factory = factory
        self.h11_max_incomplete_event_size = h11_max_incomplete_event_size
        self.reset_contextvars = reset_contextvars

        self.loaded = False
        self.configure_logging()

        if env_file is not None:
            from dotenv import load_dotenv

            logger.info("Loading environment from '%s'", env_file)
            load_dotenv(dotenv_path=env_file)

        if threads is None and "WEB_CONCURRENCY" in os.environ:
            self.threads = int(os.environ["WEB_CONCURRENCY"])

        self.forwarded_allow_ips: list[str] | str
        if forwarded_allow_ips is None:
            self.forwarded_allow_ips = os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1")
        else:
            self.forwarded_allow_ips = forwarded_allow_ips  # pragma: full coverage

    @property
    def asgi_version(self) -> Literal["2.0", "3.0"]:
        mapping: dict[str, Literal["2.0", "3.0"]] = {
            "asgi2": "2.0",
            "asgi3": "3.0",
        }
        return mapping[self.interface]

    @property
    def is_ssl(self) -> bool:
        return bool(self.ssl_keyfile or self.ssl_certfile or self.ssl_context_factory)

    def configure_logging(self) -> None:
        logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")

        if self.log_config is not None:
            if isinstance(self.log_config, os.PathLike):
                self.log_config = os.fspath(self.log_config)

            if isinstance(self.log_config, dict):
                if self.use_colors in (True, False):
                    self.log_config["formatters"]["default"]["use_colors"] = self.use_colors
                    self.log_config["formatters"]["access"]["use_colors"] = self.use_colors
                logging.config.dictConfig(self.log_config)
            elif isinstance(self.log_config, str) and self.log_config.endswith(".json"):
                with open(self.log_config) as file:
                    loaded_config = json.load(file)
                    logging.config.dictConfig(loaded_config)
            elif isinstance(self.log_config, str) and self.log_config.endswith((".yaml", ".yml")):
                try:
                    import yaml
                except ImportError as e:
                    raise ImportError(
                        "Install the PyYAML package or uvicorn[standard] to use `--log-config` with YAML files."
                    ) from e

                with open(self.log_config) as file:
                    loaded_config = yaml.safe_load(file)
                    logging.config.dictConfig(loaded_config)
            else:
                logging.config.fileConfig(self.log_config, disable_existing_loggers=False)

        if self.log_level is not None:
            if isinstance(self.log_level, str):
                log_level = LOG_LEVELS[self.log_level.lower()]
            else:
                log_level = self.log_level
            logging.getLogger("uvicorn.error").setLevel(log_level)
            logging.getLogger("uvicorn.access").setLevel(log_level)
            logging.getLogger("uvicorn.asgi").setLevel(log_level)
        if self.access_log is False:
            logging.getLogger("uvicorn.access").handlers = []
            logging.getLogger("uvicorn.access").propagate = False

    def load_app(self) -> Any:
        """Import the app and return it. Exits on failure."""
        try:
            return import_from_string(self.app)
        except ImportFromStringError as exc:
            logger.error("Error loading ASGI app. %s" % exc)
            sys.exit(1)

    def load(self) -> None:
        assert not self.loaded

        if self.ssl_context_factory is not None:

            def default_factory() -> ssl.SSLContext:
                if not self.ssl_certfile:
                    raise RuntimeError(
                        "`default_ssl_context_factory()` requires `ssl_certfile` to be set on `Config`. "
                        "Either pass `ssl_certfile` (and optionally `ssl_keyfile`) or build the `SSLContext` "
                        "directly inside `ssl_context_factory` without calling the default factory."
                    )
                return create_ssl_context(
                    keyfile=self.ssl_keyfile,
                    certfile=self.ssl_certfile,
                    password=self.ssl_keyfile_password,
                    ssl_version=self.ssl_version,
                    cert_reqs=self.ssl_cert_reqs,
                    ca_certs=self.ssl_ca_certs,
                    ciphers=self.ssl_ciphers,
                )

            context = self.ssl_context_factory(self, default_factory)
            if not isinstance(context, ssl.SSLContext):
                raise TypeError(f"`ssl_context_factory` must return an `ssl.SSLContext`, got {type(context).__name__}")
            self.ssl: ssl.SSLContext | None = context
        elif self.is_ssl:
            assert self.ssl_certfile
            self.ssl = create_ssl_context(
                keyfile=self.ssl_keyfile,
                certfile=self.ssl_certfile,
                password=self.ssl_keyfile_password,
                ssl_version=self.ssl_version,
                cert_reqs=self.ssl_cert_reqs,
                ca_certs=self.ssl_ca_certs,
                ciphers=self.ssl_ciphers,
            )
        else:
            self.ssl = None

        encoded_headers = [(key.lower().encode("latin1"), value.encode("latin1")) for key, value in self.headers]
        self.encoded_headers = (
            [(b"server", b"uvicorn")] + encoded_headers
            if b"server" not in dict(encoded_headers) and self.server_header
            else encoded_headers
        )

        if isinstance(self.http, str):
            http_protocol_class = import_from_string(HTTP_PROTOCOLS.get(self.http, self.http))
            self.http_protocol_class: Callable[..., Awaitable[None]] = http_protocol_class
        else:
            self.http_protocol_class = self.http  # type: ignore[assignment]

        if isinstance(self.ws, str):
            ws_protocol_class = import_from_string(WS_PROTOCOLS.get(self.ws, self.ws))
            self.ws_protocol_class: Callable[..., Awaitable[None]] | None = ws_protocol_class
        else:
            self.ws_protocol_class = self.ws  # type: ignore[assignment]

        self.lifespan_class = import_from_string(LIFESPAN[self.lifespan])

        self.loaded_app = self.load_app()

        try:
            self.loaded_app = self.loaded_app()
        except TypeError as exc:
            if self.factory:
                logger.error("Error loading ASGI app factory: %s", exc)
                sys.exit(1)
        else:
            if not self.factory:
                logger.warning(
                    "ASGI app factory detected. Using it, but please consider setting the --factory flag explicitly."
                )

        if self.interface == "auto":
            if inspect.isclass(self.loaded_app):
                use_asgi_3 = hasattr(self.loaded_app, "__await__")
            elif inspect.isfunction(self.loaded_app):
                use_asgi_3 = inspect.iscoroutinefunction(self.loaded_app)
            else:
                call = getattr(self.loaded_app, "__call__", None)
                use_asgi_3 = inspect.iscoroutinefunction(call)
            self.interface = "asgi3" if use_asgi_3 else "asgi2"

        if self.interface == "asgi2":
            self.loaded_app = ASGI2Middleware(self.loaded_app)

        if logger.getEffectiveLevel() <= TRACE_LOG_LEVEL:
            self.loaded_app = MessageLoggerMiddleware(self.loaded_app)
        if self.proxy_headers:
            self.loaded_app = ProxyHeadersMiddleware(self.loaded_app, trusted_hosts=self.forwarded_allow_ips)

        self.loaded = True
