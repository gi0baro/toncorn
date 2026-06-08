from __future__ import annotations

import ssl
from collections.abc import Callable

import httpx
import pytest
import trustme
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from tests.utils import run_server
from uvicorn.config import Config

pytestmark = pytest.mark.tonio

type DefaultFactory = Callable[[], ssl.SSLContext]


async def app(scope, receive, send):
    assert scope["type"] == "http"
    await send({"type": "http.response.start", "status": 204, "headers": []})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


# Yield-style pytest fixtures trigger a tonio runtime panic when their value is
# read inside an async test ("Got unsupported value '<x>' from gen iteration",
# src/handles.rs:188). Builders below return plain objects so each test can drive
# `with trustme.Blob(...).tempfile()` inline.
def _new_ca() -> tuple[trustme.CA, trustme.LeafCert, ssl.SSLContext]:
    ca = trustme.CA()
    leaf = ca.issue_cert("localhost", "127.0.0.1", "::1")
    client_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ca.configure_trust(client_ctx)
    return ca, leaf, client_ctx


def _encrypted_key_blob(leaf: trustme.LeafCert, password: bytes) -> trustme.Blob:
    private_key = serialization.load_pem_private_key(
        leaf.private_key_pem.bytes(),
        password=None,
        backend=default_backend(),
    )
    encrypted = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.BestAvailableEncryption(password),
    )
    return trustme.Blob(encrypted)


async def test_run(unused_tcp_port: int):
    ca, leaf, client_ctx = _new_ca()
    with (
        leaf.cert_chain_pems[0].tempfile() as certpath,
        leaf.private_key_pem.tempfile() as keypath,
        ca.cert_pem.tempfile() as capath,
    ):
        config = Config(
            app=app,
            limit_max_requests=1,
            ssl_keyfile=keypath,
            ssl_certfile=certpath,
            ssl_ca_certs=capath,
            port=unused_tcp_port,
        )
        async with run_server(config):
            with httpx.Client(verify=client_ctx) as client:
                response = client.get(f"https://127.0.0.1:{unused_tcp_port}")
        assert response.status_code == 204


async def test_run_chain(unused_tcp_port: int):
    ca, leaf, client_ctx = _new_ca()
    with (
        leaf.private_key_and_cert_chain_pem.tempfile() as chainpath,
        ca.cert_pem.tempfile() as capath,
    ):
        config = Config(
            app=app,
            limit_max_requests=1,
            ssl_certfile=chainpath,
            ssl_ca_certs=capath,
            port=unused_tcp_port,
        )
        async with run_server(config):
            with httpx.Client(verify=client_ctx) as client:
                response = client.get(f"https://127.0.0.1:{unused_tcp_port}")
        assert response.status_code == 204


async def test_run_chain_only(unused_tcp_port: int):
    _ca, leaf, client_ctx = _new_ca()
    with leaf.private_key_and_cert_chain_pem.tempfile() as chainpath:
        config = Config(
            app=app,
            limit_max_requests=1,
            ssl_certfile=chainpath,
            port=unused_tcp_port,
        )
        async with run_server(config):
            with httpx.Client(verify=client_ctx) as client:
                response = client.get(f"https://127.0.0.1:{unused_tcp_port}")
        assert response.status_code == 204


async def test_run_password(unused_tcp_port: int):
    ca, leaf, client_ctx = _new_ca()
    password = b"uvicorn password for the win"
    encrypted_key = _encrypted_key_blob(leaf, password)
    with (
        leaf.cert_chain_pems[0].tempfile() as certpath,
        encrypted_key.tempfile() as keypath,
        ca.cert_pem.tempfile() as capath,
    ):
        config = Config(
            app=app,
            limit_max_requests=1,
            ssl_keyfile=keypath,
            ssl_certfile=certpath,
            ssl_keyfile_password=password.decode(),
            ssl_ca_certs=capath,
            port=unused_tcp_port,
        )
        async with run_server(config):
            with httpx.Client(verify=client_ctx) as client:
                response = client.get(f"https://127.0.0.1:{unused_tcp_port}")
        assert response.status_code == 204


async def test_run_ssl_context_factory_default(unused_tcp_port: int) -> None:
    """A factory that just delegates to the default factory should produce a working server."""
    _ca, leaf, client_ctx = _new_ca()

    def ssl_context_factory(config: Config, default_ssl_context_factory: DefaultFactory) -> ssl.SSLContext:
        return default_ssl_context_factory()

    with (
        leaf.cert_chain_pems[0].tempfile() as certpath,
        leaf.private_key_pem.tempfile() as keypath,
    ):
        config = Config(
            app=app,
            limit_max_requests=1,
            ssl_keyfile=keypath,
            ssl_certfile=certpath,
            ssl_context_factory=ssl_context_factory,
            port=unused_tcp_port,
        )
        async with run_server(config):
            with httpx.Client(verify=client_ctx) as client:
                response = client.get(f"https://127.0.0.1:{unused_tcp_port}")
        assert response.status_code == 204


async def test_run_ssl_context_factory_custom(unused_tcp_port: int) -> None:
    """A factory that builds its own SSLContext from scratch should work without ssl_keyfile/ssl_certfile."""
    _ca, leaf, client_ctx = _new_ca()

    with (
        leaf.cert_chain_pems[0].tempfile() as certpath,
        leaf.private_key_pem.tempfile() as keypath,
    ):

        def ssl_context_factory(config: Config, default_ssl_context_factory: DefaultFactory) -> ssl.SSLContext:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(certpath, keypath)
            return ctx

        config = Config(
            app=app,
            limit_max_requests=1,
            ssl_context_factory=ssl_context_factory,
            port=unused_tcp_port,
        )
        async with run_server(config):
            with httpx.Client(verify=client_ctx) as client:
                response = client.get(f"https://127.0.0.1:{unused_tcp_port}")
        assert response.status_code == 204


def test_ssl_context_factory_mutates_default(
    tls_certificate_server_cert_path: str,
    tls_certificate_private_key_path: str,
) -> None:
    """The factory can call the default and mutate the result (e.g., bump TLS minimum version)."""

    def ssl_context_factory(config: Config, default_ssl_context_factory: DefaultFactory) -> ssl.SSLContext:
        ctx = default_ssl_context_factory()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        return ctx

    config = Config(
        app=app,
        ssl_keyfile=tls_certificate_private_key_path,
        ssl_certfile=tls_certificate_server_cert_path,
        ssl_context_factory=ssl_context_factory,
    )
    config.load()
    assert config.is_ssl
    assert isinstance(config.ssl, ssl.SSLContext)
    assert config.ssl.minimum_version == ssl.TLSVersion.TLSv1_3


def test_default_ssl_context_factory_requires_ssl_certfile() -> None:
    """Calling `default_ssl_context_factory()` without `ssl_certfile` raises a clear error."""

    def ssl_context_factory(config: Config, default_ssl_context_factory: DefaultFactory) -> ssl.SSLContext:
        return default_ssl_context_factory()

    config = Config(app=app, ssl_context_factory=ssl_context_factory)
    with pytest.raises(RuntimeError, match="requires `ssl_certfile`"):
        config.load()


def test_ssl_context_factory_must_return_ssl_context() -> None:
    def bad_factory(config: Config, default_ssl_context_factory: DefaultFactory) -> object:
        return "not an SSLContext"

    config = Config(app=app, ssl_context_factory=bad_factory)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must return an `ssl.SSLContext`"):
        config.load()


def test_ssl_ciphers_applied_when_set(
    tls_certificate_server_cert_path: str,
    tls_certificate_private_key_path: str,
) -> None:
    config = Config(
        app=app,
        ssl_keyfile=tls_certificate_private_key_path,
        ssl_certfile=tls_certificate_server_cert_path,
        ssl_ciphers="HIGH",
    )
    config.load()
    assert isinstance(config.ssl, ssl.SSLContext)


def test_is_ssl_true_when_only_factory_set() -> None:
    def ssl_context_factory(config: Config, default_ssl_context_factory: DefaultFactory) -> ssl.SSLContext:
        return ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)  # pragma: no cover

    config = Config(app=app, ssl_context_factory=ssl_context_factory)
    assert config.is_ssl is True
