from __future__ import annotations

import configparser
import io
import json
import logging
import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import IO, Any, Literal
from unittest.mock import MagicMock

import pytest
import yaml
from pytest_mock import MockerFixture

from uvicorn._types import ASGIApplication, ASGIReceiveCallable, ASGISendCallable, Scope
from uvicorn.config import Config
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from uvicorn.protocols.http.h11_impl import handle as h11_handle


@pytest.fixture
def mocked_logging_config_module(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("logging.config")


@pytest.fixture
def json_logging_config(logging_config: dict) -> str:
    return json.dumps(logging_config)


@pytest.fixture
def yaml_logging_config(logging_config: dict) -> str:
    return yaml.dump(logging_config)


async def asgi_app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
    pass  # pragma: nocover


def test_proxy_headers() -> None:
    config = Config(app=asgi_app)
    config.load()

    assert config.proxy_headers is True
    assert isinstance(config.loaded_app, ProxyHeadersMiddleware)


def test_app_unimportable_module() -> None:
    config = Config(app="no.such:app")
    with pytest.raises(ImportError):
        config.load()


def test_app_unimportable_other(caplog: pytest.LogCaptureFixture) -> None:
    config = Config(app="tests.test_config:app")
    with pytest.raises(SystemExit):
        config.load()
    error_messages = [
        record.message for record in caplog.records if record.name == "uvicorn.error" and record.levelname == "ERROR"
    ]
    assert 'Error loading ASGI app. Attribute "app" not found in module "tests.test_config".' == error_messages.pop(0)


def test_app_factory(caplog: pytest.LogCaptureFixture) -> None:
    def create_app() -> ASGIApplication:
        return asgi_app

    config = Config(app=create_app, factory=True, proxy_headers=False)
    config.load()
    assert config.loaded_app is asgi_app

    caplog.clear()
    config = Config(app=create_app, proxy_headers=False)
    with caplog.at_level(logging.WARNING):
        config.load()
    assert config.loaded_app is asgi_app
    assert len(caplog.records) == 1
    assert "--factory" in caplog.records[0].message

    config = Config(app=asgi_app, factory=True)
    with pytest.raises(SystemExit):
        config.load()


def test_concrete_http_handler() -> None:
    """A pre-resolved handle callable can be passed directly as `http=`."""
    config = Config(app=asgi_app, http=h11_handle)
    config.load()
    assert config.http_protocol_class is h11_handle


def test_ssl_config(
    tls_ca_certificate_pem_path: str,
    tls_ca_certificate_private_key_path: str,
) -> None:
    config = Config(
        app=asgi_app,
        ssl_certfile=tls_ca_certificate_pem_path,
        ssl_keyfile=tls_ca_certificate_private_key_path,
    )
    config.load()

    assert config.is_ssl is True


def test_ssl_config_combined(tls_certificate_key_and_chain_path: str) -> None:
    config = Config(
        app=asgi_app,
        ssl_certfile=tls_certificate_key_and_chain_path,
    )
    config.load()

    assert config.is_ssl is True


def asgi2_app(scope: Scope) -> Callable:
    async def asgi(receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:  # pragma: nocover
        pass

    return asgi  # pragma: nocover


@pytest.mark.parametrize("app, expected_interface", [(asgi_app, "3.0"), (asgi2_app, "2.0")])
def test_asgi_version(app: ASGIApplication, expected_interface: Literal["2.0", "3.0"]) -> None:
    config = Config(app=app)
    config.load()
    assert config.asgi_version == expected_interface


@pytest.mark.parametrize(
    "use_colors, expected",
    [
        pytest.param(None, None, id="use_colors_not_provided"),
        pytest.param("invalid", None, id="use_colors_invalid_value"),
        pytest.param(True, True, id="use_colors_enabled"),
        pytest.param(False, False, id="use_colors_disabled"),
    ],
)
def test_log_config_default(
    mocked_logging_config_module: MagicMock,
    use_colors: bool | None,
    expected: bool | None,
    logging_config: dict[str, Any],
) -> None:
    config = Config(app=asgi_app, use_colors=use_colors, log_config=logging_config)
    config.load()

    mocked_logging_config_module.dictConfig.assert_called_once_with(logging_config)

    (provided_dict_config,), _ = mocked_logging_config_module.dictConfig.call_args
    assert provided_dict_config["formatters"]["default"]["use_colors"] == expected


def test_log_config_json(
    mocked_logging_config_module: MagicMock,
    logging_config: dict[str, Any],
    json_logging_config: str,
    mocker: MockerFixture,
) -> None:
    mocked_open = mocker.patch("uvicorn.config.open", mocker.mock_open(read_data=json_logging_config))

    config = Config(app=asgi_app, log_config="log_config.json")
    config.load()

    mocked_open.assert_called_once_with("log_config.json")
    mocked_logging_config_module.dictConfig.assert_called_once_with(logging_config)


@pytest.mark.parametrize("config_filename", ["log_config.yml", "log_config.yaml"])
def test_log_config_yaml(
    mocked_logging_config_module: MagicMock,
    logging_config: dict[str, Any],
    yaml_logging_config: str,
    mocker: MockerFixture,
    config_filename: str,
) -> None:
    mocked_open = mocker.patch("uvicorn.config.open", mocker.mock_open(read_data=yaml_logging_config))

    config = Config(app=asgi_app, log_config=config_filename)
    config.load()

    mocked_open.assert_called_once_with(config_filename)
    mocked_logging_config_module.dictConfig.assert_called_once_with(logging_config)


def test_log_config_yaml_missing_pyyaml(mocked_logging_config_module: MagicMock, mocker: MockerFixture) -> None:
    mocker.patch.dict(sys.modules, {"yaml": None})
    with pytest.raises(ImportError, match=r"Install the PyYAML package or uvicorn\[standard\]"):
        Config(app=asgi_app, log_config="log_config.yaml")


def test_log_config_pathlike(
    mocked_logging_config_module: MagicMock,
    logging_config: dict[str, Any],
    json_logging_config: str,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    path = tmp_path / "log_config.json"
    mocked_open = mocker.patch("uvicorn.config.open", mocker.mock_open(read_data=json_logging_config))

    config = Config(app=asgi_app, log_config=path)
    config.load()

    mocked_open.assert_called_once_with(os.fspath(path))
    mocked_logging_config_module.dictConfig.assert_called_once_with(logging_config)


@pytest.mark.parametrize("config_file", ["log_config.ini", configparser.ConfigParser(), io.StringIO()])
def test_log_config_file(
    mocked_logging_config_module: MagicMock,
    config_file: str | configparser.RawConfigParser | IO[Any],
) -> None:
    config = Config(app=asgi_app, log_config=config_file)
    config.load()

    mocked_logging_config_module.fileConfig.assert_called_once_with(config_file, disable_existing_loggers=False)


@pytest.fixture(params=[0, 1])
def web_concurrency(request: pytest.FixtureRequest) -> Iterator[int]:
    yield request.param
    if os.getenv("WEB_CONCURRENCY"):
        del os.environ["WEB_CONCURRENCY"]


@pytest.fixture(params=["127.0.0.1", "127.0.0.2"])
def forwarded_allow_ips(request: pytest.FixtureRequest) -> Iterator[str]:
    yield request.param
    if os.getenv("FORWARDED_ALLOW_IPS"):
        del os.environ["FORWARDED_ALLOW_IPS"]


def test_env_file(
    web_concurrency: int,
    forwarded_allow_ips: str,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    fp = tmp_path / ".env"
    content = f"WEB_CONCURRENCY={web_concurrency}\nFORWARDED_ALLOW_IPS={forwarded_allow_ips}\n"
    fp.write_text(content)
    with caplog.at_level(logging.INFO):
        config = Config(app=asgi_app, env_file=fp)
        config.load()

    assert config.threads == int(str(os.getenv("WEB_CONCURRENCY")))
    assert config.forwarded_allow_ips == os.getenv("FORWARDED_ALLOW_IPS")
    assert len(caplog.records) == 1
    assert f"Loading environment from '{fp}'" in caplog.records[0].message


@pytest.mark.parametrize(
    "access_log, handlers",
    [
        pytest.param(True, 1, id="access log enabled should have single handler"),
        pytest.param(False, 0, id="access log disabled shouldn't have handlers"),
    ],
)
def test_config_access_log(access_log: bool, handlers: int) -> None:
    config = Config(app=asgi_app, access_log=access_log)
    config.load()

    assert len(logging.getLogger("uvicorn.access").handlers) == handlers
    assert config.access_log == access_log


@pytest.mark.parametrize("log_level", [5, 10, 20, 30, 40, 50])
def test_config_log_level(log_level: int) -> None:
    config = Config(app=asgi_app, log_level=log_level)
    config.load()

    assert logging.getLogger("uvicorn.error").level == log_level
    assert logging.getLogger("uvicorn.access").level == log_level
    assert logging.getLogger("uvicorn.asgi").level == log_level
    assert config.log_level == log_level


@pytest.mark.parametrize("log_level", [None, 0, 5, 10, 20, 30, 40, 50])
@pytest.mark.parametrize("uvicorn_logger_level", [0, 5, 10, 20, 30, 40, 50])
def test_config_log_effective_level(log_level: int, uvicorn_logger_level: int) -> None:
    default_level = 30
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {
            "uvicorn": {"level": uvicorn_logger_level},
        },
    }
    config = Config(app=asgi_app, log_level=log_level, log_config=log_config)
    config.load()

    effective_level = log_level or uvicorn_logger_level or default_level
    assert logging.getLogger("uvicorn.error").getEffectiveLevel() == effective_level
    assert logging.getLogger("uvicorn.access").getEffectiveLevel() == effective_level
    assert logging.getLogger("uvicorn.asgi").getEffectiveLevel() == effective_level


@pytest.mark.parametrize("log_level", ["INFO", "Info", "info"])
def test_config_log_level_case_insensitive(log_level: str) -> None:
    config = Config(app=asgi_app, log_level=log_level)
    config.load()
    assert logging.getLogger("uvicorn.error").level == logging.INFO


def test_ws_max_size() -> None:
    config = Config(app=asgi_app, ws_max_size=1000)
    config.load()
    assert config.ws_max_size == 1000


def test_ws_max_queue() -> None:
    config = Config(app=asgi_app, ws_max_queue=64)
    config.load()
    assert config.ws_max_queue == 64
