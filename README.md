# toncorn

*An ASGI web server for free-threaded Python, powered by [tonio][tonio].*

`toncorn` is a fork of [uvicorn][uvicorn] that swaps the asyncio runtime for [tonio][tonio].

## Install

```shell
$ pip install toncorn
```

`toncorn` requires free-threaded CPython 3.14 or newer (`python3.14t`) and runs `PYTHON_GIL=0`.
Linux and macOS only; Windows is not supported and won't be (tonio doesn't target it).

For the optional protocol/parser extras (matching upstream uvicorn's `[standard]` set):

```shell
$ pip install 'toncorn[standard]'
```

## Quickstart

`example.py`:

```python
async def app(scope, receive, send):
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({
        "type": "http.response.body",
        "body": b"Hello, world!",
    })
```

Run it:

```shell
$ PYTHON_GIL=0 toncorn example:app
```

You'll see a banner identifying the toncorn version and the uvicorn version it
tracks:

```
INFO:     toncorn 0.48.0.0 (uvicorn 0.48.0)
INFO:     Started server process [12345]
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

The package ships both `import toncorn` and `import uvicorn` — the source tree
lives under the original `uvicorn` package name to keep the diff against
upstream small, and `toncorn` is a thin re-export. Pick whichever import you
prefer; they expose the same `Config`, `Server`, `run`, `main`.

> **Note**: this means `toncorn` and the real PyPI `uvicorn` collide on the `uvicorn` import name; don't install both in the same environment.

## Programmatic use

```python
import toncorn
toncorn.run("example:app", host="127.0.0.1", port=8000)
```

Or with an explicit Config + Server:

```python
import toncorn

config = toncorn.Config(app=app, host="127.0.0.1", port=8000)
server = toncorn.Server(config)
server.run()
```

## Differences from uvicorn

- **Runtime is tonio, not asyncio**. There's no `--loop` flag, no
  `uvloop`/`asyncio` switch. Everything runs on a single tonio runtime with a
  configurable thread pool.
- **Concurrency model is threads, not processes**. The old `--workers N` flag
  is replaced by `--threads N`, which sizes the tonio scheduler's thread pool.
  No `multiprocessing` supervisor, no preforked workers, no graceful-restart
  process orchestration.
- **No `--reload`**. The auto-reload supervisor is gone. Use your editor's
  hot-reload or a separate process manager during development.
- **No Windows support**. POSIX only.
- **No in-tree WSGI middleware**. The deprecated WSGI bridge has been removed;
  use `a2wsgi` or `asgiref.wsgi` if you need WSGI compatibility.
- **Python 3.14+ only**. Earlier Python versions are not supported.

The version number reflects both upstream and fork: `{uvicorn}.{toncorn}`, so
`0.48.0.0` tracks uvicorn `0.48.0` with toncorn patch `0`. Patch-level
releases against the same uvicorn baseline bump the last segment
(`0.48.0.0` → `0.48.0.1`).

## Upstream tracking

The source tree under `uvicorn/` is kept as close to upstream uvicorn as
possible to make merges/rebases tractable. Fork-specific changes are
intentionally minimal and called out where they appear. The `toncorn/` package
is a re-export shim plus the toncorn-specific version.

## License

BSD-3-Clause, same as upstream uvicorn. See `LICENSE.md`.

[uvicorn]: https://github.com/Kludex/uvicorn
[tonio]: https://github.com/gi0baro/tonio
