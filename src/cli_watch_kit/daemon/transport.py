"""The daemon transport: NDJSON, one request and one response per connection.

POSIX uses a Unix socket (`0600`) in the private state dir (`0700`), so requests may carry secrets such as an API
key. Windows uses loopback TCP on a random port; the port and a random token are in a file only the user can read,
and every request must carry the token, because any local user can connect to a loopback port. Setting
`<PREFIX>TRANSPORT=tcp` selects TCP on POSIX too, which is how the Windows path is tested on Linux.

The file names carry the library's and the app's protocol versions, so daemons of different protocols never meet.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import os
import secrets
import socket
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cli_watch_kit.paths import ensure_private_dir

if TYPE_CHECKING:
    from cli_watch_kit.app import WatchApp

log = logging.getLogger(__name__)

SHUTDOWN_GRACE = 1.0
"""Seconds a stopping daemon gives the requests in flight before it cuts their connections."""
MAX_LINE = 64 * 1024 * 1024
"""Enough for a large artifact, base64-encoded, in one response line."""

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class DaemonUnavailable(ConnectionError):
    """No daemon answers on the endpoint."""


class DaemonTimeout(TimeoutError):
    """The daemon took the request but did not answer in time. Unlike a gone daemon, it may still carry it out."""


class DaemonStartFailed(DaemonUnavailable):
    """A daemon was started but failed before it listened; trying again would fail the same way."""


class DaemonMismatch(RuntimeError):
    """The daemon does not know a request: it runs other code than the watcher under the same protocol versions."""


MAX_SOCKET_PATH_BYTES = 103 if sys.platform == "darwin" else 107
"""The longest Unix socket path the OS takes: `sun_path` holds 104 bytes on macOS and 108 on Linux, with a NUL."""


@dataclass(frozen=True)
class DaemonFiles:
    """The daemon's files in its state dir. `transport_env` names the variable that selects TCP on POSIX."""

    state_dir: Path
    stem: str = "daemon"
    transport_env: str = ""
    state_dir_env: str = ""

    @classmethod
    def for_app(cls, app: WatchApp, state_dir: Path | None = None) -> DaemonFiles:
        from cli_watch_kit.app import LIB_PROTOCOL

        return cls(
            state_dir or app.paths.state_dir(),
            f"daemon-l{LIB_PROTOCOL}-a{app.app_protocol}",
            app.env("TRANSPORT"),
            app.paths.state_dir_env,
        )

    @property
    def use_tcp(self) -> bool:
        return (
            os.name == "nt"
            or (bool(self.transport_env) and os.environ.get(self.transport_env) == "tcp")
            or not hasattr(socket, "AF_UNIX")
        )

    def _name(self, suffix: str) -> Path:
        return self.state_dir / f"{self.stem}.{suffix}"

    @property
    def lock(self) -> Path:
        return self._name("lock")

    @property
    def spawn_lock(self) -> Path:
        return self.state_dir / f"spawn-{self.stem}.lock"

    @property
    def socket(self) -> Path:
        return self._name("sock")

    @property
    def endpoint(self) -> Path:
        return self._name("json")

    @property
    def log(self) -> Path:
        return self._name("log")

    @property
    def transport_name(self) -> str:
        return "tcp" if self.use_tcp else "unix"

    def describe_endpoint(self) -> str:
        return str(self.endpoint if self.use_tcp else self.socket)

    def check_socket_path(self) -> None:
        """Fail with a clear message when the socket path is too long for a Unix socket. Without this, the daemon
        dies at bind with only an empty log, and the watcher waits out its start timeout."""
        if self.use_tcp:
            return
        length = len(os.fsencode(self.socket))
        if length > MAX_SOCKET_PATH_BYTES:
            hint = f"; set {self.state_dir_env} to a shorter dir" if self.state_dir_env else ""
            raise DaemonUnavailable(
                f"the daemon socket path {self.socket} is {length} bytes, above the {MAX_SOCKET_PATH_BYTES}-byte limit"
                f" of Unix sockets{hint}"
            )


def _write_private(path: Path, content: str) -> None:
    tmp = path.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(content)
    os.replace(tmp, path)


@dataclass
class Listener:
    """The listening daemon: `cleanup` removes the endpoint file, `close` ends the server and its connections."""

    server: asyncio.AbstractServer
    cleanup: Callable[[], None]
    connections: set[asyncio.Task[Any]]

    async def close(self, grace: float = SHUTDOWN_GRACE) -> None:
        """Stop accepting, give the requests in flight `grace` seconds to finish, then cut the rest.

        `Server.wait_closed()` waits for every open connection since Python 3.12, so a silent or slow client would
        otherwise keep the daemon, and its lock, alive until the connection's read timeout.
        """
        self.server.close()
        pending = {t for t in self.connections if not t.done()}
        if pending:
            _, pending = await asyncio.wait(pending, timeout=grace)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=grace)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.server.wait_closed(), grace)


async def start_server(files: DaemonFiles, handler: Handler) -> Listener:
    """Start listening on the daemon's endpoint."""
    ensure_private_dir(files.state_dir)
    token = secrets.token_hex(16) if files.use_tcp else None
    connections: set[asyncio.Task[Any]] = set()

    async def on_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            connections.add(task)
            task.add_done_callback(connections.discard)
        try:
            line = await asyncio.wait_for(reader.readuntil(b"\n"), 10)
            request = json.loads(line)
            if not isinstance(request, dict):
                response: dict[str, Any] = {"ok": False, "error": "request must be a JSON object"}
            elif token is not None and not hmac.compare_digest(str(request.get("token", "")), token):
                response = {"ok": False, "error": "bad token"}
            else:
                request.pop("token", None)
                response = await handler(request)
            writer.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
            await writer.drain()
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError, OSError):
            pass
        except Exception:
            log.exception("a daemon connection failed")
        finally:
            writer.close()
            with contextlib.suppress(OSError, ConnectionError):
                await writer.wait_closed()

    if token is None:
        with contextlib.suppress(FileNotFoundError):
            files.socket.unlink()
        server = await asyncio.start_unix_server(on_connection, path=str(files.socket), limit=MAX_LINE)
        os.chmod(files.socket, 0o600)

        def cleanup() -> None:
            with contextlib.suppress(OSError):
                files.socket.unlink()

        return Listener(server, cleanup, connections)

    server = await asyncio.start_server(on_connection, host="127.0.0.1", port=0, limit=MAX_LINE)
    port = server.sockets[0].getsockname()[1]
    _write_private(files.endpoint, json.dumps({"port": port, "token": token, "pid": os.getpid()}))

    def cleanup_tcp() -> None:
        with contextlib.suppress(OSError):
            files.endpoint.unlink()

    return Listener(server, cleanup_tcp, connections)


def _connect(files: DaemonFiles, timeout: float) -> tuple[socket.socket, str | None]:
    if files.use_tcp:
        try:
            endpoint = json.loads(files.endpoint.read_text(encoding="utf-8"))
            port, token = int(endpoint["port"]), str(endpoint["token"])
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise DaemonUnavailable(f"no daemon endpoint in {files.endpoint}") from exc
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=timeout), token
        except OSError as exc:
            raise DaemonUnavailable(str(exc)) from exc
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(files.socket))
    except OSError as exc:
        sock.close()
        raise DaemonUnavailable(str(exc)) from exc
    return sock, None


def request(files: DaemonFiles, payload: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
    """Send one request and return the response.

    Raises :class:`DaemonUnavailable` when no daemon listens, or it closed the connection or cut its answer short (a
    request to a daemon that died is safe to send again), and :class:`DaemonTimeout` when it did not answer within
    `timeout` seconds (it may still be working on it)."""
    sock, token = _connect(files, timeout)
    with sock:
        body = dict(payload)
        if token is not None:
            body["token"] = token
        chunks = []
        try:
            sock.sendall(json.dumps(body, separators=(",", ":")).encode() + b"\n")
            total = 0
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if chunk.endswith(b"\n") or total > MAX_LINE:
                    break
        except TimeoutError as exc:
            # The daemon is there but slow; it may still carry out the request, so this is no reason to retry it.
            raise DaemonTimeout(f"the daemon did not answer within {timeout:.0f} s") from exc
        except OSError as exc:
            raise DaemonUnavailable(str(exc)) from exc
    raw = b"".join(chunks)
    if not raw.strip():
        raise DaemonUnavailable("the daemon closed the connection without answering")
    try:
        response = json.loads(raw)
    except ValueError as exc:
        raise DaemonUnavailable("the daemon's answer was cut off") from exc
    if not isinstance(response, dict):
        raise DaemonUnavailable("the daemon sent an unexpected response")
    return response
