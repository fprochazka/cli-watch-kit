"""Daemon lifecycle: the daemon's own main loop, and the watcher side that finds, starts or replaces it.

There is one daemon per user, app and pair of protocol versions. It holds an exclusive lock on its lock file for its
whole life, and exits after `idle_timeout` seconds without subscriptions. Watchers start it on demand; the spawn lock
makes concurrent watchers start one daemon, not one each.

The daemon is shared by every watcher of the user, so it must not be a descendant of the watcher that started it:
tools that stop a process together with its whole tree (Claude Code's task stop does) would take it down too. A new
session is not enough, because the parent stays the same. On POSIX the spawned process therefore forks once more and
exits as soon as the daemon is ready, so the daemon is reparented to init or the nearest subreaper.

**Upgrades.** The protocol versions in the file names keep incompatible daemons apart. Within one protocol, a daemon
may still run older code than a watcher, for example after `git pull` in an editable install. `hello` reports the
daemon's code stamp; a watcher whose code is newer asks the daemon to `drain` (stop), and starts a current one. The
watchers of the old daemon then resubscribe from their baselines and lose nothing. A watcher with older code than the
daemon uses it as it is.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from cli_watch_kit.app import WatchApp
from cli_watch_kit.daemon.engine import DEFAULT_CLIENT_POLL_MS, DEFAULT_LEASE_TIMEOUT, Engine
from cli_watch_kit.daemon.server import handle
from cli_watch_kit.daemon.transport import DaemonFiles, DaemonStartFailed, DaemonUnavailable, request, start_server
from cli_watch_kit.locks import FileLock
from cli_watch_kit.paths import ensure_private_dir

log = logging.getLogger("cli_watch_kit.daemon")

DEFAULT_IDLE_TIMEOUT = 30.0
SPAWN_TIMEOUT = 10.0
MAX_LOG_BYTES = 5 * 1024 * 1024


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


async def serve(
    app: WatchApp,
    files: DaemonFiles,
    engine: Engine,
    idle_timeout: float,
    on_ready: Callable[[], None] | None = None,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if os.name != "nt":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
    identity = app.identity()

    async def on_request(req: dict[str, Any]) -> dict[str, Any]:
        return await handle(engine, req, stop, identity)

    listener = await start_server(files, on_request)
    log.info(
        "daemon %s %s (cli-watch-kit %s, code %s) started, pid %d, endpoint %s, idle timeout %.0f s",
        app.name,
        app.version,
        app.lib_version,
        identity["fingerprint"],
        os.getpid(),
        files.describe_endpoint(),
        idle_timeout,
    )
    if on_ready is not None:
        on_ready()
    engine_task = asyncio.create_task(engine.run(stop))
    try:
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 1.0)
            if engine_task.done():
                # The engine never stops on its own: without it the watchers would wait forever. Exit, so they
                # start a new daemon and resubscribe.
                reason = engine_task.exception() if not engine_task.cancelled() else "cancelled"
                log.error("the engine stopped (%r), exiting", reason)
                stop.set()
            elif engine.idle_for(engine.clock()) >= idle_timeout:
                log.info("no subscriptions for %.0f s, exiting", idle_timeout)
                stop.set()
    finally:
        # Remove the endpoint first, so a watcher that connects from now on starts a new daemon.
        listener.cleanup()
        await listener.close()
        stop.set()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await engine_task
        log.info("daemon stopped")


def run_daemon(app: WatchApp, argv: Sequence[str] = ()) -> int:
    """The daemon process. On POSIX it detaches first, unless `--foreground` is given (for debugging)."""
    if os.name != "nt" and "--foreground" not in argv:
        return _detach(app)
    return _serve_daemon(app, None)


def _detach(app: WatchApp) -> int:
    """Fork the daemon off this launcher process, wait until it is ready or gave up, then let the launcher exit."""
    read_end, write_end = os.pipe()
    pid = os.fork()
    if pid > 0:
        os.close(write_end)
        with os.fdopen(read_end, "rb") as ready:
            # One byte once the daemon listens or knows another daemon holds the lock; EOF if it died first. The
            # exit code tells the watcher that started it whether to wait for it or to give up.
            started = ready.read(1)
        return 0 if started else 1
    os.close(read_end)
    code = 1
    try:
        code = _serve_daemon(app, write_end)
    except BaseException:
        # Without this the traceback would die with os._exit below, and the log would say nothing.
        traceback.print_exc()
        sys.stderr.flush()
    finally:
        logging.shutdown()
        # Never fall back into the launcher's code path, whatever happened above.
        os._exit(code)


def _serve_daemon(app: WatchApp, ready_fd: int | None) -> int:
    """Take the lock or exit, then serve until idle or stopped. `ready_fd` gets one byte when the launcher may go."""

    def notify(ready: bool = True) -> None:
        nonlocal ready_fd
        if ready_fd is not None:
            with contextlib.suppress(OSError):
                if ready:
                    os.write(ready_fd, b"1")
                os.close(ready_fd)
            ready_fd = None

    files = DaemonFiles.for_app(app)
    ensure_private_dir(files.state_dir)
    logging.basicConfig(
        level=logging.DEBUG if os.environ.get(app.env("DEBUG")) else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    lock = FileLock(files.lock)
    try:
        if not lock.try_acquire():
            log.info("another daemon holds %s, exiting", files.lock)
            notify()
            return 0
        engine = Engine(
            app.make_source(),
            lease_timeout=_env_float(app.env("LEASE_TIMEOUT"), DEFAULT_LEASE_TIMEOUT),
            client_poll_ms=int(_env_float(app.env("CLIENT_POLL_MS"), DEFAULT_CLIENT_POLL_MS)),
        )
        idle_timeout = _env_float(app.env("IDLE_TIMEOUT"), DEFAULT_IDLE_TIMEOUT)
        asyncio.run(serve(app, files, engine, idle_timeout, notify))
    except Exception:
        log.exception("the daemon failed")
        return 1
    finally:
        # Still not ready here means the daemon failed before it listened: EOF tells the launcher so.
        notify(ready=False)
        lock.release()
    return 0


def ping(files: DaemonFiles, timeout: float = 2.0) -> dict[str, Any] | None:
    try:
        response = request(files, {"op": "hello"}, timeout=timeout)
    except (DaemonUnavailable, ValueError):
        return None
    return response if response.get("ok") else None


def needs_drain(daemon: Mapping[str, Any], mine: Mapping[str, Any]) -> bool:
    """Whether a watcher of identity `mine` should replace a daemon that said `daemon` in its hello: its code differs
    and is newer. An older watcher keeps using a newer daemon, so two installs never take turns replacing each other."""
    if daemon.get("fingerprint") == mine.get("fingerprint"):
        return False
    try:
        return float(mine.get("code_mtime") or 0) > float(daemon.get("code_mtime") or 0)
    except (TypeError, ValueError):
        return False


def _rotate_log(files: DaemonFiles) -> None:
    with contextlib.suppress(OSError):
        if files.log.stat().st_size > MAX_LOG_BYTES:
            os.replace(files.log, files.log.with_suffix(".log.1"))


def spawn_daemon(app: WatchApp, files: DaemonFiles) -> subprocess.Popen[bytes]:
    """Start a detached daemon process with its output in the log file."""
    ensure_private_dir(files.state_dir)
    _rotate_log(files)
    with open(files.log, "ab") as log_file:
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": log_file,
            "stderr": log_file,
            "cwd": str(files.state_dir),
            "close_fds": True,
        }
        command = [sys.executable, "-m", app.daemon_module]
        if sys.platform == "win32":
            flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
            try:
                # Leave the caller's job object, so killing the caller's job does not kill the daemon. A job that
                # forbids breakaway makes this fail; the daemon then shares the caller's fate.
                return subprocess.Popen(command, creationflags=flags | subprocess.CREATE_BREAKAWAY_FROM_JOB, **kwargs)
            except OSError:
                return subprocess.Popen(command, creationflags=flags, **kwargs)
        # The launcher forks the daemon off and exits once it is ready; see the module docstring.
        return subprocess.Popen(command, start_new_session=True, **kwargs)


def _wait_stopped(files: DaemonFiles, deadline: float) -> None:
    while daemon_running(files):
        if time.monotonic() >= deadline:
            raise DaemonUnavailable(f"the old daemon did not stop within the start timeout; see {files.log}")
        time.sleep(0.05)


def ensure_daemon(
    app: WatchApp, files: DaemonFiles | None = None, timeout: float = SPAWN_TIMEOUT, *, drain: bool = True
) -> dict[str, Any]:
    """Return the hello response of a running daemon with code at least as new as ours, starting one first if needed.

    With `drain`, a daemon running older code is asked to stop and is replaced.
    """
    files = files or DaemonFiles.for_app(app)
    mine = app.identity()
    hello = ping(files)
    if hello is not None and not (drain and needs_drain(hello, mine)):
        return hello
    files.check_socket_path()
    with FileLock(files.spawn_lock) as spawn_lock:
        spawn_lock.acquire(timeout)
        deadline = time.monotonic() + timeout
        hello = ping(files)
        if hello is not None and drain and needs_drain(hello, mine):
            with contextlib.suppress(DaemonUnavailable, ValueError):
                request(files, {"op": "drain", **mine}, timeout=2.0)
            _wait_stopped(files, deadline)
        spawned: subprocess.Popen[bytes] | None = None
        while time.monotonic() < deadline:
            hello = ping(files, timeout=1.0)
            if hello is not None:
                return hello
            if spawned is not None and spawned.poll() not in (None, 0) and not daemon_running(files):
                # The daemon died before it listened. Starting it again would fail the same way.
                raise DaemonStartFailed(f"the daemon failed to start: {_last_log_line(files)}; see {files.log}")
            # A daemon that is exiting on idle releases its lock last; spawn again once it is gone.
            if (spawned is None or spawned.poll() is not None) and not daemon_running(files):
                # Nothing holds the daemon lock, so no daemon is alive: any endpoint file left is stale.
                for stale in (files.socket, files.endpoint):
                    with contextlib.suppress(OSError):
                        stale.unlink()
                spawned = spawn_daemon(app, files)
            time.sleep(0.05)
    raise DaemonUnavailable(f"the daemon did not start within {timeout:.0f} s; see {files.log}")


def _last_log_line(files: DaemonFiles) -> str:
    try:
        lines = files.log.read_text(encoding="utf-8", errors="replace").strip().splitlines()
    except OSError:
        return "no log"
    return lines[-1] if lines else "an empty log"


def daemon_running(files: DaemonFiles) -> bool:
    """Whether a daemon process holds the lock, answering or not."""
    probe = FileLock(files.lock)
    if probe.try_acquire():
        probe.release()
        return False
    return True


def stop_daemon(files: DaemonFiles, timeout: float = 10.0) -> bool:
    """Ask the daemon to stop and wait until it released its lock. Return whether one was running."""
    if not daemon_running(files):
        return False
    with contextlib.suppress(DaemonUnavailable, ValueError):
        request(files, {"op": "stop"}, timeout=2.0)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not daemon_running(files):
            return True
        time.sleep(0.05)
    raise DaemonUnavailable(f"the daemon did not stop within {timeout:.0f} s; see {files.log}")
