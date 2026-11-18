"""The transport's errors: a daemon that is gone, one that is slow, and one that cuts its answer short."""

from __future__ import annotations

import os
import shutil
import socket
import tempfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from cli_watch_kit import client as client_module
from cli_watch_kit.client import DaemonBackend
from cli_watch_kit.daemon.transport import DaemonFiles, DaemonTimeout, DaemonUnavailable, request
from fakeapp.app import APP

pytestmark = pytest.mark.skipif(os.name == "nt", reason="a Unix socket stands in for the daemon")


class FakeDaemon:
    """Accepts one connection at a time on the daemon's socket and answers it with `reply`."""

    def __init__(self, files: DaemonFiles, reply: Callable[[socket.socket], object]) -> None:
        files.state_dir.mkdir(parents=True, exist_ok=True)
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(files.socket))
        self.server.listen()
        self.reply = reply
        self.connections = 0
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.server.accept()
            except OSError:
                return
            self.connections += 1
            with conn:
                conn.recv(65536)
                self.reply(conn)

    def close(self) -> None:
        self.server.close()


@pytest.fixture
def files() -> Iterator[DaemonFiles]:
    root = Path(tempfile.mkdtemp(prefix="cwk-t-"))
    yield DaemonFiles(root / "s", "daemon")
    shutil.rmtree(root, ignore_errors=True)


def test_nothing_listening_is_unavailable(files: DaemonFiles) -> None:
    with pytest.raises(DaemonUnavailable):
        request(files, {"op": "hello"}, timeout=1)


def test_a_silent_daemon_is_a_timeout_not_a_gone_daemon(files: DaemonFiles) -> None:
    hold = threading.Event()
    daemon = FakeDaemon(files, lambda conn: hold.wait(5))
    try:
        with pytest.raises(DaemonTimeout, match="did not answer within"):
            request(files, {"op": "hello"}, timeout=0.3)
    finally:
        hold.set()
        daemon.close()


def test_an_answer_cut_short_is_treated_like_a_lost_connection(files: DaemonFiles) -> None:
    daemon = FakeDaemon(files, lambda conn: conn.sendall(b'{"ok": true, "pid'))
    try:
        with pytest.raises(DaemonUnavailable, match="cut off"):
            request(files, {"op": "hello"}, timeout=2)
    finally:
        daemon.close()


def test_the_backend_neither_respawns_nor_resends_on_a_timeout(
    files: DaemonFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    hold = threading.Event()
    daemon = FakeDaemon(files, lambda conn: hold.wait(5))
    spawns: list[DaemonFiles] = []

    def ensure_daemon(app: object, f: DaemonFiles) -> dict[str, int]:
        spawns.append(f)
        return {"pid": 1}

    monkeypatch.setattr(client_module, "ensure_daemon", ensure_daemon)
    backend = DaemonBackend(APP, files)
    try:
        with pytest.raises(DaemonTimeout):
            backend._call({"op": "echo"}, timeout=0.3)
        assert len(spawns) == 1 and daemon.connections == 1
    finally:
        hold.set()
        daemon.close()
