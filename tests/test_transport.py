"""The transport's errors: a daemon that is gone, one that is slow, and one that cuts its answer short."""

from __future__ import annotations

import json
import os
import shutil
import socket
import stat
import tempfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from cli_watch_kit import client as client_module
from cli_watch_kit.client import DaemonBackend
from cli_watch_kit.daemon.transport import DaemonFiles, DaemonNotTrusted, DaemonTimeout, DaemonUnavailable, request
from cli_watch_kit.paths import ensure_private_dir
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


def test_no_secret_goes_to_a_daemon_whose_dir_others_may_write(
    files: DaemonFiles, monkeypatch: pytest.MonkeyPatch
) -> None:
    daemon = FakeDaemon(files, lambda conn: conn.sendall(b'{"ok": true}\n'))
    monkeypatch.setattr(client_module, "ensure_daemon", lambda app, f: {"pid": 1})
    try:
        backend = DaemonBackend(APP, files)
        os.chmod(files.state_dir, 0o700)
        assert backend._call({"op": "echo", "params": {"token": "t"}})["ok"]
        assert daemon.connections == 1
        os.chmod(files.state_dir, 0o770)
        with pytest.raises(DaemonNotTrusted, match="open to other users"):
            backend._call({"op": "echo", "params": {"token": "t"}})
        assert daemon.connections == 1
    finally:
        daemon.close()


def test_a_stale_tcp_endpoint_gets_no_token(files: DaemonFiles, monkeypatch: pytest.MonkeyPatch) -> None:
    tcp = DaemonFiles(files.state_dir, "daemon", "CWK_TEST_TRANSPORT")
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(0.5)
    tcp.state_dir.mkdir(parents=True)
    port = listener.getsockname()[1]
    tcp.endpoint.write_text(json.dumps({"port": port, "token": "secret", "pid": 1}))
    monkeypatch.setenv("CWK_TEST_TRANSPORT", "tcp")
    try:
        with pytest.raises(DaemonUnavailable, match="stale"):
            request(tcp, {"op": "hello"}, timeout=1)
        with pytest.raises(TimeoutError):
            listener.accept()
    finally:
        listener.close()


def test_a_dir_that_is_a_symlink_is_refused(tmp_path: Path) -> None:
    (tmp_path / "theirs").mkdir()
    (tmp_path / "mine").symlink_to(tmp_path / "theirs")
    with pytest.raises(PermissionError, match="symlink"):
        ensure_private_dir(tmp_path / "mine")


def test_a_fresh_dir_is_made_private_whatever_the_umask(tmp_path: Path) -> None:
    previous = os.umask(0o002)
    try:
        made = ensure_private_dir(tmp_path / "fresh")
    finally:
        os.umask(previous)
    assert stat.S_IMODE(made.stat().st_mode) == 0o700
