from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from cli_watch_kit.atomic import atomic_write_json
from cli_watch_kit.locks import FileLock, LockTimeout
from cli_watch_kit.paths import AppPaths, sanitize_part

HOLDER = """
import sys, time
from pathlib import Path
from cli_watch_kit.locks import FileLock
lock = FileLock(Path(sys.argv[1]))
assert lock.try_acquire()
print("held", flush=True)
sys.stdin.readline()
"""


def test_a_second_holder_is_refused_until_release(tmp_path: Path) -> None:
    path = tmp_path / "daemon.lock"
    first, second = FileLock(path), FileLock(path)
    assert first.try_acquire()
    assert not second.try_acquire()
    with pytest.raises(LockTimeout):
        second.acquire(timeout=0.2)
    first.release()
    assert second.try_acquire()
    second.release()


def test_the_os_releases_the_lock_when_the_holder_dies(tmp_path: Path) -> None:
    path = tmp_path / "daemon.lock"
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert holder.stdout is not None and holder.stdout.readline().strip() == "held"
    lock = FileLock(path)
    assert not lock.try_acquire()
    holder.kill()
    holder.wait(timeout=10)
    lock.acquire(timeout=5)
    lock.release()


def test_atomic_writes_replace_the_whole_file(tmp_path: Path) -> None:
    target = tmp_path / "sub" / "state.json"
    atomic_write_json(target, {"a": 1})
    atomic_write_json(target, {"b": "ž"})
    assert json.loads(target.read_text(encoding="utf-8")) == {"b": "ž"}
    assert [p.name for p in target.parent.iterdir()] == ["state.json"]


def test_app_paths_follow_their_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = AppPaths("demo-watch", "DEMO_WATCH_")
    monkeypatch.setenv("DEMO_WATCH_STATE_DIR", str(tmp_path / "s"))
    monkeypatch.delenv("DEMO_WATCH_BASE_DIR", raising=False)
    assert paths.state_dir() == tmp_path / "s"
    assert paths.base_dir(scratchpad=tmp_path / "pad") == tmp_path / "pad" / "demo-watch"
    assert paths.base_dir(tmp_path / "o", tmp_path / "pad") == tmp_path / "o"
    monkeypatch.setenv("DEMO_WATCH_BASE_DIR", str(tmp_path / "b"))
    assert paths.base_dir(scratchpad=tmp_path / "pad") == tmp_path / "b"


def test_sanitize_part_keeps_a_path_component_safe() -> None:
    assert sanitize_part("../a/b c") == "-a-b-c"
    assert sanitize_part("..") == "unknown"
