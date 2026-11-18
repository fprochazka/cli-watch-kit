from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


class Clock:
    """A wall clock the test moves by hand."""

    def __init__(self, now: float = 1_794_220_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture(autouse=True)
def _isolated_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """No test may touch a real daemon, base dir or Claude Code session."""
    monkeypatch.setenv("FAKEAPP_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("FAKEAPP_BASE_DIR", str(tmp_path / "base"))
    for name in ("CLAUDECODE", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_PID", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(name, raising=False)
    yield
