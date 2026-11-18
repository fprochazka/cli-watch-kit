"""The stdout fallback of the Claude Code stream, which must work without cc-blackbox installed."""

from __future__ import annotations

import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from cli_watch_kit import claude
from cli_watch_kit.app import WatchApp
from cli_watch_kit.claude import StreamError, open_stream
from cli_watch_kit.sinks import StatusOutput, StdoutSink
from fakeapp.source import FakeSource

APP = WatchApp("demo-watch", FakeSource(), "unused", 1, "0", "demo watch: ", "demo-watcher", command="demo watch")
RESOURCE = "item a"


def test_without_the_extra_auto_uses_stdout_and_names_the_extra(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(claude, "claude_code_problem", lambda: claude.INSTALL_HINT)
    common: dict[str, Any] = {
        "session": None,
        "resource": RESOURCE,
        "sender_name": "x",
        "changelog": tmp_path / "e.jsonl",
        "priority": "next",
        "status": StatusOutput(""),
    }
    assert claude.find_session() is None
    assert open_stream(APP, "auto", **common).fallback is None
    monkeypatch.setenv("CLAUDECODE", "1")
    chosen = open_stream(APP, "auto", **common)
    assert isinstance(chosen.sink, StdoutSink)
    assert chosen.fallback is not None and "claude-code extra" in chosen.fallback
    with pytest.raises(StreamError, match="claude-code extra"):
        open_stream(APP, "claude-message", **common)


def test_the_package_imports_no_cc_blackbox_until_a_stream_needs_it() -> None:
    code = (
        "import sys, cli_watch_kit.cli, cli_watch_kit.client, cli_watch_kit.daemon.lifecycle;"
        " print('cc_blackbox' in sys.modules)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60, check=True)
    assert result.stdout.strip() == "False"


def test_an_old_cc_blackbox_falls_back_to_stdout_and_names_the_version_needed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A cc-blackbox without the event stream API, as 0.6 was.
    monkeypatch.setitem(sys.modules, "cc_blackbox", types.ModuleType("cc_blackbox"))
    monkeypatch.setenv("CLAUDECODE", "1")
    problem = claude.claude_code_problem()
    assert problem is not None and "does not fit" in problem and "cc-blackbox>=0.11,<0.13" in problem
    assert claude.find_session() is None
    common: dict[str, Any] = {
        "session": None,
        "resource": RESOURCE,
        "sender_name": "x",
        "changelog": tmp_path / "e.jsonl",
        "priority": "next",
        "status": StatusOutput(""),
    }
    chosen = open_stream(APP, "auto", **common)
    assert isinstance(chosen.sink, StdoutSink) and chosen.fallback == problem
