"""The claude-message stream through cc-blackbox, against a real Unix socket that reads frames the way a Claude Code
session does, and the fallback without the extra."""

from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
from collections.abc import Iterator
from dataclasses import replace
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

pytest.importorskip("cc_blackbox")

from cc_blackbox import MessageEventSink  # noqa: E402

from cli_watch_kit.app import WatchApp  # noqa: E402
from cli_watch_kit.claude import StreamError, claude_message_sink, message_envelope, open_stream  # noqa: E402
from cli_watch_kit.events import Event  # noqa: E402
from cli_watch_kit.sinks import ResourceSinks, StatusOutput, StdoutSink  # noqa: E402
from conftest import Clock  # noqa: E402
from fakeapp.source import FakeSource  # noqa: E402

pytestmark = pytest.mark.skipif(os.name == "nt", reason="Claude Code messaging has no Windows transport")

APP = WatchApp("demo-watch", FakeSource(), "unused", 1, "0", "demo watch: ", "demo-watcher", command="demo watch")
RESOURCE = "item a"


class Receiver:
    """Accepts connections on a Unix socket and keeps every frame it reads."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.frames: list[dict[str, Any]] = []
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(path))
        self._server.listen()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            with conn:
                data = b""
                while chunk := conn.recv(65536):
                    data += chunk
                for line in data.splitlines():
                    if line.strip():
                        self.frames.append(json.loads(line))

    def close(self) -> None:
        self._server.close()
        self.path.unlink(missing_ok=True)


@pytest.fixture
def receiver() -> Iterator[Receiver]:
    directory = Path(tempfile.mkdtemp(prefix="cwk-sock-"))
    rx = Receiver(directory / "s.sock")
    yield rx
    rx.close()
    directory.rmdir()


def fake_session(socket_path: Path, scratchpad: Path | None = None) -> Any:
    return SimpleNamespace(
        name="fix-the-login-form",
        address="personal/fix-the-login-form",
        messaging_socket=socket_path,
        can_receive_messages=True,
        scratchpad_dir=scratchpad,
        compatibility=SimpleNamespace(compatible=True, reasons=()),
    )


def event(kind: str, seq: int, **data: Any) -> Event:
    return Event(kind, RESOURCE, seq, "2026-11-09T10:00:00.000Z", data)


def wait_frames(rx: Receiver, count: int) -> list[dict[str, Any]]:
    for _ in range(200):
        if len(rx.frames) >= count:
            break
        threading.Event().wait(0.01)
    return rx.frames


def sink_for(receiver: Receiver, clock: Clock, changelog: Path, priority: str = "next") -> Any:
    status = StatusOutput(APP.status_prefix)
    session = cast(Any, fake_session(receiver.path))
    return claude_message_sink(APP, session, RESOURCE, "demo watch item a", changelog, priority, status, clock=clock)


def test_a_burst_goes_out_as_one_message_in_the_apps_envelope(receiver: Receiver, clock: Clock, tmp_path: Path) -> None:
    sink = sink_for(receiver, clock, tmp_path / "events.jsonl")
    assert "priority next" in sink.describe() and "'demo watch item a'" in sink.describe()
    sink.emit([event("status_changed", 3, to="review")])
    sink.emit([event("note_added", 4, notes=1)])
    sink.tick()
    assert receiver.frames == []
    clock.advance(3)
    sink.tick()
    frames = wait_frames(receiver, 1)
    assert len(frames) == 1
    assert frames[0]["from"] == f"demo-watch:{os.getpid()}"
    content = frames[0]["message"]["content"]
    assert f'<demo-watcher resource="{RESOURCE}" seq="3-4">' in content
    assert '{"kind":"note_added","seq":4,"data":{"notes":1}}' in content


def test_an_app_can_choose_the_sender_id(receiver: Receiver, clock: Clock, tmp_path: Path) -> None:
    app = replace(APP, sender="demo")
    session = cast(Any, fake_session(receiver.path))
    status = StatusOutput(app.status_prefix)
    sink = claude_message_sink(app, session, RESOURCE, "demo watch item a", tmp_path / "e.jsonl", "next", status)
    sink.emit([event("watch_ended", 1, reason="done")])
    assert wait_frames(receiver, 1)[0]["from"] == f"demo:{os.getpid()}"
    sink.close()


def test_only_the_first_watch_started_message_names_the_dir_and_the_changelog(
    receiver: Receiver, clock: Clock, tmp_path: Path
) -> None:
    changelog = tmp_path / "res" / "events.jsonl"
    sink = sink_for(receiver, clock, changelog)
    started = event("watch_started", 1, summary={"status": "open", "dir": str(tmp_path / "res")})
    for batch in ([started, event("note_added", 2)], [event("note_added", 3)], [event("watch_started", 4)]):
        sink.emit(batch)
        clock.advance(3)
        sink.tick()
    frames = wait_frames(receiver, 3)
    heads = [f["message"]["content"].split("<demo-watcher ", 1)[1].split(">", 1)[0] for f in frames]
    assert '"summary":{"status":"open"}' in frames[0]["message"]["content"]
    assert heads == [
        f'resource="{RESOURCE}" seq="1-2" dir="{tmp_path / "res"}" changelog="{changelog}"',
        f'resource="{RESOURCE}" seq="3"',
        f'resource="{RESOURCE}" seq="4"',
    ]
    sink.close()


def test_an_app_can_rewrite_the_announcing_watch_started(receiver: Receiver, clock: Clock, tmp_path: Path) -> None:
    def relative_files(event: Event, directory: Path) -> Event:
        files = Path(event.data["summary"]["files"]).relative_to(directory)
        return replace(event, data={**event.data, "summary": {**event.data["summary"], "files": str(files)}})

    app = replace(APP, announced=relative_files)
    changelog = tmp_path / "res" / "events.jsonl"
    envelope = message_envelope(app, RESOURCE, changelog)
    summary = {"dir": str(tmp_path / "res"), "files": str(tmp_path / "res" / "files")}
    content = envelope.format([event("watch_started", 1, summary=summary), event("note_added", 2)], announce=True)
    assert '"summary":{"files":"files"}' in content
    assert '"summary":{"files":"files"}' not in message_envelope(APP, RESOURCE, changelog).format(
        [event("watch_started", 1, summary=summary)], announce=True
    )


def test_watch_ended_is_sent_at_once_and_priority_is_passed(receiver: Receiver, clock: Clock, tmp_path: Path) -> None:
    sink = sink_for(receiver, clock, tmp_path / "e.jsonl", "later")
    sink.emit([event("status_changed", 9, to="done"), event("watch_ended", 10, reason="done")])
    frames = wait_frames(receiver, 1)
    assert frames[0]["priority"] == "later" and 'seq="9-10"' in frames[0]["message"]["content"]
    sink.close()


def test_a_gone_session_ends_the_watch(receiver: Receiver, clock: Clock, tmp_path: Path) -> None:
    sink = sink_for(receiver, clock, tmp_path / "e.jsonl")
    receiver.close()
    sink.emit([event("note_added", 1)])
    clock.advance(3)
    sink.tick()
    assert not sink.alive()


def test_oversized_batches_become_a_summary(tmp_path: Path) -> None:
    events = [event("note_added", i, body="x" * 2000) for i in range(1, 60)]
    envelope = message_envelope(APP, RESOURCE, tmp_path / "events.jsonl")
    content = envelope.format(events)
    assert len(content.encode()) < envelope.max_bytes == 64 * 1024
    assert '"truncated":true' in content and '"note_added":59' in content


def test_stream_selection(receiver: Receiver, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    status = StatusOutput(APP.status_prefix)
    common: dict[str, Any] = {
        "resource": RESOURCE,
        "sender_name": "demo watch item a",
        "changelog": tmp_path / "e.jsonl",
        "status": status,
    }
    plain = open_stream(APP, "auto", session=None, priority="next", **common)
    assert isinstance(plain.sink, StdoutSink) and plain.fallback is None
    with pytest.raises(StreamError, match="no Claude Code session found"):
        open_stream(APP, "claude-message", session=None, priority="next", **common)
    chosen = open_stream(APP, "auto", session=fake_session(receiver.path), priority="now", **common)
    assert isinstance(chosen.sink, MessageEventSink) and chosen.sink.priority == "now"
    forced = open_stream(APP, "stdout", session=fake_session(receiver.path), priority="next", **common)
    assert isinstance(forced.sink, StdoutSink)
    with pytest.raises(StreamError, match="--message-priority"):
        open_stream(APP, "auto", session=None, priority="whenever", **common)


def test_an_incompatible_session_falls_back_to_stdout_with_a_reason(receiver: Receiver, tmp_path: Path) -> None:
    session = fake_session(receiver.path)
    session.can_receive_messages = False
    reason = "messaging: the record speaks peer protocol 2, and this cc-blackbox version speaks 1."
    session.compatibility = SimpleNamespace(compatible=False, reasons=(reason,))
    common: dict[str, Any] = {
        "resource": RESOURCE,
        "sender_name": "x",
        "changelog": tmp_path / "e.jsonl",
        "priority": "next",
        "status": StatusOutput(""),
    }
    chosen = open_stream(APP, "auto", session=session, **common)
    assert isinstance(chosen.sink, StdoutSink)
    assert chosen.fallback is not None and "peer protocol 2" in chosen.fallback
    with pytest.raises(StreamError, match="peer protocol 2"):
        open_stream(APP, "claude-message", session=session, **common)


def test_stdout_sink_writes_one_line_per_event() -> None:
    out = StringIO()
    StdoutSink(out).emit([event("note_added", 1, notes=1)])
    assert json.loads(out.getvalue())["seq"] == 1


def test_a_watch_of_several_targets_messages_each_resource_in_its_own_envelope(
    receiver: Receiver, tmp_path: Path
) -> None:
    status = StatusOutput(APP.status_prefix)
    stream = open_stream(
        APP,
        "claude-message",
        session=fake_session(receiver.path),
        resource="item a",
        sender_name="demo watch item a b",
        changelog=tmp_path / "a" / "events.jsonl",
        priority="next",
        status=status,
        more={"item b": tmp_path / "b" / "events.jsonl"},
    )
    sink = stream.sink
    assert isinstance(sink, ResourceSinks) and "'demo watch item a b'" in sink.describe()
    first = Event("watch_ended", "item a", 3, "2026-11-09T10:00:00.000Z", {"reason": "done"})
    second = Event("watch_ended", "item b", 7, "2026-11-09T10:00:00.000Z", {"reason": "done"})
    sink.emit([first, second])
    frames = wait_frames(receiver, 2)
    contents = sorted(f["message"]["content"] for f in frames)
    assert '<demo-watcher resource="item a" seq="3">' in contents[0]
    assert '<demo-watcher resource="item b" seq="7">' in contents[1]
    assert {f["from"] for f in frames} == {f"demo-watch:{os.getpid()}"}
    resource_sinks = cast(Any, sink).sinks
    assert resource_sinks["item a"].rate_limiter is resource_sinks["item b"].rate_limiter
    sink.close()
