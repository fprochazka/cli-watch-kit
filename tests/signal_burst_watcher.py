"""A watcher that gets a second SIGTERM at every step of handling the first one. Run as a script by test_watcher.

`timeout` sends its signal twice, to the command and then to the command's process group, so the second one can
arrive while the handler of the first still runs. Python then runs the handler again, nested inside itself. Here a
profile function raises the signal at every call and return the outermost handler makes, which covers every point
where the handler could hold a lock that the nested one needs. A handler that deadlocks keeps this process alive
until faulthandler ends it with a traceback.
"""

from __future__ import annotations

import faulthandler
import io
import signal
import sys
from collections.abc import Mapping
from pathlib import Path
from types import CodeType, FrameType
from typing import Any

from cli_watch_kit.changelog import Changelog
from cli_watch_kit.client import Watcher, WatchSettings
from cli_watch_kit.sinks import StatusOutput, StdoutSink
from cli_watch_kit.source import Params, Target

TARGET = Target("item", "a")


class BurstBackend:
    """Its first poll, which runs with the watcher's signal handlers installed, starts the burst."""

    def __init__(self) -> None:
        self.nested = 0
        self._handler: CodeType | None = None

    def _depth(self, frame: FrameType | None) -> int:
        depth = 0
        while frame is not None:
            depth += frame.f_code is self._handler
            frame = frame.f_back
        return depth

    def _profile(self, frame: FrameType, event: str, arg: Any) -> None:
        if self._depth(frame) == 1:
            self.nested += 1
            signal.raise_signal(signal.SIGTERM)

    def describe(self) -> str:
        return "burst"

    def subscribe(
        self,
        client_id: str,
        target: Target,
        events: frozenset[str],
        baseline: Any,
        params: Params,
        source_options: Mapping[str, Any],
    ) -> tuple[str, int]:
        return "sub-1", 0

    def poll(self, client_id: str, cursors: dict[str, int], acks: dict[str, int]) -> dict[str, Any]:
        if self._handler is None:
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            self._handler = handler.__code__
            sys.setprofile(self._profile)
            signal.raise_signal(signal.SIGTERM)
        return {"next_poll_ms": 60_000}

    def unsubscribe(self, client_id: str, subscription_id: str) -> None:
        pass

    def call(self, op: str, payload: Mapping[str, Any], params: Params | None = None) -> dict[str, Any]:
        raise AssertionError("no op expected")

    def begin_shutdown(self) -> None:
        pass

    def close(self) -> None:
        pass


def main() -> None:
    faulthandler.dump_traceback_later(float(sys.argv[2]), exit=True)
    backend = BurstBackend()
    watcher = Watcher(
        WatchSettings(TARGET, frozenset({"watch_ended"})),
        backend,
        StdoutSink(),
        Changelog(Path(sys.argv[1]), TARGET.key),
        StatusOutput("burst: ", io.StringIO()),
    )
    code = watcher.run()
    sys.setprofile(None)
    print(f"nested {backend.nested}", file=sys.stderr)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
