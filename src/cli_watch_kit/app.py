"""`WatchApp`: everything the library needs to know about one CLI's watch command."""

from __future__ import annotations

import hashlib
import importlib.util
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any

from cli_watch_kit import __version__ as LIB_VERSION
from cli_watch_kit.paths import AppPaths
from cli_watch_kit.source import Source

LIB_PROTOCOL = 1
"""The wire protocol between the library's watchers and daemons. Bumped when a daemon of the previous version could
not serve a watcher of this one. It is part of the socket name, so daemons of two protocols never meet."""


@dataclass(frozen=True)
class WatchApp:
    """One CLI's watch command.

    - `name`: the app's name in paths, the daemon log and the Claude Code sender (`linear-watch`).
    - `source`: the :class:`Source`, or a function that makes it, so a CLI does not build it before it is needed.
    - `daemon_module`: the module that `python -m` runs as the daemon; it calls :func:`cli_watch_kit.daemon.main`.
    - `app_protocol`: the app's own wire version, for what the source sends (snapshot fields, event kinds). Bump it
      when an older daemon could not serve a newer watcher; it is part of the socket name.
    - `version`: the app's version, shown in the status and compared in the handshake.
    - `status_prefix`: the start of every status line on stderr (`linear watch: `).
    - `envelope_tag`: the tag of a Claude Code message (`linear-watcher`).
    - `command`: how a user runs the watch command, for hints (`linear watch`). Default: `name`.
    - `env_prefix`: the prefix of the override variables. Default: `name` in upper case with `_`, plus `_`.
    """

    name: str
    source: Source[Any] | Callable[[], Source[Any]]
    daemon_module: str
    app_protocol: int
    version: str
    status_prefix: str
    envelope_tag: str
    command: str = ""
    env_prefix: str = ""
    _made: dict[str, Source[Any]] = field(default_factory=dict, init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.command:
            object.__setattr__(self, "command", self.name)
        if not self.env_prefix:
            object.__setattr__(self, "env_prefix", self.name.upper().replace("-", "_") + "_")

    def make_source(self) -> Source[Any]:
        """The source, built once."""
        if isinstance(self.source, Source):
            return self.source
        made = self._made.get("source")
        if made is None:
            made = self.source()
            self._made["source"] = made
        return made

    @property
    def paths(self) -> AppPaths:
        return AppPaths(self.name, self.env_prefix)

    def env(self, name: str) -> str:
        """The name of one of the app's environment variables."""
        return f"{self.env_prefix}{name}"

    @property
    def lib_version(self) -> str:
        return LIB_VERSION

    @cached_property
    def code(self) -> CodeStamp:
        """What code this process runs: the library's and the app's, for the version handshake."""
        top = self.daemon_module.split(".", 1)[0]
        return CodeStamp.of(["cli_watch_kit", top])

    def identity(self) -> dict[str, Any]:
        """The versions and the code stamp, as the daemon's hello reports them and a watcher compares them."""
        return {
            "app": self.name,
            "lib_protocol": LIB_PROTOCOL,
            "app_protocol": self.app_protocol,
            "lib_version": LIB_VERSION,
            "app_version": self.version,
            "fingerprint": self.code.fingerprint,
            "code_mtime": self.code.mtime,
        }


@dataclass(frozen=True)
class CodeStamp:
    """A fingerprint of the installed code of some packages and the newest modification time among their files.

    An editable install changes code without changing any version, so the version alone cannot tell a watcher that
    the daemon runs older code. The fingerprint can; the modification time says which side is newer.
    """

    fingerprint: str
    mtime: float

    @classmethod
    def of(cls, packages: list[str]) -> CodeStamp:
        digest = hashlib.sha256()
        newest = 0.0
        for package in sorted(set(packages)):
            spec = importlib.util.find_spec(package)
            locations = list(spec.submodule_search_locations or []) if spec is not None else []
            if spec is not None and not locations and spec.origin:
                locations = [str(Path(spec.origin).parent)]
            for location in locations:
                for root, dirs, files in os.walk(location):
                    dirs[:] = sorted(d for d in dirs if d != "__pycache__")
                    for name in sorted(files):
                        if not name.endswith(".py"):
                            continue
                        path = Path(root, name)
                        try:
                            stat = path.stat()
                        except OSError:
                            continue
                        rel = path.relative_to(location).as_posix()
                        digest.update(f"{package}/{rel}:{stat.st_size}:{stat.st_mtime_ns}\n".encode())
                        newest = max(newest, stat.st_mtime)
        return cls(digest.hexdigest()[:16], newest)
