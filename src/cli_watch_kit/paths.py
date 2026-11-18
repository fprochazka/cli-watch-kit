"""Filesystem locations of one app: the base dir its watchers write their files under, and the daemon state dir.

Each location can be overridden by an environment variable named after the app's prefix (`<PREFIX>BASE_DIR`,
`<PREFIX>STATE_DIR`), which also keeps tests inside temp dirs.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


def sanitize_part(value: str) -> str:
    """Make one path component safe: no separators, no traversal, no characters Windows rejects."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "-", value).strip(".")
    cleaned = cleaned.replace("..", "-")
    return cleaned or "unknown"


def ensure_private_dir(path: Path) -> Path:
    """Create `path` and make it readable by the user only."""
    path.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        os.chmod(path, 0o700)
    return path


def uid_suffix() -> str:
    getuid = getattr(os, "getuid", None)
    return str(getuid()) if getuid is not None else (os.environ.get("USERNAME") or "user")


@dataclass(frozen=True)
class AppPaths:
    """Where an app named `name` keeps its files. `env_prefix` is the prefix of its override variables."""

    name: str
    env_prefix: str

    @property
    def base_dir_env(self) -> str:
        return f"{self.env_prefix}BASE_DIR"

    @property
    def state_dir_env(self) -> str:
        return f"{self.env_prefix}STATE_DIR"

    def base_dir(self, override: Path | None = None, scratchpad: Path | None = None) -> Path:
        """The dir the watchers write their resource dirs under.

        `override` wins, then `<PREFIX>BASE_DIR`. Under a Claude Code session with a scratchpad it is
        `<scratchpad>/<name>`, which Claude Code removes after a while. Otherwise `<temp dir>/<name>-<uid>`, private to
        the user, which the OS cleans.
        """
        if override is not None:
            return override
        env = os.environ.get(self.base_dir_env)
        if env:
            return Path(env)
        if scratchpad is not None:
            return scratchpad / self.name
        return ensure_private_dir(Path(tempfile.gettempdir()) / f"{self.name}-{uid_suffix()}")

    def state_dir(self) -> Path:
        """The per-user dir holding the daemon's lock, socket (or port file) and log."""
        override = os.environ.get(self.state_dir_env)
        if override:
            return Path(override)
        if sys.platform == "win32":
            base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
            return base / self.name / "run"
        if sys.platform == "darwin":
            tmpdir = os.environ.get("TMPDIR")
            if tmpdir:
                return Path(tmpdir) / self.name
        else:
            runtime = os.environ.get("XDG_RUNTIME_DIR")
            if runtime and Path(runtime).is_dir():
                return Path(runtime) / self.name
        return Path(tempfile.gettempdir()) / f"{self.name}-{uid_suffix()}"
