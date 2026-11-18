"""Filesystem locations of one app: the base dir its watchers write their files under, and the daemon state dir.

Each location can be overridden by an environment variable named after the app's prefix (`<PREFIX>BASE_DIR`,
`<PREFIX>STATE_DIR`), which also keeps tests inside temp dirs.
"""

from __future__ import annotations

import os
import re
import stat
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
    """Create `path` and make it readable by the user only.

    A dir in a shared place such as `/tmp` may have been created by someone else first, or be a symlink to their dir;
    such a dir is refused, because the daemon's socket and log, and the watchers' files, would then be theirs to read
    or replace.
    """
    path.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        # Ours, then made private: the mode a fresh dir gets from the umask is no reason to refuse it.
        check_owned(path, directory=True, modes=False)
        os.chmod(path, 0o700)
    return path


def check_owned(path: Path, *, directory: bool = False, private: bool = False, modes: bool = True) -> None:
    """Raise `PermissionError` unless `path` belongs to this user, is no symlink, and (with `modes`) nobody else may
    write it, or with `private` read it either. Does nothing on Windows, where the profile dir protects the files."""
    if os.name == "nt":
        return
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or (directory and not stat.S_ISDIR(info.st_mode)):
        raise PermissionError(f"{path} is a symlink or not a {'dir' if directory else 'file'}; refusing to use it")
    if info.st_uid != os.getuid():
        raise PermissionError(f"{path} belongs to another user; refusing to use it")
    if modes and info.st_mode & (0o077 if private else 0o022):
        raise PermissionError(
            f"{path} is open to other users (mode {stat.S_IMODE(info.st_mode):o}); refusing to use it"
        )


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
