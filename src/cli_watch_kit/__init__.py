"""Build a `watch` command for a CLI: poll a source, report changes as events, share one daemon between watchers."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("cli-watch-kit")
except PackageNotFoundError:  # pragma: no cover - only when run from a source tree that was never installed
    __version__ = "0.0.0"

__all__ = ["__version__"]
