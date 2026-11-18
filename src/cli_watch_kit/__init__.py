"""Build a `watch` command for a CLI: poll a source, report changes as JSON events, share one daemon between watchers.

The public API, by import path:

- :mod:`cli_watch_kit.source`: :class:`Source`, :class:`Target`, :class:`PollItem`, :class:`PollOutcome`,
  :class:`PollContext`, the errors :class:`NotFound`, :class:`Unauthorized`, :class:`RateLimited`, :class:`Transient`.
- :mod:`cli_watch_kit.app`: :class:`WatchApp`.
- :mod:`cli_watch_kit.events`: :class:`Event`, :class:`RawEvent`, :class:`KindCatalog`, the lifecycle kinds.
- :mod:`cli_watch_kit.budget`: :class:`DimensionSpec`, :class:`Budget`.
- :mod:`cli_watch_kit.cli`: the click helpers :func:`watch_options`, :func:`run_watch`, :func:`daemon_group`.
- :mod:`cli_watch_kit.daemon`: :func:`main`, the body of an app's daemon module.
- :mod:`cli_watch_kit.client`: :class:`Watcher` and the backends, for a CLI that does not use click.
- :mod:`cli_watch_kit.claude`: the Claude Code stream, from the `claude-code` extra.

Importing this package imports nothing but the standard library.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("cli-watch-kit")
except PackageNotFoundError:  # pragma: no cover - only when run from a source tree that was never installed
    __version__ = "0.0.0"

__all__ = ["__version__"]
