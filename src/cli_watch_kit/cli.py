"""click helpers: the common options of a watch command, the run of one watch, and the `daemon` command group.

A CLI declares its own watch commands (what to watch, its arguments) and hands the rest to :func:`run_watch`:

```python
@watch.command()
@click.argument("issue_id")
@watch_options
def issue(issue_id: str, options: WatchOptions) -> None:
    raise SystemExit(run_watch(APP, Target("issue", issue_id), options, params={"api_key": key()}))

watch.add_command(daemon_group(APP))
```

The core takes a plain :class:`WatchOptions`, so a CLI on another framework fills it from its own options.
"""

from __future__ import annotations

import functools
import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import click

from cli_watch_kit.app import WatchApp
from cli_watch_kit.changelog import Changelog
from cli_watch_kit.claude import MESSAGE_PRIORITIES, STREAM_MODES, StreamError, find_session, open_stream
from cli_watch_kit.client import Backend, Watcher, WatchSettings, Writer, WriterContext, default_backend
from cli_watch_kit.daemon.lifecycle import daemon_running, stop_daemon
from cli_watch_kit.daemon.transport import DaemonFiles, DaemonUnavailable, request
from cli_watch_kit.durations import format_duration, parse_duration
from cli_watch_kit.events import LIFECYCLE_KINDS, FilterError
from cli_watch_kit.paths import sanitize_part
from cli_watch_kit.sinks import Sink, StatusOutput
from cli_watch_kit.source import Target
from cli_watch_kit.timers import ClientTimer

EXIT_USAGE = 2


@dataclass(frozen=True)
class WatchOptions:
    """The options every watch command shares."""

    events: tuple[str, ...] = ()
    exclude_events: tuple[str, ...] = ()
    until: str | None = None
    timeout: str | None = None
    idle_after: str | None = None
    events_stream: str = "auto"
    message_priority: str = "next"
    base_dir: Path | None = None
    no_daemon: bool = False


_OPTION_NAMES = [f.name for f in fields(WatchOptions)]

_OPTIONS: list[Callable[[Any], Any]] = [
    click.option(
        "--events",
        "events",
        multiple=True,
        help="Report only these event kinds or groups, comma-separated or repeated.",
    ),
    click.option(
        "--exclude-events",
        "exclude_events",
        multiple=True,
        help="Do not report these event kinds or groups, comma-separated or repeated.",
    ),
    click.option("--until", help="Stop after the first event of this kind."),
    click.option("--timeout", help="Stop after this long, for example 90s, 30m, 2h, 1h30m."),
    click.option(
        "--idle-after",
        help="Stop when nothing was reported for this long, for example 3h. Every reported event starts the count "
        "again; threshold events do not.",
    ),
    click.option(
        "--events-stream",
        type=click.Choice(STREAM_MODES),
        default="auto",
        show_default=True,
        help="auto picks claude-message inside a Claude Code session that accepts messages, else stdout.",
    ),
    click.option(
        "--message-priority",
        type=click.Choice(MESSAGE_PRIORITIES),
        default="next",
        show_default=True,
        help="claude-message only: now or next deliver into the running turn, later waits until the session is idle.",
    ),
    click.option(
        "--base-dir",
        type=click.Path(file_okay=False, path_type=Path),
        help="Where the resource dirs go (events.jsonl, baseline.json). Default: <session scratchpad>/<app> under "
        "Claude Code, else <temp dir>/<app>-<uid>.",
    ),
    click.option(
        "--no-daemon", is_flag=True, help="Poll inside this process with a private budget, without the daemon."
    ),
]


def watch_options[F: Callable[..., Any]](func: F) -> F:
    """Add the common options to a click command; the command gets them as one `options: WatchOptions` argument."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        values = {name: kwargs.pop(name) for name in _OPTION_NAMES if name in kwargs}
        values["events"] = tuple(values.get("events") or ())
        values["exclude_events"] = tuple(values.get("exclude_events") or ())
        return func(*args, options=WatchOptions(**values), **kwargs)

    decorated: Any = wrapper
    for option in reversed(_OPTIONS):
        decorated = option(decorated)
    return decorated  # type: ignore[no-any-return]


class _Usage(Exception):
    def __init__(self, message: str, code: int = EXIT_USAGE) -> None:
        super().__init__(message)
        self.code = code


def _duration(option: str, value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = parse_duration(value)
    except ValueError as exc:
        raise _Usage(f"{option}: {exc}") from exc
    if seconds <= 0:
        raise _Usage(f"{option}: the duration must be longer than zero")
    return seconds


def default_resource_dir(base: Path, target: Target) -> Path:
    """`<base>/<kind>/<id>`, each part made safe for a path."""
    return base / sanitize_part(target.kind) / sanitize_part(target.id)


def run_watch(
    app: WatchApp,
    target: Target,
    options: WatchOptions,
    *,
    params: Mapping[str, Any] | None = None,
    source_options: Mapping[str, Any] | None = None,
    enabled_thresholds: Iterable[str] = (),
    also_subscribe: Iterable[str] = (),
    timers: Sequence[ClientTimer] = (),
    writer: Callable[[WriterContext], Writer] | None = None,
    resource_dir: Callable[[Path, Target], Path] = default_resource_dir,
    readme: str | None = None,
    sender_name: str | None = None,
    restart: str | None = None,
    header: Sequence[str] = (),
    status: StatusOutput | None = None,
) -> int:
    """Run one watch of `target` to its end and return the exit code: 0, 1 for an error, 2 for a usage error.

    - `params` go with the subscription to the daemon (an API key, for example), and so do `source_options`, which
      the source checks with `Source.check_options` and gets in `PollItem.options` (glab's `--logs` mode).
    - `enabled_thresholds` are the threshold kinds whose option was given; `timers` compute them. `also_subscribe`
      names kinds the daemon should track although the watcher does not report them, because a timer reads what
      they put into the snapshot (glab's `quiet` reads the head pipeline's status).
    - `writer` makes the app's writer of the resource dir from a :class:`WriterContext`.
    - `sender_name` is what Claude Code shows as the sender (default: `<command> <target key>`), `restart` the
      command line that watches the same target again (default: `<command> <kind> <id>`), `header` extra header lines.
    """
    status = status or StatusOutput(app.status_prefix)
    source = app.make_source()
    catalog = source.catalog
    try:
        selected = catalog.resolve(target.kind, options.events, options.exclude_events, enabled_thresholds)
        if options.until is not None and options.until not in catalog.produced(target.kind):
            raise _Usage(f"--until: a {target.kind} watch never produces {options.until!r}")
        if options.until is not None and options.until not in selected:
            raise _Usage(f"--until={options.until} is filtered out by --events/--exclude-events")
        extra = frozenset(also_subscribe)
        if extra - set(catalog.produced(target.kind)):
            unknown = ", ".join(sorted(extra - set(catalog.produced(target.kind))))
            raise _Usage(f"a {target.kind} watch never produces {unknown}, so it cannot also subscribe to it")
        timeout = _duration("--timeout", options.timeout)
        idle_after = _duration("--idle-after", options.idle_after)
        session = find_session() if options.events_stream != "stdout" else None
        scratchpad = getattr(session, "scratchpad_dir", None) if session is not None else None
        base = app.paths.base_dir(options.base_dir, scratchpad)
        changelog = Changelog(resource_dir(base, target), target.key, readme)
        stream = open_stream(
            app,
            options.events_stream,
            session=session,
            resource=target.key,
            sender_name=sender_name or f"{app.command} {target.key}",
            changelog=changelog.events_path,
            priority=options.message_priority,
            status=status,
        )
    except (FilterError, StreamError, _Usage) as exc:
        status.line(f"error: {exc}")
        return exc.code if isinstance(exc, _Usage) else EXIT_USAGE
    try:
        backend = default_backend(app, options.no_daemon)
    except (DaemonUnavailable, TimeoutError) as exc:
        status.line(f"error: cannot start the daemon: {exc}; retry with --no-daemon to poll without it")
        return 1
    thresholds = [d for t in timers if (d := t.describe())]
    print_header(
        status,
        app,
        target,
        stream.sink,
        changelog,
        selected,
        backend,
        until=options.until,
        timeout=options.timeout,
        idle_after=idle_after,
        fallback=stream.fallback,
        thresholds=thresholds,
        restart=restart or f"{app.command} {target.kind} {target.id}",
        extra=header,
    )
    settings = WatchSettings(
        target,
        selected,
        until=options.until,
        timeout=timeout,
        idle_after=idle_after,
        params=dict(params or {}),
        source_options=dict(source_options or {}),
        threshold_kinds=frozenset(catalog.thresholds),
        subscribed=catalog.daemon_kinds(selected) | catalog.daemon_kinds(extra),
    )
    watcher = Watcher(
        settings,
        backend,
        stream.sink,
        changelog,
        status,
        writer=writer(WriterContext(base, changelog.dir, target, backend, status)) if writer is not None else None,
        timers=timers,
    )
    try:
        return watcher.run()
    finally:
        backend.close()


def print_header(
    status: StatusOutput,
    app: WatchApp,
    target: Target,
    sink: Sink,
    changelog: Changelog,
    events: frozenset[str],
    backend: Backend,
    *,
    until: str | None = None,
    timeout: str | None = None,
    idle_after: float | None = None,
    fallback: str | None = None,
    thresholds: Sequence[str] = (),
    restart: str | None = None,
    extra: Sequence[str] = (),
) -> None:
    """The startup header: what is watched, how events are delivered, and how to restart with other options."""
    catalog = app.make_source().catalog
    # A threshold kind without its option is not tracked at all, rather than ignored.
    tracked = [k for k in catalog.produced(target.kind) if k not in catalog.thresholds or k in events]
    subscribed = [k for k in tracked if k in events]
    ignored = [k for k in tracked if k not in events]
    status.line(f"resource: {target.key}")
    status.line(f"stream: {sink.describe()}")
    if fallback:
        status.line(f"stream fallback: not messaging the Claude Code session: {fallback}")
    status.line(f"dir: {changelog.dir}")
    status.line(f"changelog: {changelog.events_path}")
    for line in extra:
        status.line(line)
    status.line(f"events: {','.join(subscribed) or '(none)'} (always: {','.join(LIFECYCLE_KINDS)})")
    status.line(f"ignored: {','.join(ignored) or '(none)'}")
    if thresholds:
        status.line(f"thresholds: {'; '.join(thresholds)}")
    ends = [f"on {until}"] if until else []
    ends += [f"after {timeout}"] if timeout else []
    ends += [f"after {format_duration(idle_after)} without an event"] if idle_after else []
    if ends:
        status.line(f"ends: {' or '.join(ends)}")
    status.line(f"daemon: {backend.describe()}")
    groups = ", ".join(catalog.groups_for(target.kind))
    if restart:
        options = "--events=<kinds|groups> --exclude-events=<kinds|groups>"
        status.line(f"restart with: {restart} {options} (groups: {groups or 'none'})")


def daemon_group(app: WatchApp, name: str = "daemon") -> click.Group:
    """`daemon status|stop|logs` for the app's daemon."""

    @click.group(name, help="Inspect or stop the shared polling daemon.")
    def group() -> None:
        pass

    @group.command("status")
    @click.option("--json", "as_json", is_flag=True, help="Print the raw status object.")
    def status(as_json: bool) -> None:
        """Show the daemon's subscriptions, resources, accounts and budgets."""
        files = DaemonFiles.for_app(app)
        try:
            response = request(files, {"op": "status"}, timeout=5)
        except DaemonUnavailable:
            running = daemon_running(files)
            click.echo("daemon: not running" if not running else "daemon: running but not answering; see daemon logs")
            sys.exit(0 if not running else 1)
        if as_json:
            click.echo(json.dumps(response, indent=2))
            return
        for line in format_status(response, files):
            click.echo(line)

    @group.command("stop")
    def stop() -> None:
        """Stop the daemon. Running watchers start a new one on their next poll."""
        files = DaemonFiles.for_app(app)
        try:
            stopped = stop_daemon(files)
        except DaemonUnavailable as exc:
            StatusOutput(app.status_prefix).line(f"error: {exc}")
            sys.exit(1)
        click.echo("daemon: stopped" if stopped else "daemon: not running")

    @group.command("logs")
    @click.option("--lines", "-n", default=50, show_default=True, help="How many lines from the end.")
    def logs(lines: int) -> None:
        """Print the end of the daemon's log file."""
        files = DaemonFiles.for_app(app)
        try:
            content = files.log.read_text(encoding="utf-8", errors="replace").splitlines()
        except FileNotFoundError:
            click.echo(f"no daemon log at {files.log}")
            return
        click.echo(f"==> {files.log}")
        for line in content[-lines:]:
            click.echo(line)

    return group


def format_status(status: Mapping[str, Any], files: DaemonFiles) -> list[str]:
    """The daemon status as text lines."""
    lines = [
        f"daemon: pid {status.get('pid')}, {status.get('app')} {status.get('app_version')}, cli-watch-kit"
        f" {status.get('lib_version')}, protocols {status.get('lib_protocol')}/{status.get('app_protocol')},"
        f" up {status.get('uptime_s')} s, endpoint {files.describe_endpoint()}"
    ]
    for sub in status.get("subscriptions", []):
        lines.append(
            f"subscription {sub['id'][:8]} client {sub['client_id'][:8]} {sub['resource']}"
            f" (account {sub['account'] or '-'}, lease idle {sub['idle_s']} s)"
        )
    for res in status.get("resources", []):
        state = "ended" if res["ended"] else (f"degraded: {res['degraded']}" if res["degraded"] else "active")
        lines.append(
            f"resource {res['resource']}: {state}, {res['polls']} polls, seq {res['seq']},"
            f" next poll in {res['next_poll_in_s']} s"
        )
    for name, account in status.get("accounts", {}).items():
        dims = ", ".join(
            f"{dim} {info['rate_per_minute']}/min (remaining {info['remaining']})"
            for dim, info in account["budget"]["dimensions"].items()
        )
        lines.append(
            f"account {name or '-'}: {account['calls']} calls for {account['items']} items,"
            f" stretch {account['stretch']}, {dims}"
        )
    if status.get("source"):
        lines.append(f"source: {json.dumps(status['source'], separators=(',', ':'))}")
    return lines
