# cli-watch-kit

A library for building a `watch` command into a CLI. The CLI says how to poll its API and how to diff two snapshots into events; the library does the rest:

- one JSON event per line on stdout (or a message into the Claude Code session that started the watch), with a status header on stderr;
- `--events` / `--exclude-events` filters with groups, `--until`, `--timeout`, `--idle-after`, and the app's own threshold events computed from time;
- a changelog (`events.jsonl`) and a baseline per watched resource, so a watcher resumes its numbering and never reports a change twice;
- one shared daemon per user and app that polls each resource once for all its watchers, batches resources of one account into one API call, paces every account on a rate budget with several dimensions (requests and complexity points, for example), and survives the watcher that started it;
- a `daemon status|stop|logs` command group.

It was extracted from the `glab-watch` command of [glab-cli-extras](https://github.com/fprochazka/glab-cli-extras).

## Installation

Requires Python 3.12+. The core depends only on click.

```bash
uv add cli-watch-kit                  # stdout only
uv add 'cli-watch-kit[claude-code]'   # also messages into a Claude Code session, through cc-blackbox
```

## Concepts

- A **source** (`cli_watch_kit.source.Source`) polls **targets** (`Target(kind, id, data)`), such as `issue ABC-123`. Its `poll()` gets a batch of `PollItem`s (target, previous snapshot, the event kinds anyone wants) and returns a `PollOutcome` per target: the new snapshot, the events, an optional end, or a per-target error (`NotFound`, `Unauthorized`, `RateLimited`, `Transient`). The diff should be a pure function the CLI tests on its own. A poll without a previous snapshot is the baseline and reports nothing.
- `PollItem.state` is a dict private to one resource that lives as long as the resource: the place for an ETag, work held back for a later poll, or a stateful poller object. It starts empty after a daemon restart; only the snapshot survives one.
- An **account** (`Source.account(target, params)`) is whose rate limit a poll spends. Targets of different accounts never share a call or a budget.
- Targets of one account with the same **batch key** (`Source.batch_key`) that are due within `coalesce_window` seconds go into one `poll()` call, split so that no call costs more than `batch_cost_cap`. `cost(batch)` estimates a call's cost per budget dimension; the source reports what the API said through `ctx.budget.observe(...)` and corrects the estimate with `ctx.charge(...)`.
- **Params** are what a watcher sends along with its subscription, such as an API key. They travel over the daemon's private socket (`0600` in a `0700` dir), reach the source per account, and never appear in the status or the log. The latest params of an account win.
- The **catalog** (`KindCatalog`) names the event kinds per resource kind, the groups of the filter options, and the threshold kinds the watcher computes itself. `watch_started`, `watch_degraded` and `watch_ended` are always reported.

## A minimal source and CLI

```python
# my_cli/watch.py
from collections.abc import Mapping, Sequence
from typing import Any

import click

from cli_watch_kit.app import WatchApp
from cli_watch_kit.budget import DimensionSpec
from cli_watch_kit.cli import WatchOptions, daemon_group, run_watch, watch_options
from cli_watch_kit.events import KindCatalog, RawEvent
from cli_watch_kit.source import NotFound, Params, PollContext, PollItem, PollOutcome, Source, Target

type Snapshot = dict[str, Any]


class IssueSource(Source[Snapshot]):
    catalog = KindCatalog({"issue": ("state_changed",)}, groups={"status": ("state_changed",)})
    dimensions = {
        "requests": DimensionSpec(per_minute=40),
        "complexity": DimensionSpec(per_minute=50_000, burst=10_000),
    }
    batch_cost_cap = {"complexity": 5_000}

    async def account(self, target: Target, params: Params) -> str:
        return await whoami(params["api_key"])  # the user the key belongs to

    def batch_key(self, target: Target) -> str:
        return "issues"  # every issue of an account in one request

    def interval(self, target: Target, snapshot: Snapshot | None) -> float:
        return 30.0

    def cost(self, batch: Sequence[PollItem[Snapshot]]) -> Mapping[str, float]:
        return {"requests": 1, "complexity": 20 * len(batch)}

    async def poll(self, batch: Sequence[PollItem[Snapshot]], ctx: PollContext) -> Mapping[str, PollOutcome[Snapshot]]:
        found = await fetch_issues(ctx.params["api_key"], [item.target.id for item in batch])  # raise RateLimited etc.
        outcomes = {}
        for item in batch:
            issue = found.get(item.target.id)
            if issue is None:
                outcomes[item.key] = PollOutcome(error=NotFound(f"no issue {item.target.id}"))
                continue
            events = diff(item.snapshot, issue) if item.snapshot is not None else []  # list[RawEvent]
            ended = "completed" if issue["state"] == "Done" else None
            outcomes[item.key] = PollOutcome(issue, events, ended=ended)
        return outcomes


APP = WatchApp(
    name="my-cli-watch",
    source=IssueSource,  # or an instance; a callable is called once, when needed
    daemon_module="my_cli.watch_daemon",
    app_protocol=1,
    version="1.0.0",
    status_prefix="my-cli watch: ",
    envelope_tag="my-cli-watcher",
    command="my-cli watch",
)


@click.group()
def watch() -> None:
    """Watch issues."""


@watch.command()
@click.argument("issue_id")
@watch_options
def issue(issue_id: str, options: WatchOptions) -> None:
    raise SystemExit(run_watch(APP, Target("issue", issue_id), options, params={"api_key": api_key()}))


watch.add_command(daemon_group(APP))
```

```python
# my_cli/watch_daemon.py: what `python -m my_cli.watch_daemon` runs
from cli_watch_kit.daemon import main
from my_cli.watch import APP

if __name__ == "__main__":
    main(APP)
```

`run_watch` also takes `enabled_thresholds` and `timers` (the app's `ClientTimer`s for threshold kinds), a `writer` for the app's own files in the resource dir, `resource_dir`, `readme`, `sender_name`, `restart` and extra `header` lines. A CLI on another framework fills a `WatchOptions` itself, or uses `cli_watch_kit.client.Watcher` directly.

An app with requests the daemon should serve within the budget (fetching a log for a watcher, for example) puts them in `Source.ops`; a watcher calls them with `backend.call(op, payload, params)`. A source whose targets relay other targets (an MR and its head pipeline) returns them from `Source.children`.

## The daemon

- The first watcher starts the daemon with `python -m <daemon_module>` under a spawn lock. The daemon forks once more, so a tool that kills the watcher's whole process tree does not kill it, and holds a lock for its lifetime; a second one exits at once.
- IPC is one NDJSON request and response per connection over a Unix socket under `$XDG_RUNTIME_DIR/<name>/` (or `$TMPDIR`, or the temp dir). Windows, or `<PREFIX>TRANSPORT=tcp`, uses loopback TCP with a token in a private file. The socket name carries the library's and the app's protocol versions (`daemon-l1-a1.sock`), so incompatible daemons never meet.
- Each watcher polls the daemon every 5 s, which renews its lease; a subscription not renewed for 20 s is dropped. Watchers of one target in one account share one resource, which keeps its last 500 events. The daemon exits 30 s after its last subscription.
- Each account has a token bucket per budget dimension, refilled at half of `remaining / (reset - now)` once the source reports readings. A 429 (`RateLimited`) blocks the account for its `Retry-After` and backs off exponentially. When the watched resources would exceed any dimension, all their intervals stretch by the same factor. Errors are reported once per kind as `watch_degraded`, with exponential backoff per resource.
- After a daemon crash, the watcher starts a new daemon and resubscribes with its baseline, so the new daemon reports only what changed meanwhile, and the watcher drops a repeated `watch_started`.
- **Upgrades:** `hello` returns the library and app versions and a stamp of the installed code (a fingerprint and the newest file time of both packages). A watcher with newer code asks an older daemon to `drain` (stop) and starts a current one; the old daemon's watchers resubscribe from their baselines. A watcher with older code uses a newer daemon as it is. This also covers editable installs, where the code changes without a version.
- `--no-daemon` runs the same engine and request handler in a thread of the watcher, with a private budget.
- Environment variables, with the app's prefix (default: the name in upper case, `-` as `_`, plus `_`): `STATE_DIR`, `BASE_DIR`, `TRANSPORT`, `IDLE_TIMEOUT`, `LEASE_TIMEOUT`, `CLIENT_POLL_MS`, `DEBUG`.

## Claude Code

With the `claude-code` extra, `--events-stream=auto` (the default) sends the events into the Claude Code session that started the watcher, when that session accepts messages, as `<envelope_tag resource="issue ABC-123" seq="1-2">` blocks. cc-blackbox does the delivery: session lookup, the protocol check, burst batching, the 64 KB cap, the receiver's rate limit, and the end of the watch when the session is gone. The resource dir then lives in the session's scratchpad. Without the extra, `auto` uses stdout, and under Claude Code the header says which extra to install; `claude-message` fails with the same hint. Nothing imports cc-blackbox until a stream needs it.

## Developing

```bash
uv sync
uv run ruff check . && uv run ruff format --check . && uv run mypy --strict src tests && uv run pytest
```

The tests run the engine on a fake clock against a fake source (`tests/fakeapp`), and start real daemon processes in short temp dirs over both transports. CI runs the same checks on Python 3.12 to 3.15.

To develop a CLI against a local checkout, add it as an editable path dependency of the CLI:

```toml
[tool.uv.sources]
cli-watch-kit = { path = "../cli-watch-kit", editable = true }
```

### Commit hooks

Fixtures and captured output can carry private identifiers such as workspace, account or issue names, API keys or paths. `scripts/hooks` holds a pre-commit and a commit-msg hook that refuse a commit when a staged line or the commit message matches a private identifier. The identifiers live in a file outside the repository, because listing them here would publish the very strings the hooks exist to keep out. Set it up once per clone:

```bash
git config core.hooksPath scripts/hooks
git config hooks.forbiddenPatternsFile ~/.config/git/forbidden-patterns.txt  # any path outside the repo
```

The file holds one extended regex per line, matched case-insensitively; blank lines and `#` comments are ignored. Until `hooks.forbiddenPatternsFile` points at a file with at least one pattern, both hooks refuse every commit and print how to fix it.

## Releasing

The version comes from the git tag through hatch-vcs. Pushing a `vX.Y.Z` tag runs `.github/workflows/publish.yml`, which builds the package and publishes it to PyPI through trusted publishing (the `pypi` environment of the GitHub repository must be registered as a trusted publisher on PyPI first).

## License

MIT
