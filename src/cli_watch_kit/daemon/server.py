"""Dispatch of daemon requests to the engine and to the source's extra ops.

Built-in ops: `hello` (versions and code stamp), `subscribe`, `poll`, `unsubscribe`, `status`, `stop`, and `drain`,
which a watcher running newer code sends to stop an older daemon so that a current one replaces it.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Mapping
from typing import Any

from cli_watch_kit.daemon.engine import Engine, EngineError
from cli_watch_kit.source import OpContext, SourceError, Target

log = logging.getLogger(__name__)

BUILTIN_OPS = frozenset({"hello", "subscribe", "poll", "unsubscribe", "status", "stop", "drain"})


class RequestError(ValueError):
    pass


def _str(request: Mapping[str, Any], name: str) -> str:
    value = request.get(name)
    if not isinstance(value, str) or not value:
        raise RequestError(f"{name} must be a non-empty string")
    return value


def _object(request: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = request.get(name) or {}
    if not isinstance(value, dict):
        raise RequestError(f"{name} must be an object")
    return value


def _params(request: Mapping[str, Any]) -> dict[str, Any]:
    return _object(request, "params")


async def handle(
    engine: Engine, request: dict[str, Any], stop: asyncio.Event, identity: Mapping[str, Any]
) -> dict[str, Any]:
    op = request.get("op")
    try:
        if op == "hello":
            return {"ok": True, "pid": os.getpid(), **identity}
        if op == "subscribe":
            options = request.get("options") or {}
            if not isinstance(options, dict):
                raise RequestError("options must be an object")
            raw_events = options.get("events")
            if not isinstance(raw_events, list) or not all(isinstance(e, str) for e in raw_events):
                raise RequestError("options.events must be a list of event kinds")
            result = await engine.subscribe(
                _str(request, "client_id"),
                Target.from_wire(request.get("target")),
                frozenset(raw_events),
                options.get("baseline"),
                _params(request),
                _object(request, "source_options"),
            )
            return {"ok": True, **result}
        if op == "poll":
            cursors = request.get("cursors") or {}
            if not isinstance(cursors, dict) or not all(isinstance(v, int) for v in cursors.values()):
                raise RequestError("cursors must map subscription ids to integers")
            acks = request.get("acks")
            if acks is not None and (not isinstance(acks, dict) or not all(isinstance(v, int) for v in acks.values())):
                raise RequestError("acks must map subscription ids to integers")
            return {"ok": True, **engine.poll(_str(request, "client_id"), cursors, acks)}
        if op == "unsubscribe":
            removed = engine.unsubscribe(_str(request, "client_id"), _str(request, "subscription_id"))
            return {"ok": True, "removed": removed}
        if op == "status":
            now = engine.clock()
            return {
                "ok": True,
                "pid": os.getpid(),
                **identity,
                "uptime_s": round(now - engine.started_at, 1),
                "idle_s": round(engine.idle_for(now), 1),
                **engine.status(),
            }
        if op == "stop":
            log.info("stop requested")
            stop.set()
            return {"ok": True, "pid": os.getpid()}
        if op == "drain":
            log.info(
                "draining for a watcher with newer code (%s %s, cli-watch-kit %s, code %s)",
                request.get("app_version"),
                request.get("app"),
                request.get("lib_version"),
                request.get("fingerprint"),
            )
            stop.set()
            return {"ok": True, "pid": os.getpid()}
        handler = engine.source.ops.get(op) if isinstance(op, str) and op not in BUILTIN_OPS else None
        if handler is None:
            return {"ok": False, "error": f"unknown op {op!r}"}
        body = {k: v for k, v in request.items() if k not in ("op", "params")}
        return {"ok": True, **await handler(OpContext(engine, _params(request)), body)}
    except (RequestError, EngineError, ValueError, TimeoutError) as exc:
        return {"ok": False, "error": str(exc)}
    except SourceError as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__, "reason": exc.reason}
    except Exception as exc:
        # A bug in the daemon or in the source's code: answer, so the watcher does not take it for a dead daemon. The
        # reason `error` lets a resubscribing watcher try again: the bug may sit in a path that fails only sometimes.
        log.exception("the %r request failed", op)
        return {"ok": False, "error": f"the daemon failed: {type(exc).__name__}: {exc}", "reason": "error"}
