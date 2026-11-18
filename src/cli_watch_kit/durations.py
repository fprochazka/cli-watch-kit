"""Durations as the options spell them: `90`, `90s`, `30m`, `2h`, `1h30m`."""

from __future__ import annotations

import re

_DURATION_RE = re.compile(r"^(?:(?P<h>\d+)h)?(?:(?P<m>\d+)m)?(?:(?P<s>\d+)s)?$")


def parse_duration(value: str) -> float:
    """Seconds in `value`. A bare number is seconds and may have a fraction."""
    text = value.strip().lower()
    if text.replace(".", "", 1).isdigit():
        return float(text)
    match = _DURATION_RE.match(text)
    if not text or not match or not any(match.groupdict().values()):
        raise ValueError(f"cannot read the duration {value!r}; use for example 90s, 30m, 2h or 1h30m")
    parts = {k: int(v) if v else 0 for k, v in match.groupdict().items()}
    return float(parts["h"] * 3600 + parts["m"] * 60 + parts["s"])


def format_duration(seconds: float) -> str:
    """`90` -> `1m30s`, `7200` -> `2h`: the spelling :func:`parse_duration` reads back."""
    total = int(round(seconds))
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    text = (f"{hours}h" if hours else "") + (f"{minutes}m" if minutes else "") + (f"{secs}s" if secs else "")
    return text or "0s"
