"""Deterministic, offline replay of recorded protocol events."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path

Send = Callable[[dict], Awaitable[None]]


async def replay_recording(path: str | Path, send: Send) -> dict:
    """Emit a JSONL recording's protocol events in order and return its metadata."""
    rows = [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows or rows[0].get("type") != "recording_meta":
        raise ValueError("recording must begin with a recording_meta event")

    metadata, *events = rows
    for event in events:
        await send(event)
    return metadata
