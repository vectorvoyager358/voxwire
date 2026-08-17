"""Recorded-session replay for offline debugging."""

from __future__ import annotations

from typing import Any

__all__ = [
    "Recording",
    "ReplayRequest",
    "TurnRecorder",
    "load_recording",
    "replay_recording",
    "resolve_recording_path",
    "run_replay",
]


def __getattr__(name: str) -> Any:
    if name == "TurnRecorder":
        from server.replay.recorder import TurnRecorder

        return TurnRecorder
    if name in {
        "Recording",
        "ReplayRequest",
        "load_recording",
        "replay_recording",
        "resolve_recording_path",
        "run_replay",
    }:
        from server.replay import replayer

        return getattr(replayer, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
