"""Deterministic replay-contract coverage for issue #24."""

from __future__ import annotations

import asyncio
from pathlib import Path

from server.replay import replay_recording

_FIXTURE = Path(__file__).parents[1] / "recordings" / "samples" / "degraded-llm-config.jsonl"


def test_checked_in_degraded_turn_replays_protocol_events_in_order() -> None:
    emitted: list[dict] = []

    async def send(event: dict) -> None:
        emitted.append(event)

    metadata = asyncio.run(replay_recording(_FIXTURE, send))

    assert metadata == {
        "type": "recording_meta",
        "sessionId": "fixture-session",
        "turnId": "fixture-turn",
        "timestamp": "2026-08-07T00:00:00+00:00",
        "audioFile": None,
        "audioEncoding": "pcm_s16le",
        "audioSampleRate": 16000,
        "audioChannels": 1,
        "audioBytes": 0,
        "transcriptLength": 18,
        "replyLength": 0,
        "tokenCount": 0,
        "degraded": True,
        "ttsSkipped": True,
        "ttsChunks": 0,
    }
    assert [event["type"] for event in emitted] == [
        "transcript_final",
        "error",
        "capture_summary",
        "latency_report",
        "turn_complete",
    ]
    assert emitted[1]["code"] == "CONFIG_ERROR"
    assert emitted[3]["failedStage"] == "llm"
    assert emitted[3]["meta"]["degraded"] is True
    assert emitted[4]["meta"]["degraded"] is True
    assert emitted[4]["meta"]["ttsSkipped"] is True
