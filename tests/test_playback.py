"""TTS playback gate: drop leftover chunks when replay starts (client/src/playback.ts)."""

from __future__ import annotations

from pathlib import Path


def should_enqueue_tts(active_turn_id: str | None, chunk_turn_id: str | None) -> bool:
    """Mirrors ``shouldEnqueueTts`` in client/src/playback.ts."""
    if not isinstance(chunk_turn_id, str) or not chunk_turn_id:
        return False
    return active_turn_id is None or active_turn_id == chunk_turn_id


def test_stale_tts_chunks_are_dropped() -> None:
    assert should_enqueue_tts("replay-1", "live-1") is False
    assert should_enqueue_tts("replay-1", "replay-1") is True
    assert should_enqueue_tts(None, "live-1") is True
    assert should_enqueue_tts("live-1", None) is False
    assert should_enqueue_tts("live-1", "") is False


def test_should_enqueue_tts_is_exported() -> None:
    src = (Path(__file__).parents[1] / "client" / "src" / "playback.ts").read_text(encoding="utf-8")
    assert "export function shouldEnqueueTts" in src
    assert "activeTurnId === chunkTurnId" in src
