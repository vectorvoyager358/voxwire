"""Phase 0/1 WebSocket session handler.

Phase 0 proved the transport with a ``ping``/``pong`` + generic echo. Phase 1
issue #6 adds the client's upstream capture path, so this handler now also
*receives* the audio stream and acknowledges it without echoing the (large)
base64 payloads back:

- ``session_start`` -> logged; the declared audio format is remembered.
- ``audio_chunk``   -> counted (chunks + decoded bytes); never echoed.
- ``utterance_end`` -> answered with a small ``capture_summary``; then a
  **mock** TTS reply is streamed back as ``tts_audio_chunk`` events followed by
  ``turn_complete``, so the client's playback queue (issue #7) can be exercised
  before real TTS (issue #10) exists.

The real ASR/LLM/TTS pipeline (issues #8-#11) will replace this receiver.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import math
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("voxwire.ws")

# Mock TTS downstream format (matches docs/event-protocol.md: 24 kHz mono PCM16).
TTS_SAMPLE_RATE = 24000
_TTS_CHUNK_MS = 120


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class _TurnStats:
    """Per-utterance accumulator for the current turn."""

    turn_id: str | None = None
    chunks: int = 0
    bytes: int = 0
    next_seq: int = 0
    gaps: list[int] = field(default_factory=list)

    def reset(self, turn_id: str) -> None:
        self.turn_id = turn_id
        self.chunks = 0
        self.bytes = 0
        self.next_seq = 0
        self.gaps = []


async def echo_session(websocket: WebSocket, session_id: str) -> None:
    """Accept a WebSocket and handle Phase 0 control + Phase 1 capture messages."""
    await websocket.accept()
    logger.info("ws connected session=%s", session_id)

    stats = _TurnStats()

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "ping":
                await websocket.send_json(
                    {
                        "type": "pong",
                        "sessionId": session_id,
                        "timestamp": _now_iso(),
                        "echo": message.get("payload"),
                    }
                )

            elif msg_type == "session_start":
                audio = message.get("audio", {})
                logger.info("session_start session=%s audio=%s", session_id, audio)

            elif msg_type == "audio_chunk":
                _accumulate_chunk(stats, message)

            elif msg_type == "utterance_end":
                summary = _finish_turn(stats, message)
                logger.info("utterance_end session=%s %s", session_id, summary)
                await websocket.send_json(
                    {
                        "type": "capture_summary",
                        "sessionId": session_id,
                        "turnId": summary["turnId"],
                        "timestamp": _now_iso(),
                        **summary,
                    }
                )
                # Stand-in for the real TTS stage so issue #7 playback is testable.
                await _stream_mock_tts(websocket, session_id, summary["turnId"])

            else:
                # Unknown / Phase 0 messages: echo back for visibility.
                await websocket.send_json(
                    {
                        "type": "echo",
                        "sessionId": session_id,
                        "timestamp": _now_iso(),
                        "received": message,
                    }
                )
    except WebSocketDisconnect:
        logger.info("ws disconnected session=%s", session_id)
    except Exception:  # noqa: BLE001 - log and close, never hang the socket
        logger.exception("ws error session=%s", session_id)
        await websocket.close(code=1011)


def _accumulate_chunk(stats: _TurnStats, message: dict) -> None:
    """Count one ``audio_chunk``, tracking sequence gaps and decoded byte size."""
    turn_id = message.get("turnId")
    if stats.turn_id != turn_id:
        stats.reset(turn_id)

    seq = message.get("seq", stats.next_seq)
    if seq != stats.next_seq:
        stats.gaps.append(seq)
    stats.next_seq = seq + 1

    data = message.get("data", "")
    try:
        stats.bytes += len(base64.b64decode(data, validate=True))
    except (binascii.Error, ValueError):
        logger.warning("bad base64 in audio_chunk turn=%s seq=%s", turn_id, seq)
    stats.chunks += 1


def _finish_turn(stats: _TurnStats, message: dict) -> dict:
    """Build a summary dict for an ``utterance_end`` and reset the accumulator."""
    declared = message.get("totalChunks")
    summary = {
        "turnId": message.get("turnId"),
        "received": stats.chunks,
        "declared": declared,
        "bytes": stats.bytes,
        "clean": not stats.gaps and (declared is None or declared == stats.chunks),
    }
    stats.turn_id = None
    return summary


def _make_tone() -> list[int]:
    """A short, continuous-phase five-note arpeggio as PCM16 samples.

    Phase carries across note boundaries so there are no clicks within the
    reply; any gap or overlap the client introduces between chunks would
    therefore be audible — exactly what issue #7 needs to verify.
    """
    notes = (440, 554, 659, 554, 440)  # A4, C#5, E5, C#5, A4
    note_samples = TTS_SAMPLE_RATE // 2  # 0.5 s per note
    amplitude = 0.25
    samples: list[int] = []
    phase = 0.0
    for freq in notes:
        step = 2 * math.pi * freq / TTS_SAMPLE_RATE
        for _ in range(note_samples):
            samples.append(int(amplitude * 32767 * math.sin(phase)))
            phase += step
    return samples


async def _stream_mock_tts(websocket: WebSocket, session_id: str, turn_id: str | None) -> None:
    """Stream a mock TTS reply as ``tts_audio_chunk`` events + ``turn_complete``."""
    tone = _make_tone()
    samples_per_chunk = TTS_SAMPLE_RATE * _TTS_CHUNK_MS // 1000
    seq = 0
    for start in range(0, len(tone), samples_per_chunk):
        block = tone[start : start + samples_per_chunk]
        raw = struct.pack(f"<{len(block)}h", *block)
        await websocket.send_json(
            {
                "type": "tts_audio_chunk",
                "sessionId": session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                "seq": seq,
                "encoding": "pcm_s16le",
                "sampleRate": TTS_SAMPLE_RATE,
                "data": base64.b64encode(raw).decode(),
            }
        )
        seq += 1
        # Stream faster than real time so the client's queue stays ahead.
        await asyncio.sleep(_TTS_CHUNK_MS / 1000 / 2)

    await websocket.send_json(
        {
            "type": "turn_complete",
            "sessionId": session_id,
            "turnId": turn_id,
            "timestamp": _now_iso(),
            "meta": {"degraded": False, "ttsSkipped": False, "mock": True, "ttsChunks": seq},
        }
    )
