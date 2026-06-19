"""Phase 0/1 WebSocket session handler.

Phase 0 proved the transport with a ``ping``/``pong`` + generic echo. Phase 1
issue #6 adds the client's upstream capture path, so this handler now also
*receives* the audio stream and acknowledges it without echoing the (large)
base64 payloads back:

- ``session_start`` -> logged; the declared audio format is remembered.
- ``audio_chunk``   -> counted (chunks + decoded bytes); never echoed.
- ``utterance_end`` -> answered with a small ``capture_summary`` so the client
  can confirm the server received a clean stream for the turn.

The real ASR/LLM/TTS pipeline (issues #8-#11) will replace this receiver.
"""

from __future__ import annotations

import base64
import binascii
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("voxwire.ws")


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
