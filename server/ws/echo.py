"""Phase 0/1 WebSocket session handler.

Phase 0 proved the transport with a ``ping``/``pong`` + generic echo. Phase 1
issue #6 adds the client's upstream capture path, and issue #8 adds the first
real pipeline stage (ASR), so this handler now:

- ``session_start`` -> logged; the declared audio format is remembered.
- ``audio_chunk``   -> counted (chunks + decoded bytes) **and** streamed to the
  ASR provider, which emits ``transcript_partial`` events live.
- ``utterance_end`` -> the ASR session is finalized into one ``transcript_final``;
  that text is sent to the LLM, whose reply is streamed back as ``llm_token``
  events plus a final ``llm_complete`` (issue #9). The reply text is then
  synthesized by the TTS provider and streamed back as ``tts_audio_chunk`` events
  (issue #10), and the turn ends with ``turn_complete``. A small
  ``capture_summary`` is also sent for diagnostics.

This completes the streaming ASR -> LLM -> TTS pipeline (issue #11 adds the
full end-to-end orchestration/latency work).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

from server.config import get_settings
from server.providers.asr import ASRSession, Transcript, get_asr_provider
from server.providers.llm import get_llm_provider
from server.providers.tts import TTS_ENCODING, TTS_SAMPLE_RATE, get_tts_provider

logger = logging.getLogger("voxwire.ws")

# Sends one JSON envelope to the client (serialized via the session's lock).
Send = Callable[[dict], Awaitable[None]]


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
    # The ASR partials arrive on a background task while the main loop is blocked
    # on receive_json(); serialize all sends so two tasks never interleave a
    # half-written JSON frame on the shared socket.
    send_lock = asyncio.Lock()

    async def send(payload: dict) -> None:
        async with send_lock:
            await websocket.send_json(payload)

    # One streaming recognizer per push-to-talk turn (lazily opened).
    asr_session: ASRSession | None = None
    asr_turn_id: str | None = None

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "ping":
                await send(
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
                turn_id = message.get("turnId")
                if asr_session is None or asr_turn_id != turn_id:
                    if asr_session is not None:
                        await asr_session.aclose()
                        asr_session = None
                    asr_session = await _start_asr(send, session_id, turn_id)
                    asr_turn_id = turn_id
                if asr_session is not None:
                    await _forward_audio(asr_session, send, session_id, turn_id, message)

            elif msg_type == "utterance_end":
                turn_id = message.get("turnId")
                transcript = ""
                if asr_session is not None:
                    transcript = await _finalize_asr(asr_session, send, session_id, turn_id)
                    asr_session = None
                    asr_turn_id = None
                # Generate the spoken reply from the transcript (issue #9).
                reply = ""
                if transcript:
                    reply = await _run_llm(send, session_id, turn_id, transcript)
                summary = _finish_turn(stats, message)
                logger.info("utterance_end session=%s %s", session_id, summary)
                await send(
                    {
                        "type": "capture_summary",
                        "sessionId": session_id,
                        "turnId": summary["turnId"],
                        "timestamp": _now_iso(),
                        **summary,
                    }
                )
                # Synthesize the reply and stream it back, then end the turn (issue #10).
                await _stream_tts(send, session_id, turn_id, reply)

            else:
                # Unknown / Phase 0 messages: echo back for visibility.
                await send(
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
    finally:
        if asr_session is not None:
            await asr_session.aclose()


def _error_event(
    session_id: str,
    turn_id: str | None,
    stage: str,
    code: str,
    detail: str,
    *,
    recoverable: bool = True,
) -> dict:
    return {
        "type": "error",
        "sessionId": session_id,
        "turnId": turn_id,
        "timestamp": _now_iso(),
        "stage": stage,
        "code": code,
        "recoverable": recoverable,
        "message": detail,
    }


async def _start_asr(send: Send, session_id: str, turn_id: str | None) -> ASRSession | None:
    """Open an ASR session for a turn, emitting partials; ``None`` on failure."""

    async def on_transcript(transcript: Transcript) -> None:
        if not transcript.text:
            return
        await send(
            {
                "type": "transcript_partial",
                "sessionId": session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                "text": transcript.text,
            }
        )

    try:
        provider = get_asr_provider(get_settings())
        return await provider.start(on_transcript)
    except Exception as exc:  # noqa: BLE001 - degrade the turn, don't kill the socket
        logger.exception("asr start failed session=%s turn=%s", session_id, turn_id)
        await send(_error_event(session_id, turn_id, "asr", "ASR_UNAVAILABLE", str(exc)))
        return None


async def _forward_audio(
    session: ASRSession,
    send: Send,
    session_id: str,
    turn_id: str | None,
    message: dict,
) -> None:
    """Decode one ``audio_chunk`` and stream its PCM to the recognizer."""
    try:
        pcm = base64.b64decode(message.get("data", ""), validate=True)
    except (binascii.Error, ValueError):
        return  # already warned in _accumulate_chunk
    try:
        await session.send_audio(pcm)
    except Exception:  # noqa: BLE001 - a single dropped chunk shouldn't end the turn
        logger.warning("asr send_audio failed session=%s turn=%s", session_id, turn_id)


async def _finalize_asr(
    session: ASRSession,
    send: Send,
    session_id: str,
    turn_id: str | None,
) -> str:
    """Flush the recognizer, emit one ``transcript_final``, return its text."""
    text = ""
    try:
        text = await session.finalize()
        await send(
            {
                "type": "transcript_final",
                "sessionId": session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                "text": text,
            }
        )
    except Exception as exc:  # noqa: BLE001 - report and still close the turn
        logger.exception("asr finalize failed session=%s turn=%s", session_id, turn_id)
        await send(_error_event(session_id, turn_id, "asr", "ASR_FINALIZE_FAILED", str(exc)))
    finally:
        await session.aclose()
    return text


async def _run_llm(send: Send, session_id: str, turn_id: str | None, user_text: str) -> str:
    """Stream the assistant reply as ``llm_token`` events + one ``llm_complete``.

    Returns the full reply text (also used by the TTS stage). Failures emit a
    protocol ``error`` (stage ``llm``) but never hang the turn.
    """
    try:
        provider = get_llm_provider(get_settings())
    except Exception as exc:  # noqa: BLE001 - degrade the turn, don't kill the socket
        logger.exception("llm start failed session=%s turn=%s", session_id, turn_id)
        await send(_error_event(session_id, turn_id, "llm", "LLM_UNAVAILABLE", str(exc)))
        return ""

    parts: list[str] = []
    index = 0
    try:
        async for token in provider.stream(user_text):
            if not token:
                continue
            parts.append(token)
            await send(
                {
                    "type": "llm_token",
                    "sessionId": session_id,
                    "turnId": turn_id,
                    "timestamp": _now_iso(),
                    "index": index,
                    "text": token,
                }
            )
            index += 1
    except Exception as exc:  # noqa: BLE001 - emit what we have, report, keep going
        logger.exception("llm stream failed session=%s turn=%s", session_id, turn_id)
        await send(_error_event(session_id, turn_id, "llm", "LLM_STREAM_FAILED", str(exc)))

    full = "".join(parts)
    await send(
        {
            "type": "llm_complete",
            "sessionId": session_id,
            "turnId": turn_id,
            "timestamp": _now_iso(),
            "text": full,
        }
    )
    return full


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


async def _stream_tts(send: Send, session_id: str, turn_id: str | None, text: str) -> None:
    """Synthesize ``text`` and stream it as ``tts_audio_chunk`` + ``turn_complete``.

    Always ends the turn with exactly one ``turn_complete``. An empty reply is
    skipped (no audio); a missing key / TTS error emits a protocol ``error``
    (stage ``tts``) and ends the turn degraded rather than hanging the socket.
    """
    seq = 0
    degraded = False
    skipped = not text

    if text:
        try:
            provider = get_tts_provider(get_settings())
        except Exception as exc:  # noqa: BLE001 - degrade the turn, don't kill the socket
            logger.exception("tts start failed session=%s turn=%s", session_id, turn_id)
            await send(_error_event(session_id, turn_id, "tts", "TTS_UNAVAILABLE", str(exc)))
            provider = None
            degraded = True

        if provider is not None:
            try:
                async for pcm in provider.stream(text):
                    if not pcm:
                        continue
                    await send(
                        {
                            "type": "tts_audio_chunk",
                            "sessionId": session_id,
                            "turnId": turn_id,
                            "timestamp": _now_iso(),
                            "seq": seq,
                            "encoding": TTS_ENCODING,
                            "sampleRate": TTS_SAMPLE_RATE,
                            "data": base64.b64encode(pcm).decode(),
                        }
                    )
                    seq += 1
            except Exception as exc:  # noqa: BLE001 - report and still close the turn
                logger.exception("tts stream failed session=%s turn=%s", session_id, turn_id)
                await send(_error_event(session_id, turn_id, "tts", "TTS_STREAM_FAILED", str(exc)))
                degraded = True

    await send(
        {
            "type": "turn_complete",
            "sessionId": session_id,
            "turnId": turn_id,
            "timestamp": _now_iso(),
            "meta": {"degraded": degraded, "ttsSkipped": skipped, "ttsChunks": seq},
        }
    )
