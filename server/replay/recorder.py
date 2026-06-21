"""Per-turn recording for offline replay (issue #13).

Each completed turn writes:

- ``recordings/{turnId}.pcm`` — concatenated upstream PCM16 capture
- ``recordings/{turnId}.jsonl`` — one ``recording_meta`` line plus every
  server-emitted protocol event for the turn (Phase 3 replay input)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from server.providers.asr import ASR_SAMPLE_RATE

logger = logging.getLogger("voxwire.recordings")

# Upstream capture format (matches docs/event-protocol.md).
CAPTURE_ENCODING = "pcm_s16le"
CAPTURE_CHANNELS = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TurnRecorder:
    """Accumulates one turn's inbound audio and outbound events, then persists."""

    session_id: str
    recordings_dir: Path
    _turn_id: str | None = field(default=None, init=False, repr=False)
    _audio: bytearray = field(default_factory=bytearray, init=False, repr=False)
    _events: list[dict] = field(default_factory=list, init=False, repr=False)

    def begin_turn(self, turn_id: str) -> None:
        """Start (or restart) accumulation for ``turn_id``."""
        if self._turn_id == turn_id:
            return
        self._turn_id = turn_id
        self._audio = bytearray()
        self._events = []

    def append_audio(self, pcm: bytes) -> None:
        if self._turn_id and pcm:
            self._audio.extend(pcm)

    def record_event(self, event: dict) -> None:
        if self._turn_id:
            self._events.append(event)

    def persist(
        self,
        *,
        transcript: str,
        reply: str,
        token_count: int,
        degraded: bool,
        tts_skipped: bool,
        tts_chunks: int,
    ) -> Path | None:
        """Write ``.pcm`` + ``.jsonl`` for the current turn; return the jsonl path."""
        if not self._turn_id:
            return None

        turn_id = self._turn_id
        self.recordings_dir.mkdir(parents=True, exist_ok=True)

        audio_path = self.recordings_dir / f"{turn_id}.pcm"
        jsonl_path = self.recordings_dir / f"{turn_id}.jsonl"

        if self._audio:
            audio_path.write_bytes(self._audio)
        elif audio_path.exists():
            audio_path.unlink()

        meta = {
            "type": "recording_meta",
            "sessionId": self.session_id,
            "turnId": turn_id,
            "timestamp": _now_iso(),
            "audioFile": audio_path.name if self._audio else None,
            "audioEncoding": CAPTURE_ENCODING,
            "audioSampleRate": ASR_SAMPLE_RATE,
            "audioChannels": CAPTURE_CHANNELS,
            "audioBytes": len(self._audio),
            "transcriptLength": len(transcript),
            "replyLength": len(reply),
            "tokenCount": token_count,
            "degraded": degraded,
            "ttsSkipped": tts_skipped,
            "ttsChunks": tts_chunks,
        }
        with jsonl_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for event in self._events:
                handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")

        logger.info(
            "turn_recorded session=%s turn=%s transcript_len=%d token_count=%d "
            "events=%d audio_bytes=%d degraded=%s file=%s",
            self.session_id,
            turn_id,
            len(transcript),
            token_count,
            len(self._events),
            len(self._audio),
            degraded,
            jsonl_path,
        )

        self._turn_id = None
        self._audio = bytearray()
        self._events = []
        return jsonl_path
