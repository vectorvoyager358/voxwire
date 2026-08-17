"""Load JSONL+PCM recordings and replay them as events or through the pipeline."""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from server.config import Settings

if TYPE_CHECKING:
    from server.pipeline.orchestrator import PipelineOrchestrator

Send = Callable[[dict], Awaitable[None]]

ReplayMode = Literal["events", "full", "mock"]

# Upstream capture format (docs/event-protocol.md): 16 kHz mono pcm_s16le.
_ASR_SAMPLE_RATE = 16000
_CHUNK_BYTES = _ASR_SAMPLE_RATE * 2 // 10


def _pcm_is_silent(pcm: bytes) -> bool:
    return not any(pcm)


class ReplayRequest(BaseModel):
    """Trigger payload for WebSocket ``replay_start`` and ``POST /replay``."""

    model_config = ConfigDict(populate_by_name=True)

    recording_path: str = Field(alias="recordingPath")
    mode: ReplayMode = "events"
    mock_llm: bool = Field(default=False, alias="mockLlm")
    session_id: str = Field(default="replay", alias="sessionId")
    turn_id: str | None = Field(default=None, alias="turnId")


@dataclass
class Recording:
    """One persisted turn: metadata, protocol events, and optional PCM."""

    path: Path
    metadata: dict
    events: list[dict]
    pcm: bytes

    @property
    def transcript(self) -> str:
        for event in self.events:
            if event.get("type") == "transcript_final":
                return str(event.get("text") or "").strip()
        return ""


class ScriptedLLMProvider:
    """Yields recorded token deltas so mock replay can skip a live LLM."""

    def __init__(self, tokens: list[str]) -> None:
        self.tokens = tokens

    async def stream(self, user_text: str) -> AsyncIterator[str]:
        del user_text
        for token in self.tokens:
            yield token


class ScriptedTTSProvider:
    """Yields recorded PCM chunks so mock replay can skip live TTS."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def stream(self, text: str) -> AsyncIterator[bytes]:
        del text
        for chunk in self.chunks:
            yield chunk


def resolve_recording_path(raw: str, recordings_dir: Path) -> Path:
    """Resolve ``raw`` to a file inside ``recordings_dir``; reject escapes."""
    base = recordings_dir.expanduser().resolve()
    given = Path(raw).expanduser()
    candidates: list[Path] = []
    if given.is_absolute():
        candidates.append(given)
    else:
        candidates.append(Path.cwd() / given)
        candidates.append(base / given)
        parts = given.parts
        if parts and parts[0] == base.name:
            candidates.append(base.joinpath(*parts[1:]))
        if len(parts) > 1:
            candidates.append(base / given.name)

    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if not resolved.is_file():
            continue
        if not _is_inside(resolved, base):
            raise ValueError("recording path is outside RECORDINGS_DIR")
        return resolved
    raise ValueError(f"recording not found: {raw}")


def _is_inside(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def load_recording(path: str | Path) -> Recording:
    """Read a JSONL recording and its sibling PCM file, if declared."""
    jsonl_path = Path(path)
    rows = [
        json.loads(line)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows or rows[0].get("type") != "recording_meta":
        raise ValueError("recording must begin with a recording_meta event")

    metadata, *events = rows
    pcm = b""
    audio_file = metadata.get("audioFile")
    if audio_file:
        audio_path = jsonl_path.with_name(str(audio_file))
        if not audio_path.is_file():
            raise ValueError(f"recording audio file not found: {audio_path.name}")
        pcm = audio_path.read_bytes()
    return Recording(path=jsonl_path, metadata=metadata, events=events, pcm=pcm)


async def replay_recording(path: str | Path, send: Send) -> dict:
    """Emit a JSONL recording's protocol events in order and return its metadata."""
    recording = load_recording(path)
    for event in recording.events:
        await send(event)
    return recording.metadata


def llm_from_recording(recording: Recording) -> ScriptedLLMProvider:
    tokens = [
        str(event["text"])
        for event in recording.events
        if event.get("type") == "llm_token" and event.get("text")
    ]
    if not tokens:
        complete = next(
            (
                str(event.get("text") or "")
                for event in recording.events
                if event.get("type") == "llm_complete"
            ),
            "",
        )
        tokens = [complete] if complete else ["OK"]
    return ScriptedLLMProvider(tokens)


def tts_from_recording(recording: Recording) -> ScriptedTTSProvider:
    chunks: list[bytes] = []
    for event in recording.events:
        if event.get("type") != "tts_audio_chunk":
            continue
        data = event.get("data")
        if not data:
            continue
        chunks.append(base64.b64decode(data))
    return ScriptedTTSProvider(chunks)


async def run_replay(
    request: ReplayRequest,
    send: Send,
    settings: Settings,
    orchestrator: PipelineOrchestrator | None = None,
) -> dict:
    """Run event, full, or mock replay and return recording metadata."""
    path = resolve_recording_path(request.recording_path, Path(settings.recordings_dir))
    recording = load_recording(path)
    turn_id = request.turn_id or recording.metadata.get("turnId") or str(uuid.uuid4())

    if request.mode == "events":
        for event in recording.events:
            payload = {**event, "sessionId": request.session_id}
            if request.turn_id:
                payload["turnId"] = request.turn_id
            await send(payload)
        return recording.metadata

    from server.pipeline.orchestrator import PipelineOrchestrator

    owns_orchestrator = orchestrator is None
    llm = tts = None
    if request.mode == "mock" and request.mock_llm:
        llm = llm_from_recording(recording)
        tts = tts_from_recording(recording)
    if orchestrator is None:
        orchestrator = PipelineOrchestrator(
            request.session_id,
            send,
            settings,
            llm_provider=llm,
            tts_provider=tts,
        )

    previous = (
        orchestrator._llm_override,
        orchestrator._tts_override,
    )
    try:
        if request.mode == "mock" and request.mock_llm:
            orchestrator._llm_override = llm
            orchestrator._tts_override = tts
        if request.mode == "full":
            await _replay_full(orchestrator, recording, str(turn_id))
        else:
            await _replay_mock(orchestrator, recording, str(turn_id))
    finally:
        orchestrator._llm_override, orchestrator._tts_override = previous
        if owns_orchestrator:
            await orchestrator.close()
    return recording.metadata


async def _replay_full(
    orchestrator: PipelineOrchestrator, recording: Recording, turn_id: str
) -> None:
    if not recording.pcm:
        raise ValueError(
            "Full pipeline needs a .pcm capture. This recording has none — use Events or Mock."
        )
    # Bundled fixtures (and some captures) are silence; live ASR yields no text.
    if _pcm_is_silent(recording.pcm) and recording.transcript:
        await _replay_mock(orchestrator, recording, turn_id)
        return
    seq = 0
    for offset in range(0, len(recording.pcm), _CHUNK_BYTES):
        piece = recording.pcm[offset : offset + _CHUNK_BYTES]
        await orchestrator.on_audio_chunk(
            {
                "turnId": turn_id,
                "seq": seq,
                "data": base64.b64encode(piece).decode(),
            }
        )
        seq += 1
    capture_ms = round(len(recording.pcm) / (_ASR_SAMPLE_RATE * 2) * 1000)
    await orchestrator.on_utterance_end(
        {
            "turnId": turn_id,
            "totalChunks": seq,
            "captureMs": capture_ms,
        }
    )


async def _replay_mock(
    orchestrator: PipelineOrchestrator, recording: Recording, turn_id: str
) -> None:
    text = recording.transcript
    if not text:
        raise ValueError("mock replay requires a transcript_final event")
    await orchestrator.on_text_turn({"turnId": turn_id, "text": text})


def replay_error_payload(session_id: str, exc: BaseException, turn_id: str | None = None) -> dict:
    """Structured ``error`` for a rejected replay request (does not end a turn)."""
    return {
        "type": "error",
        "sessionId": session_id,
        "turnId": turn_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": "orchestrator",
        "code": "BAD_REQUEST",
        "recoverable": False,
        "message": str(exc),
    }


__all__ = [
    "Recording",
    "ReplayRequest",
    "load_recording",
    "replay_error_payload",
    "replay_recording",
    "resolve_recording_path",
    "run_replay",
]
