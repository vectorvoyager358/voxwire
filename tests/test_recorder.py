"""Issue #13 tests: per-turn recording to recordings/."""

from __future__ import annotations

import asyncio
import base64
import json

from server.config import Settings
from server.pipeline.orchestrator import PipelineOrchestrator
from server.replay.recorder import TurnRecorder
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x01" * 160).decode()
_PCM = b"\x00\x01" * 160


def test_turn_recorder_persist(tmp_path) -> None:
    recorder = TurnRecorder("sess-a", tmp_path)
    recorder.begin_turn("turn-1")
    recorder.append_audio(_PCM)
    recorder.record_event({"type": "transcript_final", "turnId": "turn-1", "text": "hi"})
    recorder.record_event({"type": "turn_complete", "turnId": "turn-1"})

    path = recorder.persist(
        transcript="hello world",
        reply="Hi there!",
        token_count=3,
        degraded=False,
        tts_skipped=False,
        tts_chunks=2,
    )

    assert path is not None
    assert path.name == "turn-1.jsonl"
    assert (tmp_path / "turn-1.pcm").read_bytes() == _PCM

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    meta = json.loads(lines[0])
    assert meta["type"] == "recording_meta"
    assert meta["sessionId"] == "sess-a"
    assert meta["turnId"] == "turn-1"
    assert meta["transcriptLength"] == len("hello world")
    assert meta["tokenCount"] == 3
    assert meta["audioFile"] == "turn-1.pcm"
    assert meta["audioBytes"] == len(_PCM)
    assert len(lines) == 3  # meta + 2 events


def test_orchestrator_writes_recording(
    tmp_path,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    settings = Settings(recordings_dir=str(tmp_path))

    async def run() -> None:
        async def send(_payload: dict) -> None:
            return None

        orch = PipelineOrchestrator("sess-b", send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-rec", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-rec", "totalChunks": 1})

    asyncio.run(run())

    jsonl = tmp_path / "t-rec.jsonl"
    pcm = tmp_path / "t-rec.pcm"
    assert jsonl.is_file()
    assert pcm.is_file()

    lines = jsonl.read_text(encoding="utf-8").strip().split("\n")
    meta = json.loads(lines[0])
    assert meta["transcriptLength"] == len("hello world")
    assert meta["tokenCount"] == 3
    assert meta["degraded"] is False

    event_types = [json.loads(line)["type"] for line in lines[1:]]
    assert "transcript_final" in event_types
    assert "llm_complete" in event_types
    assert "turn_complete" in event_types
