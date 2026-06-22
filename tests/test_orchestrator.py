"""Issue #11 tests: PipelineOrchestrator sequences stages and tracks degraded turns."""

from __future__ import annotations

import asyncio
import base64

import pytest

from server.pipeline.orchestrator import PipelineOrchestrator
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


class _Collector:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def send(self, payload: dict) -> None:
        self.events.append(payload)


def test_orchestrator_full_turn(
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-1", collector.send)
        await orch.on_audio_chunk({"turnId": "t1", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t1", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    types = [e["type"] for e in events]
    assert "transcript_final" in types
    assert "llm_complete" in types
    assert "tts_audio_chunk" in types
    assert types[-1] == "turn_complete"
    assert events[-1]["meta"]["degraded"] is False
    assert fake_llm.received == ["hello world"]
    assert fake_tts.received == ["Hi there!"]


def test_orchestrator_degraded_on_llm_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    def _raise(_settings):
        raise RuntimeError("Missing required provider credentials:\n  gemini -> set GEMINI_API_KEY")

    monkeypatch.setattr("server.pipeline.orchestrator.get_llm_provider", _raise)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-2", collector.send)
        await orch.on_audio_chunk({"turnId": "t2", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t2", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    errors = [e for e in events if e["type"] == "error"]
    complete = events[-1]
    assert errors and errors[0]["stage"] == "llm"
    assert errors[0]["code"] == "CONFIG_ERROR"
    assert errors[0]["recoverable"] is False
    assert complete["type"] == "turn_complete"
    assert complete["meta"]["degraded"] is True
    assert fake_tts.received == []


def test_orchestrator_close_releases_asr(fake_asr: FakeASRProvider) -> None:
    async def run() -> bool:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-3", collector.send)
        await orch.on_audio_chunk({"turnId": "t3", "seq": 0, "data": _SILENCE})
        await orch.close()
        return fake_asr.last_session is not None and fake_asr.last_session.closed

    assert asyncio.run(run())
