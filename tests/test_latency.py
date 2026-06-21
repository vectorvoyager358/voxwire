"""Issue #15 tests: LatencyTracker marks and stage breakdown."""

from __future__ import annotations

import asyncio
import base64

import pytest

from server.latency.tracker import LatencyTracker, _union_length
from server.pipeline.orchestrator import PipelineOrchestrator
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


def test_union_length_merges_overlaps() -> None:
    assert _union_length([(0.0, 1.0), (0.5, 1.5), (2.0, 3.0)]) == 2.5


def test_latency_tracker_happy_path_stages() -> None:
    tracker = LatencyTracker()
    tracker.begin_turn()
    tracker.mark("first_audio_chunk", at=0.0)
    tracker.mark("last_audio_chunk", at=0.5)
    tracker.set_client_capture_ms(850)
    tracker.mark("utterance_end", at=1.0)  # T₀
    tracker.mark("asr_first_partial", at=1.18)
    tracker.mark("asr_final", at=1.42)
    tracker.mark("llm_start", at=1.43)
    tracker.mark("llm_first_token", at=1.71)
    tracker.mark("llm_complete", at=2.29)
    tracker.mark("tts_start", at=1.8)
    tracker.mark("tts_first_byte", at=2.02)
    tracker.mark("tts_complete", at=2.38)
    tracker.mark("turn_complete", at=2.5)

    report = tracker.build_report()
    stages = report["stages"]

    assert report["totalMs"] == 1500
    assert report["bottleneckStage"] == "asr"
    assert report["failedStage"] is None
    assert report["meta"]["totalMs"] == 1500
    assert report["meta"]["degraded"] is False
    assert stages["clientCaptureMs"] == 850
    assert stages["audioUploadMs"] == 500
    assert stages["asrFirstPartialMs"] == 180
    assert stages["asrFinalMs"] == 420
    assert stages["llmTtftMs"] == 710
    assert stages["llmCompleteMs"] == 1290
    assert stages["ttsTtfbMs"] == 1020
    assert stages["ttsCompleteMs"] == 1380
    assert stages["orchestrationOverheadMs"] == 130


def test_latency_tracker_skips_tts_stages_when_flagged() -> None:
    tracker = LatencyTracker()
    tracker.mark("utterance_end", at=0.0)
    tracker.set_tts_skipped(True)
    tracker.mark("turn_complete", at=0.1)
    stages = tracker.build_report()["stages"]
    assert stages["ttsTtfbMs"] is None
    assert stages["ttsCompleteMs"] is None


def test_latency_tracker_degraded_failed_stage() -> None:
    tracker = LatencyTracker()
    tracker.mark("utterance_end", at=0.0)
    tracker.mark("asr_final", at=0.2)
    tracker.set_failed_stage("llm")
    tracker.set_tts_skipped(True)
    tracker.mark("turn_complete", at=0.5)

    report = tracker.build_report(degraded=True)
    assert report["failedStage"] == "llm"
    assert report["meta"]["failedStage"] == "llm"
    assert report["meta"]["degraded"] is True
    assert report["stages"]["asrFinalMs"] == 200
    assert report["stages"]["llmCompleteMs"] is None


def test_orchestrator_emits_latency_report(
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    async def run() -> list[dict]:
        events: list[dict] = []

        async def send(payload: dict) -> None:
            events.append(payload)

        orch = PipelineOrchestrator("sess-lat", send)
        await orch.on_audio_chunk({"turnId": "t1", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t1", "totalChunks": 1, "captureMs": 320})
        return events

    events = asyncio.run(run())
    complete = events[-1]
    latency_event = events[-2]
    assert latency_event["type"] == "latency_report"
    assert complete["type"] == "turn_complete"
    report = complete["meta"]["latency_report"]
    assert complete["meta"]["latency"] == report["meta"]
    assert "totalMs" in report
    assert report["bottleneckStage"] is not None
    assert report["stages"]["clientCaptureMs"] == 320
    assert report["stages"]["asrFinalMs"] is not None
    assert report["stages"]["llmCompleteMs"] is not None
    assert report["stages"]["ttsCompleteMs"] is not None


def test_orchestrator_degraded_latency_report(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    def _raise(_settings):
        raise RuntimeError("no gemini key")

    monkeypatch.setattr("server.pipeline.orchestrator.get_llm_provider", _raise)

    async def run() -> dict:
        events: list[dict] = []

        async def send(payload: dict) -> None:
            events.append(payload)

        orch = PipelineOrchestrator("sess-deg", send)
        await orch.on_audio_chunk({"turnId": "t2", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t2", "totalChunks": 1})
        return events[-1]

    complete = asyncio.run(run())
    report = complete["meta"]["latency_report"]
    assert complete["meta"]["degraded"] is True
    assert report["failedStage"] == "llm"
    assert report["meta"]["degraded"] is True
    assert report["stages"]["asrFinalMs"] is not None
    assert report["stages"]["llmCompleteMs"] is None
