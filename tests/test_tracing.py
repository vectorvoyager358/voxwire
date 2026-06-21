"""Issue #18 tests: optional Langfuse tracing per turn."""

from __future__ import annotations

import asyncio
import base64
from typing import Any

import pytest

from server.config import Settings
from server.observability.tracing import TurnTracer, get_turn_tracer
from server.pipeline.orchestrator import PipelineOrchestrator
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


class _FakeObservation:
    def __init__(self, name: str, as_type: str, **kwargs: Any) -> None:
        self.name = name
        self.as_type = as_type
        self.kwargs = kwargs
        self.updates: list[dict[str, Any]] = []
        self.children: list[_FakeObservation] = []
        self.ended = False

    def start_observation(self, *, name: str, as_type: str, **kwargs: Any) -> _FakeObservation:
        child = _FakeObservation(name, as_type, **kwargs)
        self.children.append(child)
        return child

    def update(self, **kwargs: Any) -> None:
        self.updates.append(kwargs)

    def end(self) -> None:
        self.ended = True


class _FakeLangfuseClient:
    def __init__(self) -> None:
        self.roots: list[_FakeObservation] = []
        self.flush_count = 0

    def start_observation(self, **kwargs: Any) -> _FakeObservation:
        root = _FakeObservation(
            kwargs.pop("name", ""),
            kwargs.pop("as_type", "span"),
            **kwargs,
        )
        self.roots.append(root)
        return root

    def flush(self) -> None:
        self.flush_count += 1


def test_langfuse_tracing_requires_enabled_flag() -> None:
    settings = Settings(
        langfuse_enabled=False,
        langfuse_public_key="pk-test",
        langfuse_secret_key="sk-test",
    )
    tracer = get_turn_tracer(settings)
    assert tracer.enabled is False
    trace = tracer.begin_turn("sess", "turn-1")
    trace.begin_asr()
    trace.end_asr("hello")
    trace.finish(
        transcript="hello",
        reply="hi",
        token_count=1,
        degraded=False,
        latency_report={"totalMs": 0},
    )


def test_langfuse_tracing_requires_credentials() -> None:
    settings = Settings(langfuse_enabled=True)
    assert settings.langfuse_tracing_enabled is False
    assert get_turn_tracer(settings).enabled is False


def test_turn_tracer_emits_asr_llm_tts_spans(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    fake_client = _FakeLangfuseClient()
    settings = Settings(
        langfuse_enabled=True,
        langfuse_public_key="pk-test",
        langfuse_secret_key="sk-test",
    )

    monkeypatch.setattr(
        "server.observability.tracing._langfuse_client",
        lambda _pk, _sk, _host: fake_client,
    )

    async def run() -> None:
        events: list[dict] = []

        async def send(payload: dict) -> None:
            events.append(payload)

        orch = PipelineOrchestrator("sess-trace", send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-trace", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-trace", "totalChunks": 1, "captureMs": 120})

    asyncio.run(run())

    assert len(fake_client.roots) == 1
    root = fake_client.roots[0]
    assert root.name == "turn"
    assert root.ended is True
    assert fake_client.flush_count == 1

    child_names = [child.name for child in root.children]
    assert child_names == ["asr", "llm", "tts"]

    asr, llm, tts = root.children
    assert asr.as_type == "span"
    assert asr.ended is True
    assert asr.updates[-1]["output"] == "hello world"

    assert llm.as_type == "generation"
    assert llm.kwargs["input"] == "hello world"
    assert llm.updates[-1]["output"] == "Hi there!"
    assert llm.updates[-1]["usage_details"] == {"output": 3}

    assert tts.ended is True
    assert tts.updates[-1]["output"] == {"skipped": False, "chunks": 2}

    finish_meta = root.updates[-1]["metadata"]
    assert finish_meta["turnId"] == "t-trace"
    assert finish_meta["sessionId"] == "sess-trace"
    assert "latency" in finish_meta


def test_orchestrator_skips_tracing_when_disabled(
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    async def run() -> None:
        async def send(_payload: dict) -> None:
            return None

        orch = PipelineOrchestrator("sess-off", send)
        await orch.on_audio_chunk({"turnId": "t-off", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-off", "totalChunks": 1})

    asyncio.run(run())
    assert TurnTracer(Settings()).enabled is False
