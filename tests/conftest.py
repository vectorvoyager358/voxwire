"""Shared fakes/fixtures so pipeline tests run without keys or network.

A push-to-talk turn now drives ASR, LLM **and** TTS, so tests stub all three
provider factories the WebSocket handler uses (``server.ws.echo.get_asr_provider``,
``server.ws.echo.get_llm_provider`` and ``server.ws.echo.get_tts_provider``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from server.providers.asr import ASRProvider, ASRSession, OnTranscript, Transcript
from server.providers.llm import LLMProvider
from server.providers.tts import TTSProvider


class FakeASRSession(ASRSession):
    """Emits one interim partial on the first chunk; finalizes to fixed text."""

    def __init__(self, on_transcript: OnTranscript, partial_text: str, final_text: str) -> None:
        self._on_transcript = on_transcript
        self._partial_text = partial_text
        self._final_text = final_text
        self._emitted_partial = False
        self.closed = False

    async def send_audio(self, pcm: bytes) -> None:
        if self._partial_text and not self._emitted_partial:
            self._emitted_partial = True
            await self._on_transcript(Transcript(text=self._partial_text, is_final=False))

    async def finalize(self) -> str:
        return self._final_text

    async def aclose(self) -> None:
        self.closed = True


class FakeASRProvider(ASRProvider):
    def __init__(self, partial_text: str = "hello", final_text: str = "hello world") -> None:
        self._partial_text = partial_text
        self._final_text = final_text

    async def start(self, on_transcript: OnTranscript) -> ASRSession:
        return FakeASRSession(on_transcript, self._partial_text, self._final_text)


class FakeLLMProvider(LLMProvider):
    """Streams a scripted list of token deltas and records the prompt."""

    def __init__(self, tokens: list[str] | None = None) -> None:
        self.tokens = tokens if tokens is not None else ["Hi", " there", "!"]
        self.received: list[str] = []

    async def stream(self, user_text: str) -> AsyncIterator[str]:
        self.received.append(user_text)
        for token in self.tokens:
            yield token


class FakeTTSProvider(TTSProvider):
    """Streams a scripted list of raw PCM byte chunks and records the text."""

    def __init__(self, chunks: list[bytes] | None = None) -> None:
        self.chunks = chunks if chunks is not None else [b"\x01\x02\x03\x04", b"\x05\x06"]
        self.received: list[str] = []

    async def stream(self, text: str) -> AsyncIterator[bytes]:
        self.received.append(text)
        for chunk in self.chunks:
            yield chunk


@pytest.fixture
def fake_asr(monkeypatch: pytest.MonkeyPatch) -> FakeASRProvider:
    provider = FakeASRProvider()
    monkeypatch.setattr("server.ws.echo.get_asr_provider", lambda _settings: provider)
    return provider


@pytest.fixture
def fake_llm(monkeypatch: pytest.MonkeyPatch) -> FakeLLMProvider:
    provider = FakeLLMProvider()
    monkeypatch.setattr("server.ws.echo.get_llm_provider", lambda _settings: provider)
    return provider


@pytest.fixture
def fake_tts(monkeypatch: pytest.MonkeyPatch) -> FakeTTSProvider:
    provider = FakeTTSProvider()
    monkeypatch.setattr("server.ws.echo.get_tts_provider", lambda _settings: provider)
    return provider


@pytest.fixture
def fake_pipeline(
    fake_asr: FakeASRProvider, fake_llm: FakeLLMProvider, fake_tts: FakeTTSProvider
) -> None:
    """Stub ASR, LLM and TTS so a full turn needs no keys/network."""
    return None
