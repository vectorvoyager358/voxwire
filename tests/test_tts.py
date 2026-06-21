"""Issue #10 tests: the reply text is synthesized into streamed audio.

Fakes (see conftest.py) replace Deepgram/Gemini/Cartesia. The tests assert the
protocol: ``tts_audio_chunk`` events (in seq order, pcm_s16le @ 24 kHz, base64
of the provider's bytes) after ``llm_complete``, ending with ``turn_complete``.
"""

from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from server.app import app
from server.providers.tts import TTS_SAMPLE_RATE
from tests.conftest import FakeASRProvider, FakeTTSProvider

client = TestClient(app)

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


def _drain_turn(ws) -> list[dict]:
    messages: list[dict] = []
    while True:
        message = ws.receive_json()
        messages.append(message)
        if message["type"] == "turn_complete":
            return messages


def _run_turn(ws, turn_id: str) -> list[dict]:
    ws.send_json({"type": "audio_chunk", "turnId": turn_id, "seq": 0, "data": _SILENCE})
    ws.send_json({"type": "utterance_end", "turnId": turn_id, "totalChunks": 1})
    return _drain_turn(ws)


def test_tts_streams_audio_chunks_then_complete(
    fake_pipeline: None, fake_tts: FakeTTSProvider
) -> None:
    with client.websocket_connect("/ws/session/tts-test") as ws:
        messages = _run_turn(ws, "t1")

    types = [m["type"] for m in messages]
    chunks = [m for m in messages if m["type"] == "tts_audio_chunk"]
    completes = [m for m in messages if m["type"] == "turn_complete"]

    assert [c["seq"] for c in chunks] == [0, 1]
    assert [base64.b64decode(c["data"]) for c in chunks] == [b"\x01\x02\x03\x04", b"\x05\x06"]
    assert all(c["encoding"] == "pcm_s16le" for c in chunks)
    assert all(c["sampleRate"] == TTS_SAMPLE_RATE for c in chunks)

    assert len(completes) == 1
    meta = completes[0]["meta"]
    assert meta["ttsChunks"] == 2
    assert meta["ttsSkipped"] is False
    assert meta["degraded"] is False

    # TTS is fed the LLM reply, and audio precedes turn_complete.
    assert fake_tts.received == ["Hi there!"]
    assert types.index("llm_complete") < types.index("tts_audio_chunk")
    assert types.index("tts_audio_chunk") < types.index("turn_complete")


def test_no_tts_when_no_reply(monkeypatch, fake_pipeline: None, fake_tts: FakeTTSProvider) -> None:
    # Empty transcript -> no LLM, no reply -> TTS skipped, turn still completes.
    monkeypatch.setattr(
        "server.ws.echo.get_asr_provider",
        lambda _settings: FakeASRProvider(partial_text="", final_text=""),
    )
    with client.websocket_connect("/ws/session/tts-empty") as ws:
        messages = _run_turn(ws, "t2")

    types = [m["type"] for m in messages]
    completes = [m for m in messages if m["type"] == "turn_complete"]

    assert "tts_audio_chunk" not in types
    assert fake_tts.received == []
    assert len(completes) == 1
    assert completes[0]["meta"]["ttsSkipped"] is True
    assert completes[0]["meta"]["ttsChunks"] == 0
