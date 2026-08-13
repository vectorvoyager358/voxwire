"""Offline WebSocket-to-recording integration coverage for issue #24."""

from __future__ import annotations

import base64
import json

import pytest
from fastapi.testclient import TestClient

from server.app import app
from server.config import Settings
from server.pipeline.errors import CONFIG_ERROR
from tests.conftest import FakeASRProvider

_PCM = b"\x00\x01" * 160


def test_degraded_websocket_turn_is_recorded_in_protocol_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A missing LLM key must degrade and persist without any provider I/O."""
    settings = Settings(
        recordings_dir=str(tmp_path),
        deepgram_api_key=None,
        gemini_api_key=None,
        cartesia_api_key=None,
        langfuse_enabled=False,
    )
    asr = FakeASRProvider(partial_text="offline", final_text="offline transcript")

    monkeypatch.setattr("server.ws.echo.get_settings", lambda: settings)
    monkeypatch.setattr("server.pipeline.orchestrator.get_settings", lambda: settings)
    monkeypatch.setattr("server.pipeline.orchestrator.get_asr_provider", lambda _s: asr)

    def unexpected_llm(*_args, **_kwargs):
        raise AssertionError("an LLM client must not be created without provider credentials")

    monkeypatch.setattr("server.providers.llm.GeminiLLMProvider", unexpected_llm)

    def unexpected_tts(_settings):
        raise AssertionError("TTS provider must not be created after the LLM config failure")

    monkeypatch.setattr("server.pipeline.orchestrator.get_tts_provider", unexpected_tts)

    session_id = "offline-session"
    turn_id = "degraded-turn"
    emitted: list[dict] = []

    with (
        TestClient(app) as client,
        client.websocket_connect(f"/ws/session/{session_id}") as websocket,
    ):
        websocket.send_json(
            {
                "type": "audio_chunk",
                "turnId": turn_id,
                "seq": 0,
                "data": base64.b64encode(_PCM).decode(),
            }
        )
        websocket.send_json({"type": "utterance_end", "turnId": turn_id, "totalChunks": 1})

        while not emitted or emitted[-1]["type"] != "turn_complete":
            emitted.append(websocket.receive_json())

        # The pong proves the handler completed the turn and resumed receiving,
        # which also places recording persistence before the assertions below.
        websocket.send_json({"type": "ping", "payload": {"after": turn_id}})
        pong = websocket.receive_json()

    assert pong["type"] == "pong"
    assert asr.last_session is not None and asr.last_session.closed is True

    event_types = [event["type"] for event in emitted]
    error_index = event_types.index("error")
    latency_index = event_types.index("latency_report")
    complete_index = event_types.index("turn_complete")
    assert error_index < latency_index < complete_index == len(emitted) - 1

    error = emitted[error_index]
    assert error["stage"] == "llm"
    assert error["code"] == CONFIG_ERROR

    latency = emitted[latency_index]
    assert latency["failedStage"] == "llm"
    assert latency["meta"]["degraded"] is True

    complete = emitted[complete_index]
    assert complete["meta"]["degraded"] is True
    assert complete["meta"]["ttsSkipped"] is True
    assert complete["meta"]["ttsChunks"] == 0
    assert complete["meta"]["latency_report"] == {
        key: value
        for key, value in latency.items()
        if key not in {"type", "sessionId", "turnId", "timestamp"}
    }

    recording_path = tmp_path / f"{turn_id}.jsonl"
    recorded_lines = [
        json.loads(line) for line in recording_path.read_text(encoding="utf-8").splitlines()
    ]
    recording_meta, *recorded_events = recorded_lines
    assert (tmp_path / f"{turn_id}.pcm").read_bytes() == _PCM
    assert recording_meta["sessionId"] == session_id
    assert recording_meta["turnId"] == turn_id
    assert recording_meta["degraded"] is True
    assert recording_meta["ttsSkipped"] is True
    assert recording_meta["ttsChunks"] == 0
    assert [event["type"] for event in recorded_events] == event_types
    assert recorded_events == emitted
