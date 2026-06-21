"""Issue #9 tests: the transcript drives a streamed LLM reply.

Fakes (see conftest.py) replace Deepgram and Gemini. The tests assert the
protocol: ``llm_token`` deltas (in index order, after ``transcript_final``)
followed by exactly one ``llm_complete`` whose text is the concatenation of the
tokens, all before ``turn_complete``.
"""

from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from server.app import app
from tests.conftest import FakeASRProvider, FakeLLMProvider

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


def test_llm_streams_tokens_then_complete(
    fake_asr: FakeASRProvider, fake_llm: FakeLLMProvider
) -> None:
    with client.websocket_connect("/ws/session/llm-test") as ws:
        messages = _run_turn(ws, "t1")

    types = [m["type"] for m in messages]
    tokens = [m for m in messages if m["type"] == "llm_token"]
    completes = [m for m in messages if m["type"] == "llm_complete"]

    assert [t["text"] for t in tokens] == ["Hi", " there", "!"]
    assert [t["index"] for t in tokens] == [0, 1, 2]

    assert len(completes) == 1, "exactly one llm_complete per turn"
    assert completes[0]["text"] == "Hi there!"
    assert "".join(t["text"] for t in tokens) == completes[0]["text"]

    # Ordering: transcript_final -> llm_token(s) -> llm_complete -> turn_complete.
    assert types.index("transcript_final") < types.index("llm_token")
    assert types.index("llm_token") < types.index("llm_complete")
    assert types.index("llm_complete") < types.index("turn_complete")

    # The LLM was prompted with the final transcript.
    assert fake_llm.received == ["hello world"]


def test_no_llm_when_transcript_empty(monkeypatch, fake_llm: FakeLLMProvider) -> None:
    monkeypatch.setattr(
        "server.ws.echo.get_asr_provider",
        lambda _settings: FakeASRProvider(partial_text="", final_text=""),
    )
    with client.websocket_connect("/ws/session/llm-empty") as ws:
        messages = _run_turn(ws, "t2")

    types = [m["type"] for m in messages]
    finals = [m for m in messages if m["type"] == "transcript_final"]

    assert finals and finals[0]["text"] == ""
    assert "llm_token" not in types
    assert "llm_complete" not in types
    assert fake_llm.received == []
    assert "turn_complete" in types
