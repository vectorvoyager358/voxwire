"""Issue #8 tests: ASR wiring produces protocol partial(s) + one final.

Fakes (see conftest.py) replace Deepgram and Gemini so no key or network is
needed; the test drives a full push-to-talk turn over the WebSocket and asserts
the ASR ordering: ``transcript_partial`` then exactly one ``transcript_final``
ahead of ``turn_complete``.
"""

from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from server.app import app

client = TestClient(app)

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


def _drain_turn(ws) -> list[dict]:
    """Read messages until (and including) ``turn_complete``."""
    messages: list[dict] = []
    while True:
        message = ws.receive_json()
        messages.append(message)
        if message["type"] == "turn_complete":
            return messages


def test_turn_emits_partial_then_single_final(fake_pipeline: None) -> None:
    with client.websocket_connect("/ws/session/asr-test") as ws:
        ws.send_json({"type": "session_start", "audio": {"sampleRate": 16000}})
        ws.send_json({"type": "audio_chunk", "turnId": "t1", "seq": 0, "data": _SILENCE})
        ws.send_json({"type": "audio_chunk", "turnId": "t1", "seq": 1, "data": _SILENCE})
        ws.send_json({"type": "utterance_end", "turnId": "t1", "totalChunks": 2})

        messages = _drain_turn(ws)

    types = [m["type"] for m in messages]
    partials = [m for m in messages if m["type"] == "transcript_partial"]
    finals = [m for m in messages if m["type"] == "transcript_final"]

    assert partials, "expected at least one transcript_partial"
    assert partials[0]["text"] == "hello"
    assert partials[0]["turnId"] == "t1"

    assert len(finals) == 1, "exactly one transcript_final per turn"
    assert finals[0]["text"] == "hello world"
    assert finals[0]["turnId"] == "t1"

    assert types.index("transcript_partial") < types.index("transcript_final")
    assert types.index("transcript_final") < types.index("turn_complete")


def test_turn_partial_arrives_before_final(fake_pipeline: None) -> None:
    with client.websocket_connect("/ws/session/asr-test-2") as ws:
        ws.send_json({"type": "audio_chunk", "turnId": "t2", "seq": 0, "data": _SILENCE})
        # Partial is emitted synchronously while handling the first chunk.
        partial = ws.receive_json()
        assert partial["type"] == "transcript_partial"
        assert partial["text"] == "hello"

        ws.send_json({"type": "utterance_end", "turnId": "t2", "totalChunks": 1})
        final = ws.receive_json()
        assert final["type"] == "transcript_final"
        assert final["text"] == "hello world"
