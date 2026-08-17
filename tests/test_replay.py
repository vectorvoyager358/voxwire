"""Replay: event playback, full/mock pipeline, HTTP, WebSocket, and CLI."""

from __future__ import annotations

import asyncio
import re
import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import app
from server.config import Settings
from server.pipeline.errors import BAD_REQUEST
from server.replay import replay_recording
from server.replay.__main__ import main as replay_cli
from server.replay.replayer import ReplayRequest, resolve_recording_path, run_replay
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SAMPLES = Path(__file__).parents[1] / "recordings" / "samples"
_DEGRADED = _SAMPLES / "degraded-llm-config.jsonl"
_HELLO_JSONL = _SAMPLES / "hello-turn.jsonl"
_HELLO_PCM = _SAMPLES / "hello-turn.pcm"


def _copy_hello(tmp_path: Path) -> None:
    shutil.copy(_HELLO_JSONL, tmp_path / "hello-turn.jsonl")
    shutil.copy(_HELLO_PCM, tmp_path / "hello-turn.pcm")


def test_checked_in_degraded_turn_replays_protocol_events_in_order() -> None:
    emitted: list[dict] = []

    async def send(event: dict) -> None:
        emitted.append(event)

    metadata = asyncio.run(replay_recording(_DEGRADED, send))

    assert metadata == {
        "type": "recording_meta",
        "sessionId": "fixture-session",
        "turnId": "fixture-turn",
        "timestamp": "2026-08-07T00:00:00+00:00",
        "audioFile": None,
        "audioEncoding": "pcm_s16le",
        "audioSampleRate": 16000,
        "audioChannels": 1,
        "audioBytes": 0,
        "transcriptLength": 18,
        "replyLength": 0,
        "tokenCount": 0,
        "degraded": True,
        "ttsSkipped": True,
        "ttsChunks": 0,
    }
    assert [event["type"] for event in emitted] == [
        "transcript_final",
        "error",
        "capture_summary",
        "latency_report",
        "turn_complete",
    ]
    assert emitted[1]["code"] == "CONFIG_ERROR"
    assert emitted[3]["failedStage"] == "llm"
    assert emitted[3]["meta"]["degraded"] is True
    assert emitted[4]["meta"]["degraded"] is True
    assert emitted[4]["meta"]["ttsSkipped"] is True


def test_resolve_recording_path_rejects_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / "secret.jsonl"
    outside.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="outside RECORDINGS_DIR"):
        resolve_recording_path(str(outside), tmp_path)


def test_resolve_recording_path_falls_back_to_basename(tmp_path: Path) -> None:
    (tmp_path / "live-turn.jsonl").write_text("{}\n", encoding="utf-8")
    resolved = resolve_recording_path("samples/live-turn.jsonl", tmp_path)
    assert resolved == (tmp_path / "live-turn.jsonl").resolve()


def test_full_replay_feeds_audio_through_pipeline(
    tmp_path: Path,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    _copy_hello(tmp_path)
    (tmp_path / "hello-turn.pcm").write_bytes(b"\x00\x10" * 1600)
    settings = Settings(recordings_dir=str(tmp_path), langfuse_enabled=False)
    emitted: list[dict] = []

    async def send(event: dict) -> None:
        emitted.append(event)

    request = ReplayRequest(
        recordingPath="hello-turn.jsonl",
        mode="full",
        sessionId="replay-sess",
        turnId="replay-full",
    )
    metadata = asyncio.run(run_replay(request, send, settings))
    assert metadata["turnId"] == "hello-turn"
    types = [event["type"] for event in emitted]
    assert "transcript_final" in types
    assert "llm_complete" in types
    assert "tts_audio_chunk" in types
    assert types[-1] == "turn_complete"
    assert fake_asr.last_session is not None
    assert fake_llm.received == ["hello world"]
    assert fake_tts.received == ["Hi there!"]


def test_full_replay_silent_pcm_uses_saved_transcript(
    tmp_path: Path,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    _copy_hello(tmp_path)
    settings = Settings(recordings_dir=str(tmp_path), langfuse_enabled=False)
    emitted: list[dict] = []

    async def send(event: dict) -> None:
        emitted.append(event)

    request = ReplayRequest(
        recordingPath="hello-turn.jsonl",
        mode="full",
        sessionId="replay-sess",
        turnId="replay-silent",
    )
    asyncio.run(run_replay(request, send, settings))
    assert fake_asr.last_session is None
    assert fake_llm.received == ["hello"]
    assert [event["type"] for event in emitted][-1] == "turn_complete"


def test_mock_replay_skips_asr_and_injects_transcript(
    tmp_path: Path,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    _copy_hello(tmp_path)
    settings = Settings(recordings_dir=str(tmp_path), langfuse_enabled=False)
    emitted: list[dict] = []

    async def send(event: dict) -> None:
        emitted.append(event)

    request = ReplayRequest(
        recordingPath="hello-turn.jsonl",
        mode="mock",
        sessionId="replay-sess",
        turnId="replay-mock",
    )
    asyncio.run(run_replay(request, send, settings))
    assert fake_asr.last_session is None
    assert fake_llm.received == ["hello"]
    assert not any(event["type"] == "transcript_partial" for event in emitted)
    assert emitted[-1]["type"] == "turn_complete"


def test_mock_replay_with_scripted_llm_skips_live_providers(tmp_path: Path) -> None:
    _copy_hello(tmp_path)
    settings = Settings(recordings_dir=str(tmp_path), langfuse_enabled=False)
    emitted: list[dict] = []

    async def send(event: dict) -> None:
        emitted.append(event)

    request = ReplayRequest(
        recordingPath="hello-turn.jsonl",
        mode="mock",
        mockLlm=True,
        sessionId="replay-sess",
        turnId="replay-scripted",
    )
    asyncio.run(run_replay(request, send, settings))
    tokens = [event["text"] for event in emitted if event["type"] == "llm_token"]
    assert tokens == ["Hi", " there"]
    tts = next(event for event in emitted if event["type"] == "tts_audio_chunk")
    assert tts["data"] == "AQIDBA=="
    assert emitted[-1]["type"] == "turn_complete"


def test_full_replay_without_audio_fails(tmp_path: Path) -> None:
    shutil.copy(_DEGRADED, tmp_path / "degraded-llm-config.jsonl")
    settings = Settings(recordings_dir=str(tmp_path), langfuse_enabled=False)

    async def send(_event: dict) -> None:
        return None

    request = ReplayRequest(recordingPath="degraded-llm-config.jsonl", mode="full")
    with pytest.raises(ValueError, match="needs a .pcm capture"):
        asyncio.run(run_replay(request, send, settings))


def test_post_replay_events() -> None:
    client = TestClient(app)
    response = client.post(
        "/replay",
        json={"recordingPath": "samples/degraded-llm-config.jsonl", "mode": "events"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "events"
    assert body["metadata"]["turnId"] == "fixture-turn"
    assert [event["type"] for event in body["events"]][-1] == "turn_complete"


def test_post_replay_rejects_unknown_path() -> None:
    client = TestClient(app)
    response = client.post("/replay", json={"recordingPath": "missing.jsonl", "mode": "events"})
    assert response.status_code == 400


def test_ws_replay_start_events() -> None:
    client = TestClient(app)
    with client.websocket_connect("/ws/session/ws-replay") as websocket:
        websocket.send_json(
            {
                "type": "replay_start",
                "recordingPath": "samples/degraded-llm-config.jsonl",
                "mode": "events",
            }
        )
        emitted: list[dict] = []
        while not emitted or emitted[-1]["type"] != "turn_complete":
            emitted.append(websocket.receive_json())
    assert emitted[0]["sessionId"] == "ws-replay"
    assert [event["type"] for event in emitted][-1] == "turn_complete"


def test_ws_replay_start_bad_path() -> None:
    client = TestClient(app)
    with client.websocket_connect("/ws/session/ws-replay") as websocket:
        websocket.send_json(
            {"type": "replay_start", "recordingPath": "nope.jsonl", "mode": "events"}
        )
        error = websocket.receive_json()
    assert error["type"] == "error"
    assert error["code"] == BAD_REQUEST
    assert error["stage"] == "orchestrator"


def test_ws_full_replay_without_pcm_explains_events_or_mock() -> None:
    client = TestClient(app)
    with client.websocket_connect("/ws/session/ws-replay-full") as websocket:
        websocket.send_json(
            {
                "type": "replay_start",
                "recordingPath": "samples/degraded-llm-config.jsonl",
                "mode": "full",
                "turnId": "ui-full-no-pcm",
            }
        )
        error = websocket.receive_json()
    assert error["type"] == "error"
    assert error["code"] == BAD_REQUEST
    assert error["turnId"] == "ui-full-no-pcm"
    assert "Events or Mock" in error["message"]


def test_cli_events_mode(capsys: pytest.CaptureFixture[str]) -> None:
    code = replay_cli(["samples/degraded-llm-config.jsonl", "--mode", "events"])
    assert code == 0
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert "transcript_final" in lines[0]
    assert "turn_complete" in lines[-1]
    assert "fixture-turn" in captured.err


def test_resolve_recording_path_accepts_turn_id_jsonl(tmp_path: Path) -> None:
    turn_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    (tmp_path / f"{turn_id}.jsonl").write_text("{}\n", encoding="utf-8")
    resolved = resolve_recording_path(f"{turn_id}.jsonl", tmp_path)
    assert resolved == (tmp_path / f"{turn_id}.jsonl").resolve()


def test_client_replayed_error_banner_copy() -> None:
    src = (Path(__file__).parents[1] / "client" / "src" / "replay.ts").read_text(encoding="utf-8")
    assert "export function replayedErrorBanner" in src
    assert "saved fixture; live API keys were not used" in src


def test_client_recording_path_for_turn_id() -> None:
    src = (Path(__file__).parents[1] / "client" / "src" / "replay.ts").read_text(encoding="utf-8")
    assert "export function recordingPathForTurnId" in src
    assert 'id.endsWith(".jsonl") ? id : `${id}.jsonl`' in src


def test_full_replay_disabled_for_no_audio_fixture() -> None:
    src = (Path(__file__).parents[1] / "client" / "src" / "replay.ts").read_text(encoding="utf-8")
    assert "export function fullReplayAvailable" in src
    assert "hasAudio: false" in src
    assert "samples/degraded-llm-config.jsonl" in src


def test_client_bundled_sample_paths_exist() -> None:
    src = Path(__file__).parents[1] / "client" / "src" / "replay.ts"
    paths = re.findall(r'path:\s*"([^"]+\.jsonl)"', src.read_text(encoding="utf-8"))
    assert paths == [
        "samples/hello-turn.jsonl",
        "samples/degraded-llm-config.jsonl",
    ]
    recordings = Path(__file__).parents[1] / "recordings"
    for rel in paths:
        assert (recordings / rel).is_file(), rel


def test_ws_replay_start_uses_client_turn_id() -> None:
    client = TestClient(app)
    with client.websocket_connect("/ws/session/ws-replay-ui") as websocket:
        websocket.send_json(
            {
                "type": "replay_start",
                "recordingPath": "samples/degraded-llm-config.jsonl",
                "mode": "events",
                "turnId": "ui-replay-turn",
            }
        )
        emitted: list[dict] = []
        while not emitted or emitted[-1]["type"] != "turn_complete":
            emitted.append(websocket.receive_json())
    assert all(event["turnId"] == "ui-replay-turn" for event in emitted)
