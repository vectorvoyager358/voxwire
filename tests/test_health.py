"""Phase 0 smoke tests: health endpoint and WebSocket echo round-trip."""

from fastapi.testclient import TestClient

from server.app import app

client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "version" in body


def test_ws_ping_pong() -> None:
    with client.websocket_connect("/ws/session/test-session") as ws:
        ws.send_json({"type": "ping", "payload": {"n": 1}})
        message = ws.receive_json()
        assert message["type"] == "pong"
        assert message["sessionId"] == "test-session"
        assert message["echo"] == {"n": 1}
        assert "timestamp" in message


def test_ws_echo_other() -> None:
    with client.websocket_connect("/ws/session/abc") as ws:
        ws.send_json({"type": "hello", "value": 42})
        message = ws.receive_json()
        assert message["type"] == "echo"
        assert message["received"] == {"type": "hello", "value": 42}
