"""Phase 0 WebSocket echo handler.

This proves the transport works before the real pipeline exists. The client can
connect to ``/ws/session/{session_id}`` and round-trip JSON messages. A ``ping``
message is answered with a ``pong`` that echoes the session id and a server
timestamp; any other JSON is echoed back wrapped in an ``echo`` envelope.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("voxwire.ws")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def echo_session(websocket: WebSocket, session_id: str) -> None:
    """Accept a WebSocket and echo JSON messages for the given session."""
    await websocket.accept()
    logger.info("ws connected session=%s", session_id)
    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "ping":
                await websocket.send_json(
                    {
                        "type": "pong",
                        "sessionId": session_id,
                        "timestamp": _now_iso(),
                        "echo": message.get("payload"),
                    }
                )
            else:
                await websocket.send_json(
                    {
                        "type": "echo",
                        "sessionId": session_id,
                        "timestamp": _now_iso(),
                        "received": message,
                    }
                )
    except WebSocketDisconnect:
        logger.info("ws disconnected session=%s", session_id)
    except Exception:  # noqa: BLE001 - log and close, never hang the socket
        logger.exception("ws error session=%s", session_id)
        await websocket.close(code=1011)
