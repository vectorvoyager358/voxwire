"""Phase 0/1 WebSocket session handler.

Transport-only: accepts the socket, routes control messages, and delegates
capture + pipeline work to :class:`server.pipeline.orchestrator.PipelineOrchestrator`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

from server.pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger("voxwire.ws")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def echo_session(websocket: WebSocket, session_id: str) -> None:
    """Accept a WebSocket and handle Phase 0 control + Phase 1 capture messages."""
    await websocket.accept()
    logger.info("ws connected session=%s", session_id)

    send_lock = asyncio.Lock()

    async def send(payload: dict) -> None:
        async with send_lock:
            await websocket.send_json(payload)

    pipeline = PipelineOrchestrator(session_id, send)

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "ping":
                await send(
                    {
                        "type": "pong",
                        "sessionId": session_id,
                        "timestamp": _now_iso(),
                        "echo": message.get("payload"),
                    }
                )

            elif msg_type == "session_start":
                audio = message.get("audio", {})
                logger.info("session_start session=%s audio=%s", session_id, audio)

            elif msg_type == "audio_chunk":
                await pipeline.on_audio_chunk(message)

            elif msg_type == "utterance_end":
                await pipeline.on_utterance_end(message)

            else:
                await send(
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
    finally:
        await pipeline.close()
