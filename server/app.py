"""voxwire FastAPI application.

Phase 0: a health endpoint and a WebSocket echo route to validate the transport.
Run locally with::

    python -m server.app
    # or
    uvicorn server.app:app --reload
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from server import __version__
from server.config import get_settings
from server.ws.echo import echo_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("voxwire")

app = FastAPI(title="voxwire", version=__version__)

# Dev convenience: the Vite client runs on a different port.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe with version info."""
    return {"status": "ok", "version": __version__}


@app.websocket("/ws/session/{session_id}")
async def ws_session(websocket: WebSocket, session_id: str) -> None:
    """Phase 0 echo session. Replaced by the pipeline orchestrator in Phase 1."""
    await echo_session(websocket, session_id)


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "server.app:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        reload=True,
    )


if __name__ == "__main__":
    main()
