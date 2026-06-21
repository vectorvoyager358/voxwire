"""TTS (text-to-speech) provider interface and the Cartesia implementation.

Issue #10 adds the final pipeline stage: turn the LLM reply text into streamed
audio. As with ASR/LLM, a small provider interface keeps the WebSocket handler
vendor-agnostic and isolates the API key here.

The Cartesia implementation uses the official ``cartesia`` v3 async SSE stream
(``client.tts.sse``) requesting **raw ``pcm_s16le`` @ 24 kHz mono** — exactly
what the client playback queue (issue #7) expects, so chunks can be forwarded
straight through as ``tts_audio_chunk`` events. v1 synthesizes the whole reply
in one request (sentence-chunking is a Phase 2 optimization).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from cartesia import AsyncCartesia

from server.config import Settings

logger = logging.getLogger("voxwire.tts")

# Downstream playback format (docs/event-protocol.md + client/src/playback.ts).
TTS_SAMPLE_RATE = 24000
TTS_ENCODING = "pcm_s16le"

DEFAULT_MODEL = "sonic-3"
# A stock Cartesia voice; swap for a different voice id to change the speaker.
DEFAULT_VOICE_ID = "db6b0ed5-d5d3-463d-ae85-518a07d3c2b4"


class TTSProvider(ABC):
    """Synthesizes speech audio for reply text."""

    @abstractmethod
    def stream(self, text: str) -> AsyncIterator[bytes]:
        """Yield raw ``pcm_s16le`` @ ``TTS_SAMPLE_RATE`` audio chunks in order."""


class CartesiaTTSProvider(TTSProvider):
    """Streams raw PCM via the Cartesia ``tts.sse`` API."""

    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_MODEL,
        voice_id: str = DEFAULT_VOICE_ID,
    ) -> None:
        self._client = AsyncCartesia(api_key=api_key)
        self._model = model
        self._voice_id = voice_id

    async def stream(self, text: str) -> AsyncIterator[bytes]:
        events = await self._client.tts.sse(
            model_id=self._model,
            transcript=text,
            voice={"mode": "id", "id": self._voice_id},
            output_format={
                "container": "raw",
                "encoding": TTS_ENCODING,
                "sample_rate": TTS_SAMPLE_RATE,
            },
        )
        async for event in events:
            if event.type == "chunk":
                audio = event.audio
                if audio:
                    yield audio
            elif event.type == "error":
                raise RuntimeError(getattr(event, "error", "cartesia tts error"))


def get_tts_provider(settings: Settings) -> TTSProvider:
    """Return the configured TTS provider, validating its credential first."""
    provider = settings.tts_provider
    settings.require(provider)
    if provider == "cartesia":
        return CartesiaTTSProvider(api_key=settings.cartesia_api_key or "")
    raise ValueError(f"Unsupported TTS provider '{provider}'.")
