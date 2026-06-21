"""LLM (reply generation) provider interface and the Gemini implementation.

Issue #9 adds the second pipeline stage. As with ASR, a small provider
interface keeps the WebSocket handler vendor-agnostic and isolates the API key
here. The reply is produced with **token streaming** (not wait-for-full) so the
client can render and (later) synthesize speech as text arrives.

The Gemini implementation uses the official ``google-genai`` v2 async streaming
API (``client.aio.models.generate_content_stream``), yielding text deltas. v1
uses a single generic assistant persona with no retrieval/history.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from google import genai
from google.genai import types

from server.config import Settings

logger = logging.getLogger("voxwire.llm")

DEFAULT_MODEL = "gemini-2.5-flash"

# Single generic assistant persona (no RAG in v1). Kept brief because replies
# are spoken back, so concise, plain sentences synthesize and listen better.
SYSTEM_PERSONA = (
    "You are Voxwire, a friendly and concise voice assistant. "
    "Answer in one to three short, natural sentences suitable for being read "
    "aloud. Avoid markdown, lists, code blocks, and emoji."
)


class LLMProvider(ABC):
    """Streams an assistant reply for a user's final transcript."""

    @abstractmethod
    def stream(self, user_text: str) -> AsyncIterator[str]:
        """Yield reply text deltas in order. Concatenated, they form the reply."""


class GeminiLLMProvider(LLMProvider):
    """Token-streaming replies via the Gemini ``generate_content_stream`` API."""

    def __init__(self, api_key: str, model: str = DEFAULT_MODEL) -> None:
        self._client = genai.Client(api_key=api_key)
        self._model = model
        self._config = types.GenerateContentConfig(system_instruction=SYSTEM_PERSONA)

    async def stream(self, user_text: str) -> AsyncIterator[str]:
        response = await self._client.aio.models.generate_content_stream(
            model=self._model,
            contents=user_text,
            config=self._config,
        )
        async for chunk in response:
            text = getattr(chunk, "text", None)
            if text:
                yield text


def get_llm_provider(settings: Settings) -> LLMProvider:
    """Return the configured LLM provider, validating its credential first."""
    provider = settings.llm_provider
    settings.require(provider)
    if provider == "gemini":
        return GeminiLLMProvider(api_key=settings.gemini_api_key or "")
    raise ValueError(f"Unsupported LLM provider '{provider}'.")
