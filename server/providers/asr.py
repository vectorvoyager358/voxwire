"""ASR (speech-to-text) provider interface and the Deepgram implementation.

Issue #8 adds the first real pipeline stage. A small provider interface keeps
the WebSocket handler ignorant of which vendor is used (and isolates the API
key here, not in the orchestrator). The Deepgram implementation uses the
official ``deepgram-sdk`` v7 **Listen v1** streaming socket, which supports
interim results and an explicit *finalize* control message — a clean match for
push-to-talk, where releasing the button means "flush what you have".

Each push-to-talk utterance gets its own :class:`ASRSession`:

- ``send_audio(pcm)``  -> forward raw PCM16 frames as they arrive,
- partial hypotheses are pushed back via the ``on_transcript`` callback,
- ``finalize()``       -> flush and return the single final transcript,
- ``aclose()``         -> tear the connection down.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from deepgram import AsyncDeepgramClient
from deepgram.core.events import EventType

from server.config import Settings

logger = logging.getLogger("voxwire.asr")

# Upstream capture format (docs/event-protocol.md: 16 kHz mono PCM16).
ASR_SAMPLE_RATE = 16000
ASR_CHANNELS = 1
DEFAULT_MODEL = "nova-3"

# How long finalize() waits for Deepgram to flush the post-finalize result.
_FINALIZE_TIMEOUT_S = 3.0


@dataclass
class Transcript:
    """One ASR hypothesis. ``is_final`` segments are committed (won't change)."""

    text: str
    is_final: bool


OnTranscript = Callable[[Transcript], Awaitable[None]]


class ASRSession(ABC):
    """A single in-progress utterance's streaming recognition."""

    @abstractmethod
    async def send_audio(self, pcm: bytes) -> None:
        """Forward one chunk of raw PCM16 audio to the recognizer."""

    @abstractmethod
    async def finalize(self) -> str:
        """Flush the recognizer and return the utterance's final transcript."""

    @abstractmethod
    async def aclose(self) -> None:
        """Release the underlying connection. Safe to call more than once."""


class ASRProvider(ABC):
    """Factory for per-utterance :class:`ASRSession` objects."""

    @abstractmethod
    async def start(self, on_transcript: OnTranscript) -> ASRSession:
        """Open a recognition session, routing partials to ``on_transcript``."""


class DeepgramASRSession(ASRSession):
    """Listen v1 streaming session for one push-to-talk utterance."""

    def __init__(self, connection_cm, on_transcript: OnTranscript) -> None:
        self._cm = connection_cm
        self._on_transcript = on_transcript
        self._connection = None
        self._listen_task: asyncio.Task | None = None
        # Committed (is_final) text segments, joined into the final transcript.
        self._final_parts: list[str] = []
        # Set once the flushed, post-finalize result has been observed.
        self._final_event = asyncio.Event()
        self._closed = False

    async def _open(self) -> None:
        self._connection = await self._cm.__aenter__()
        self._connection.on(EventType.MESSAGE, self._on_message)
        self._connection.on(EventType.ERROR, self._on_error)
        # start_listening() pumps the socket and dispatches the events above.
        self._listen_task = asyncio.create_task(self._connection.start_listening())

    async def _on_message(self, message) -> None:
        if getattr(message, "type", None) != "Results":
            return

        channel = getattr(message, "channel", None)
        alternatives = getattr(channel, "alternatives", None) or []
        text = alternatives[0].transcript.strip() if alternatives else ""

        is_final = bool(getattr(message, "is_final", False))
        from_finalize = bool(getattr(message, "from_finalize", False))
        speech_final = bool(getattr(message, "speech_final", False))

        if text:
            if is_final:
                self._final_parts.append(text)
            else:
                # Interim hypothesis -> live partial for the client display.
                await self._safe_emit(Transcript(text=text, is_final=False))

        # The flushed result after send_finalize() unblocks finalize().
        if from_finalize or speech_final:
            self._final_event.set()

    async def _on_error(self, error) -> None:
        logger.warning("deepgram stream error: %s", error)
        # Don't strand finalize() on a dead connection.
        self._final_event.set()

    async def _safe_emit(self, transcript: Transcript) -> None:
        try:
            await self._on_transcript(transcript)
        except Exception:  # noqa: BLE001 - a consumer error must not kill the stream
            logger.exception("on_transcript callback failed")

    async def send_audio(self, pcm: bytes) -> None:
        if self._connection is None or self._closed:
            return
        await self._connection.send_media(pcm)

    async def finalize(self) -> str:
        if self._connection is not None and not self._closed:
            await self._connection.send_finalize()
            try:
                await asyncio.wait_for(self._final_event.wait(), _FINALIZE_TIMEOUT_S)
            except asyncio.TimeoutError:
                logger.warning("deepgram finalize timed out after %ss", _FINALIZE_TIMEOUT_S)
        return " ".join(self._final_parts).strip()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._connection is not None:
            try:
                await self._connection.send_close_stream()
            except Exception:  # noqa: BLE001 - best-effort close
                logger.debug("send_close_stream failed during aclose", exc_info=True)
        if self._listen_task is not None:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        try:
            await self._cm.__aexit__(None, None, None)
        except Exception:  # noqa: BLE001 - best-effort close
            logger.debug("connection __aexit__ failed during aclose", exc_info=True)


class DeepgramASRProvider(ASRProvider):
    """Opens Deepgram Listen v1 streaming connections (one per utterance)."""

    def __init__(self, api_key: str, model: str = DEFAULT_MODEL) -> None:
        self._client = AsyncDeepgramClient(api_key=api_key)
        self._model = model

    async def start(self, on_transcript: OnTranscript) -> ASRSession:
        connection_cm = self._client.listen.v1.connect(
            model=self._model,
            encoding="linear16",
            sample_rate=ASR_SAMPLE_RATE,
            channels=ASR_CHANNELS,
            interim_results=True,
            smart_format=True,
        )
        session = DeepgramASRSession(connection_cm, on_transcript)
        await session._open()
        return session


def get_asr_provider(settings: Settings) -> ASRProvider:
    """Return the configured ASR provider, validating its credential first."""
    provider = settings.asr_provider
    settings.require(provider)
    if provider == "deepgram":
        return DeepgramASRProvider(api_key=settings.deepgram_api_key or "")
    raise ValueError(f"Unsupported ASR provider '{provider}'.")
