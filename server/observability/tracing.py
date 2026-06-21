"""Langfuse traces per turn with ASR / LLM / TTS spans (issue #18).

No-op when ``LANGFUSE_ENABLED`` is false or credentials are missing so local
dev and CI stay keyless.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Protocol

from server.config import Settings

logger = logging.getLogger("voxwire.tracing")

_SPAN_LEVEL_ERROR = "ERROR"


class TurnTrace(Protocol):
    """Per-turn trace with stage spans; noop implementation satisfies this."""

    def begin_asr(self) -> None: ...

    def end_asr(
        self,
        transcript: str,
        *,
        error: str | None = None,
        latency_ms: int | None = None,
    ) -> None: ...

    def begin_llm(self, user_text: str) -> None: ...

    def end_llm(
        self,
        reply: str,
        *,
        token_count: int = 0,
        error: str | None = None,
        ttft_ms: int | None = None,
        complete_ms: int | None = None,
    ) -> None: ...

    def begin_tts(self) -> None: ...

    def end_tts(
        self,
        *,
        skipped: bool = False,
        chunks: int = 0,
        error: str | None = None,
        ttfb_ms: int | None = None,
        complete_ms: int | None = None,
    ) -> None: ...

    def finish(
        self,
        *,
        transcript: str,
        reply: str,
        token_count: int,
        degraded: bool,
        latency_report: dict[str, Any],
    ) -> None: ...


class _NoopTurnTrace:
    def begin_asr(self) -> None:
        return None

    def end_asr(
        self,
        transcript: str,
        *,
        error: str | None = None,
        latency_ms: int | None = None,
    ) -> None:
        return None

    def begin_llm(self, user_text: str) -> None:
        return None

    def end_llm(
        self,
        reply: str,
        *,
        token_count: int = 0,
        error: str | None = None,
        ttft_ms: int | None = None,
        complete_ms: int | None = None,
    ) -> None:
        return None

    def begin_tts(self) -> None:
        return None

    def end_tts(
        self,
        *,
        skipped: bool = False,
        chunks: int = 0,
        error: str | None = None,
        ttfb_ms: int | None = None,
        complete_ms: int | None = None,
    ) -> None:
        return None

    def finish(
        self,
        *,
        transcript: str,
        reply: str,
        token_count: int,
        degraded: bool,
        latency_report: dict[str, Any],
    ) -> None:
        return None


_NOOP = _NoopTurnTrace()


def _stage_meta(**fields: int | str | bool | None) -> dict[str, Any]:
    return {key: value for key, value in fields.items() if value is not None}


@lru_cache
def _langfuse_client(public_key: str, secret_key: str, host: str | None) -> Any:
    from langfuse import Langfuse

    kwargs: dict[str, str] = {"public_key": public_key, "secret_key": secret_key}
    if host:
        kwargs["host"] = host
    return Langfuse(**kwargs)


class _LangfuseTurnTrace:
    """One Langfuse trace (root span) per push-to-talk turn."""

    def __init__(
        self,
        client: Any,
        *,
        session_id: str,
        turn_id: str,
        llm_model: str,
    ) -> None:
        self._client = client
        self._session_id = session_id
        self._turn_id = turn_id
        self._llm_model = llm_model
        self._asr: Any | None = None
        self._llm: Any | None = None
        self._tts: Any | None = None
        self._root = client.start_observation(
            name="turn",
            as_type="span",
            metadata={
                "sessionId": session_id,
                "turnId": turn_id,
            },
        )

    def _close_span(
        self,
        span: Any | None,
        *,
        output: Any = None,
        metadata: dict[str, Any] | None = None,
        error: str | None = None,
        **extra: Any,
    ) -> None:
        if span is None:
            return
        try:
            update: dict[str, Any] = {}
            if output is not None:
                update["output"] = output
            if metadata:
                update["metadata"] = metadata
            if error:
                update["level"] = _SPAN_LEVEL_ERROR
                update["status_message"] = error
            update.update(extra)
            if update:
                span.update(**update)
            span.end()
        except Exception:  # noqa: BLE001
            logger.exception("langfuse span end failed turn=%s", self._turn_id)

    def begin_asr(self) -> None:
        try:
            self._asr = self._root.start_observation(name="asr", as_type="span")
        except Exception:  # noqa: BLE001
            logger.exception("langfuse asr span start failed turn=%s", self._turn_id)
            self._asr = None

    def end_asr(
        self,
        transcript: str,
        *,
        error: str | None = None,
        latency_ms: int | None = None,
    ) -> None:
        self._close_span(
            self._asr,
            output=transcript or None,
            metadata=_stage_meta(latencyMs=latency_ms),
            error=error,
        )
        self._asr = None

    def begin_llm(self, user_text: str) -> None:
        try:
            self._llm = self._root.start_observation(
                name="llm",
                as_type="generation",
                model=self._llm_model,
                input=user_text,
            )
        except Exception:  # noqa: BLE001
            logger.exception("langfuse llm span start failed turn=%s", self._turn_id)
            self._llm = None

    def end_llm(
        self,
        reply: str,
        *,
        token_count: int = 0,
        error: str | None = None,
        ttft_ms: int | None = None,
        complete_ms: int | None = None,
    ) -> None:
        usage = {"output": token_count} if token_count > 0 else None
        self._close_span(
            self._llm,
            output=reply or None,
            metadata=_stage_meta(ttftMs=ttft_ms, completeMs=complete_ms),
            error=error,
            usage_details=usage,
        )
        self._llm = None

    def begin_tts(self) -> None:
        try:
            self._tts = self._root.start_observation(name="tts", as_type="span")
        except Exception:  # noqa: BLE001
            logger.exception("langfuse tts span start failed turn=%s", self._turn_id)
            self._tts = None

    def end_tts(
        self,
        *,
        skipped: bool = False,
        chunks: int = 0,
        error: str | None = None,
        ttfb_ms: int | None = None,
        complete_ms: int | None = None,
    ) -> None:
        self._close_span(
            self._tts,
            output={"skipped": skipped, "chunks": chunks},
            metadata=_stage_meta(ttfbMs=ttfb_ms, completeMs=complete_ms, skipped=skipped),
            error=error,
        )
        self._tts = None

    def finish(
        self,
        *,
        transcript: str,
        reply: str,
        token_count: int,
        degraded: bool,
        latency_report: dict[str, Any],
    ) -> None:
        try:
            self._root.update(
                input={"transcript": transcript},
                output={"reply": reply, "tokenCount": token_count},
                metadata={
                    "sessionId": self._session_id,
                    "turnId": self._turn_id,
                    "degraded": degraded,
                    "latency": latency_report,
                    "bottleneckStage": latency_report.get("bottleneckStage"),
                    "failedStage": latency_report.get("failedStage"),
                    "totalMs": latency_report.get("totalMs"),
                },
            )
            self._root.end()
            self._client.flush()
        except Exception:  # noqa: BLE001
            logger.exception("langfuse turn trace finish failed turn=%s", self._turn_id)


class TurnTracer:
    """Creates per-turn traces when Langfuse is configured."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Any | None = None
        if settings.langfuse_tracing_enabled:
            self._client = _langfuse_client(
                settings.langfuse_public_key or "",
                settings.langfuse_secret_key or "",
                settings.langfuse_host,
            )

    @property
    def enabled(self) -> bool:
        return self._client is not None

    def noop(self) -> TurnTrace:
        return _NOOP

    def begin_turn(self, session_id: str, turn_id: str) -> TurnTrace:
        if self._client is None:
            return _NOOP
        try:
            from server.providers.llm import DEFAULT_MODEL

            return _LangfuseTurnTrace(
                self._client,
                session_id=session_id,
                turn_id=turn_id,
                llm_model=DEFAULT_MODEL,
            )
        except Exception:  # noqa: BLE001
            logger.exception("langfuse turn trace start failed turn=%s", turn_id)
            return _NOOP


def get_turn_tracer(settings: Settings) -> TurnTracer:
    """Return a tracer for the given settings (cached client when enabled)."""
    return TurnTracer(settings)
