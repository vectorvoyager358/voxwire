"""Per-session pipeline orchestrator (issue #11).

Sequences ASR -> LLM -> TTS for each push-to-talk turn, emitting protocol events
via a caller-supplied ``send`` callback. The WebSocket handler stays transport-
only; all stage logic and per-turn state live here.

Turn lifecycle::

    idle -> capturing -> asr -> llm -> tts -> complete
                      |       |      |
                      v       v      v
                   degraded (error emitted, turn still completes)

Stage timeouts and bounded transient retry (issue #19).
"""

from __future__ import annotations

import base64
import binascii
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from server.config import Settings, get_settings
from server.latency import LatencyTracker
from server.observability import TurnTrace, get_turn_tracer
from server.providers.asr import ASRSession, Transcript, get_asr_provider
from server.providers.llm import get_llm_provider
from server.providers.tts import TTS_ENCODING, TTS_SAMPLE_RATE, get_tts_provider
from server.replay.recorder import TurnRecorder
from server.resilience import StageTimeoutError, run_with_timeout_and_retry, stream_with_retry

logger = logging.getLogger("voxwire.pipeline")

Send = Callable[[dict], Awaitable[None]]

ASR_TIMEOUT_MESSAGE = "ASR provider did not respond in time. Please try again."
LLM_TIMEOUT_FALLBACK = "Sorry, I'm having trouble responding right now. Please try again."


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TurnStats:
    """Per-utterance accumulator for capture diagnostics."""

    turn_id: str | None = None
    chunks: int = 0
    bytes: int = 0
    next_seq: int = 0
    gaps: list[int] = field(default_factory=list)

    def reset(self, turn_id: str) -> None:
        self.turn_id = turn_id
        self.chunks = 0
        self.bytes = 0
        self.next_seq = 0
        self.gaps = []

    def accumulate_chunk(self, message: dict) -> None:
        """Count one ``audio_chunk``, tracking sequence gaps and byte size."""
        turn_id = message.get("turnId")
        if self.turn_id != turn_id:
            self.reset(turn_id)

        seq = message.get("seq", self.next_seq)
        if seq != self.next_seq:
            self.gaps.append(seq)
        self.next_seq = seq + 1

        data = message.get("data", "")
        try:
            self.bytes += len(base64.b64decode(data, validate=True))
        except (binascii.Error, ValueError):
            logger.warning("bad base64 in audio_chunk turn=%s seq=%s", turn_id, seq)
        self.chunks += 1

    def finish(self, message: dict) -> dict:
        """Build a summary dict for ``utterance_end`` and reset the accumulator."""
        declared = message.get("totalChunks")
        summary = {
            "turnId": message.get("turnId"),
            "received": self.chunks,
            "declared": declared,
            "bytes": self.bytes,
            "clean": not self.gaps and (declared is None or declared == self.chunks),
        }
        self.turn_id = None
        return summary


class PipelineOrchestrator:
    """One orchestrator per WebSocket session; reusable across turns."""

    def __init__(self, session_id: str, send: Send, settings: Settings | None = None) -> None:
        self._session_id = session_id
        self._send = send
        self._settings = settings if settings is not None else get_settings()
        self._stats = TurnStats()
        self._asr_session: ASRSession | None = None
        self._asr_turn_id: str | None = None
        self._degraded = False
        self._recorder = TurnRecorder(
            session_id,
            Path(self._settings.recordings_dir),
        )
        self._latency = LatencyTracker()
        self._latency_turn_id: str | None = None
        self._tracer = get_turn_tracer(self._settings)
        self._turn_trace: TurnTrace | None = None

    async def _emit(self, payload: dict) -> None:
        """Send a protocol event to the client and append it to the turn trace."""
        await self._send(payload)
        turn_id = payload.get("turnId")
        if turn_id and turn_id == self._recorder._turn_id:
            self._recorder.record_event(payload)

    def _begin_turn_tracking(self, turn_id: str) -> None:
        if self._latency_turn_id != turn_id:
            self._latency.begin_turn()
            self._latency_turn_id = turn_id

    async def on_audio_chunk(self, message: dict) -> None:
        """Stream one upstream audio chunk to the ASR provider."""
        self._stats.accumulate_chunk(message)
        turn_id = message.get("turnId")
        if turn_id:
            self._recorder.begin_turn(turn_id)
            self._begin_turn_tracking(turn_id)
            self._latency.mark("first_audio_chunk")
            self._latency.mark("last_audio_chunk")
        if self._asr_session is None or self._asr_turn_id != turn_id:
            if self._asr_session is not None:
                await self._asr_session.aclose()
                self._asr_session = None
            self._asr_session = await self._start_asr(turn_id)
            self._asr_turn_id = turn_id
        if self._asr_session is not None:
            await self._forward_audio(self._asr_session, turn_id, message)

    async def on_utterance_end(self, message: dict) -> None:
        """Finalize ASR, run LLM + TTS, emit capture_summary and turn_complete."""
        self._degraded = False
        turn_id = message.get("turnId")
        if turn_id:
            self._begin_turn_tracking(turn_id)

        capture_ms = message.get("captureMs")
        if isinstance(capture_ms, (int, float)) and capture_ms >= 0:
            self._latency.set_client_capture_ms(int(capture_ms))
        self._latency.mark("utterance_end")

        if turn_id:
            trace = self._tracer.begin_turn(self._session_id, turn_id)
        else:
            trace = self._tracer.noop()
        self._turn_trace = trace
        trace.begin_asr()

        transcript = ""
        asr_error: str | None = None
        if self._asr_session is not None:
            transcript, asr_error = await self._finalize_asr(self._asr_session, turn_id)
            self._asr_session = None
            self._asr_turn_id = None

        trace.end_asr(
            transcript,
            error=asr_error,
            latency_ms=self._latency.ms_since_utterance_end("asr_final"),
        )

        reply = ""
        token_count = 0
        llm_error: str | None = None
        if transcript:
            trace.begin_llm(transcript)
            reply, token_count, llm_error = await self._run_llm(turn_id, transcript)
            trace.end_llm(
                reply,
                token_count=token_count,
                error=llm_error,
                ttft_ms=self._latency.ms_since_utterance_end("llm_first_token"),
                complete_ms=self._latency.ms_since_utterance_end("llm_complete"),
            )

        summary = self._stats.finish(message)
        logger.info("utterance_end session=%s %s", self._session_id, summary)
        await self._emit(
            {
                "type": "capture_summary",
                "sessionId": self._session_id,
                "turnId": summary["turnId"],
                "timestamp": _now_iso(),
                **summary,
            }
        )
        tts_meta = await self._stream_tts(
            turn_id,
            reply,
            trace,
            transcript=transcript,
            token_count=token_count,
        )
        self._recorder.persist(
            transcript=transcript,
            reply=reply,
            token_count=token_count,
            degraded=self._degraded,
            **tts_meta,
        )

    async def close(self) -> None:
        """Release in-flight provider sessions (e.g. on WebSocket disconnect)."""
        if self._asr_session is not None:
            await self._asr_session.aclose()
            self._asr_session = None
            self._asr_turn_id = None

    async def _emit_error(
        self,
        turn_id: str | None,
        stage: str,
        code: str,
        detail: str,
        *,
        recoverable: bool = True,
    ) -> None:
        self._degraded = True
        self._latency.set_failed_stage(stage)
        await self._emit(
            {
                "type": "error",
                "sessionId": self._session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                "stage": stage,
                "code": code,
                "recoverable": recoverable,
                "message": detail,
            }
        )

    async def _start_asr(self, turn_id: str | None) -> ASRSession | None:
        async def on_transcript(transcript: Transcript) -> None:
            if not transcript.text:
                return
            if self._latency.anchor_set:
                self._latency.mark("asr_first_partial")
            await self._emit(
                {
                    "type": "transcript_partial",
                    "sessionId": self._session_id,
                    "turnId": turn_id,
                    "timestamp": _now_iso(),
                    "text": transcript.text,
                }
            )

        try:
            provider = get_asr_provider(self._settings)
            return await run_with_timeout_and_retry(
                lambda: provider.start(on_transcript),
                stage="asr",
                timeout_s=self._settings.asr_timeout_s,
                backoff_ms=self._settings.retry_backoff_ms,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("asr start failed session=%s turn=%s", self._session_id, turn_id)
            await self._emit_error(turn_id, "asr", "ASR_UNAVAILABLE", str(exc))
            return None

    async def _forward_audio(self, session: ASRSession, turn_id: str | None, message: dict) -> None:
        try:
            pcm = base64.b64decode(message.get("data", ""), validate=True)
        except (binascii.Error, ValueError):
            return
        self._recorder.append_audio(pcm)
        self._latency.mark("asr_start")
        try:
            await session.send_audio(pcm)
        except Exception:  # noqa: BLE001
            logger.warning("asr send_audio failed session=%s turn=%s", self._session_id, turn_id)

    async def _finalize_asr(
        self, session: ASRSession, turn_id: str | None
    ) -> tuple[str, str | None]:
        text = ""
        error: str | None = None
        try:
            text = await run_with_timeout_and_retry(
                session.finalize,
                stage="asr",
                timeout_s=self._settings.asr_timeout_s,
                backoff_ms=self._settings.retry_backoff_ms,
            )
            self._latency.mark("asr_final")
            await self._emit(
                {
                    "type": "transcript_final",
                    "sessionId": self._session_id,
                    "turnId": turn_id,
                    "timestamp": _now_iso(),
                    "text": text,
                }
            )
        except StageTimeoutError:
            error = ASR_TIMEOUT_MESSAGE
            text = ""
            logger.warning("asr timeout session=%s turn=%s", self._session_id, turn_id)
            await self._emit_error(
                turn_id,
                "asr",
                "TIMEOUT",
                f"ASR provider did not respond within {self._settings.asr_timeout_s:.0f}s",
            )
            await self._emit(
                {
                    "type": "transcript_final",
                    "sessionId": self._session_id,
                    "turnId": turn_id,
                    "timestamp": _now_iso(),
                    "text": text,
                }
            )
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.exception("asr finalize failed session=%s turn=%s", self._session_id, turn_id)
            await self._emit_error(turn_id, "asr", "ASR_FINALIZE_FAILED", error)
        finally:
            await session.aclose()
        return text, error

    async def _run_llm(self, turn_id: str | None, user_text: str) -> tuple[str, int, str | None]:
        try:
            provider = get_llm_provider(self._settings)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.exception("llm start failed session=%s turn=%s", self._session_id, turn_id)
            await self._emit_error(turn_id, "llm", "LLM_UNAVAILABLE", error)
            return "", 0, error

        parts: list[str] = []
        index = 0
        error: str | None = None
        try:
            self._latency.mark("llm_start")
            async for token in stream_with_retry(
                lambda: provider.stream(user_text),
                stage="llm",
                timeout_s=self._settings.llm_timeout_s,
                ttft_timeout_s=self._settings.llm_ttft_timeout_s,
                backoff_ms=self._settings.retry_backoff_ms,
            ):
                if not token:
                    continue
                self._latency.mark("llm_first_token")
                parts.append(token)
                await self._emit(
                    {
                        "type": "llm_token",
                        "sessionId": self._session_id,
                        "turnId": turn_id,
                        "timestamp": _now_iso(),
                        "index": index,
                        "text": token,
                    }
                )
                index += 1
        except StageTimeoutError as exc:
            error = str(exc)
            if exc.ttft:
                detail = (
                    f"LLM did not produce a first token within "
                    f"{self._settings.llm_ttft_timeout_s:.0f}s"
                )
            else:
                detail = f"LLM did not finish within {self._settings.llm_timeout_s:.0f}s"
            logger.warning(
                "llm timeout session=%s turn=%s partial=%s",
                self._session_id,
                turn_id,
                index,
            )
            await self._emit_error(turn_id, "llm", "TIMEOUT", detail)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.exception("llm stream failed session=%s turn=%s", self._session_id, turn_id)
            await self._emit_error(turn_id, "llm", "LLM_STREAM_FAILED", error)

        full = "".join(parts) if parts else (LLM_TIMEOUT_FALLBACK if error else "")
        self._latency.mark("llm_complete")
        await self._emit(
            {
                "type": "llm_complete",
                "sessionId": self._session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                "text": full,
            }
        )
        return full, index, error

    async def _stream_tts(
        self,
        turn_id: str | None,
        text: str,
        trace: TurnTrace,
        *,
        transcript: str,
        token_count: int,
    ) -> dict[str, int | bool]:
        seq = 0
        skipped = not text
        tts_error: str | None = None
        self._latency.set_tts_skipped(skipped)
        trace.begin_tts()

        if text:
            try:
                provider = get_tts_provider(self._settings)
            except Exception as exc:  # noqa: BLE001
                tts_error = str(exc)
                logger.exception("tts start failed session=%s turn=%s", self._session_id, turn_id)
                await self._emit_error(turn_id, "tts", "TTS_UNAVAILABLE", tts_error)
                provider = None

            if provider is not None:
                try:
                    self._latency.mark("tts_start")
                    async for pcm in stream_with_retry(
                        lambda: provider.stream(text),
                        stage="tts",
                        timeout_s=self._settings.tts_timeout_s,
                        backoff_ms=self._settings.retry_backoff_ms,
                    ):
                        if not pcm:
                            continue
                        self._latency.mark("tts_first_byte")
                        await self._emit(
                            {
                                "type": "tts_audio_chunk",
                                "sessionId": self._session_id,
                                "turnId": turn_id,
                                "timestamp": _now_iso(),
                                "seq": seq,
                                "encoding": TTS_ENCODING,
                                "sampleRate": TTS_SAMPLE_RATE,
                                "data": base64.b64encode(pcm).decode(),
                            }
                        )
                        seq += 1
                except StageTimeoutError as exc:
                    tts_error = str(exc)
                    logger.warning(
                        "tts timeout session=%s turn=%s chunks=%s",
                        self._session_id,
                        turn_id,
                        seq,
                    )
                    await self._emit_error(
                        turn_id,
                        "tts",
                        "TIMEOUT",
                        (
                            f"TTS did not finish within {self._settings.tts_timeout_s:.0f}s; "
                            "reply shown as text only"
                        ),
                    )
                except Exception as exc:  # noqa: BLE001
                    tts_error = str(exc)
                    logger.exception(
                        "tts stream failed session=%s turn=%s", self._session_id, turn_id
                    )
                    await self._emit_error(turn_id, "tts", "TTS_STREAM_FAILED", tts_error)
                else:
                    self._latency.mark("tts_complete")

        self._latency.mark("turn_complete")
        latency_report = self._latency.build_report(degraded=self._degraded)
        trace.end_tts(
            skipped=skipped,
            chunks=seq,
            error=tts_error,
            ttfb_ms=self._latency.ms_since_utterance_end("tts_first_byte"),
            complete_ms=self._latency.ms_since_utterance_end("tts_complete"),
        )
        trace.finish(
            transcript=transcript,
            reply=text,
            token_count=token_count,
            degraded=self._degraded,
            latency_report=latency_report,
        )
        self._turn_trace = None
        logger.info(
            "latency turn=%s total_ms=%s bottleneck=%s failed=%s",
            turn_id,
            latency_report["totalMs"],
            latency_report["bottleneckStage"],
            latency_report["failedStage"],
        )
        await self._emit(
            {
                "type": "latency_report",
                "sessionId": self._session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                **latency_report,
            }
        )
        await self._emit(
            {
                "type": "turn_complete",
                "sessionId": self._session_id,
                "turnId": turn_id,
                "timestamp": _now_iso(),
                "meta": {
                    "degraded": self._degraded,
                    "ttsSkipped": skipped,
                    "ttsChunks": seq,
                    "latency": latency_report["meta"],
                    "latency_report": latency_report,
                },
            }
        )
        self._latency_turn_id = None
        return {"tts_skipped": skipped, "tts_chunks": seq}
