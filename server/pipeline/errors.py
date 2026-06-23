"""Structured pipeline error codes and user-facing copy (issue #20)."""

from __future__ import annotations

TIMEOUT = "TIMEOUT"
PROVIDER_DOWN = "PROVIDER_DOWN"
STREAM_FAILED = "STREAM_FAILED"
CONFIG_ERROR = "CONFIG_ERROR"
BAD_REQUEST = "BAD_REQUEST"
BREAKER_OPEN = "BREAKER_OPEN"

_RECOVERABLE = frozenset({TIMEOUT, PROVIDER_DOWN, STREAM_FAILED})

# User-visible degradation copy (issue #20 matrix).
MSG_ASR_DOWN = "Couldn't hear you — try again or type below."
MSG_LLM_TIMEOUT = "Sorry, that took too long. Please try again."
MSG_TTS_DOWN = "Audio isn't available — reply shown as text."
MSG_PARTIAL_LLM = "Response was cut short — partial reply shown above."
MSG_CONFIG = "Server configuration is incomplete. Check provider API keys."
MSG_GENERIC = "Something went wrong. Please try again."

MSG_ASR_BREAKER = "Speech recognition unavailable. Try again shortly."
MSG_LLM_BREAKER = "Assistant temporarily unavailable. Try again shortly."
MSG_TTS_BREAKER = "Speech output temporarily unavailable. Replies are text-only."


def breaker_message(stage: str) -> str:
    if stage == "asr":
        return MSG_ASR_BREAKER
    if stage == "llm":
        return MSG_LLM_BREAKER
    if stage == "tts":
        return MSG_TTS_BREAKER
    return MSG_GENERIC


def is_recoverable(code: str) -> bool:
    """Whether the client should allow another push-to-talk attempt."""
    return code in _RECOVERABLE


def error_recoverable(stage: str, code: str) -> bool:
    """Resolve recoverability for a stage error, including breaker-friendly overrides."""
    if code == CONFIG_ERROR and stage in ("asr", "llm", "tts"):
        # Allow in-session retries so repeated hard failures can trip the breaker.
        return True
    return is_recoverable(code)


def is_config_error(exc: BaseException) -> bool:
    """Missing credentials or other non-retryable setup failures."""
    message = str(exc).lower()
    return "missing required provider credentials" in message or "copy .env" in message


def classify_provider_error(exc: BaseException) -> tuple[str, str]:
    """Return ``(code, user_message)`` for a provider failure."""
    if is_config_error(exc):
        return CONFIG_ERROR, MSG_CONFIG
    return PROVIDER_DOWN, MSG_GENERIC
