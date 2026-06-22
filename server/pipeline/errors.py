"""Structured pipeline error codes and user-facing copy (issue #20)."""

from __future__ import annotations

TIMEOUT = "TIMEOUT"
PROVIDER_DOWN = "PROVIDER_DOWN"
STREAM_FAILED = "STREAM_FAILED"
CONFIG_ERROR = "CONFIG_ERROR"
BAD_REQUEST = "BAD_REQUEST"

_RECOVERABLE = frozenset({TIMEOUT, PROVIDER_DOWN, STREAM_FAILED})

# User-visible degradation copy (issue #20 matrix).
MSG_ASR_DOWN = "Couldn't hear you — try again or type below."
MSG_LLM_TIMEOUT = "Sorry, that took too long. Please try again."
MSG_TTS_DOWN = "Audio isn't available — reply shown as text."
MSG_PARTIAL_LLM = "Response was cut short — partial reply shown above."
MSG_CONFIG = "Server configuration is incomplete. Check provider API keys."
MSG_GENERIC = "Something went wrong. Please try again."


def is_recoverable(code: str) -> bool:
    """Whether the client should allow another push-to-talk attempt."""
    return code in _RECOVERABLE


def is_config_error(exc: BaseException) -> bool:
    """Missing credentials or other non-retryable setup failures."""
    message = str(exc).lower()
    return "missing required provider credentials" in message or "copy .env" in message


def classify_provider_error(exc: BaseException) -> tuple[str, str]:
    """Return ``(code, user_message)`` for a provider failure."""
    if is_config_error(exc):
        return CONFIG_ERROR, MSG_CONFIG
    return PROVIDER_DOWN, MSG_GENERIC
