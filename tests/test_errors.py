"""Issue #20 tests: error code helpers."""

from __future__ import annotations

from server.pipeline.errors import (
    CONFIG_ERROR,
    PROVIDER_DOWN,
    TIMEOUT,
    classify_provider_error,
    error_recoverable,
    is_recoverable,
)


def test_recoverable_codes() -> None:
    assert is_recoverable(TIMEOUT) is True
    assert is_recoverable(PROVIDER_DOWN) is True
    assert is_recoverable(CONFIG_ERROR) is False


def test_error_recoverable_by_stage() -> None:
    assert error_recoverable("asr", CONFIG_ERROR) is True
    assert error_recoverable("llm", CONFIG_ERROR) is True
    assert error_recoverable("tts", CONFIG_ERROR) is True
    assert error_recoverable("orchestrator", CONFIG_ERROR) is False


def test_classify_config_error() -> None:
    exc = RuntimeError("Missing required provider credentials:\n  gemini -> set GEMINI_API_KEY")
    code, _message = classify_provider_error(exc)
    assert code == CONFIG_ERROR
