"""Configuration / environment loading.

Provider keys are loaded from the environment (and a local ``.env`` file).
The settings object intentionally does NOT crash at import time when keys are
missing, because Phase 0 only needs the transport layer. Instead, call
:func:`Settings.require` for the providers a given stage actually needs so we
"fail fast with a clear message" the moment a stage that needs a key runs.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the voxwire server.

    Values come from environment variables or a ``.env`` file at the repo root.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- App ---
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"
    recordings_dir: str = "recordings"

    # --- Provider selection (swap stages without touching code) ---
    asr_provider: str = "deepgram"  # deepgram
    llm_provider: str = "gemini"  # gemini
    tts_provider: str = "cartesia"  # cartesia

    # --- Provider credentials (optional until the stage runs) ---
    deepgram_api_key: str | None = None
    gemini_api_key: str | None = None
    cartesia_api_key: str | None = None

    # --- Observability (Phase 2, optional) ---
    langfuse_enabled: bool = False
    langfuse_public_key: str | None = None
    langfuse_secret_key: str | None = None
    langfuse_host: str | None = None

    @property
    def langfuse_tracing_enabled(self) -> bool:
        """True when tracing is explicitly enabled and credentials are present."""
        return bool(self.langfuse_enabled and self.langfuse_public_key and self.langfuse_secret_key)

    # Maps a logical credential name to the attribute that holds it.
    _KEY_ATTRS = {
        "deepgram": "deepgram_api_key",
        "gemini": "gemini_api_key",
        "cartesia": "cartesia_api_key",
        "elevenlabs": "elevenlabs_api_key",
    }

    def require(self, *providers: str) -> None:
        """Ensure the given providers have credentials, else raise clearly.

        Example::

            settings.require(settings.asr_provider, settings.llm_provider)
        """
        missing: list[str] = []
        for provider in providers:
            attr = self._KEY_ATTRS.get(provider)
            if attr is None:
                raise ValueError(f"Unknown provider '{provider}'.")
            if not getattr(self, attr):
                env_name = attr.upper()
                missing.append(f"{provider} -> set {env_name}")
        if missing:
            details = "\n  ".join(missing)
            raise RuntimeError(
                "Missing required provider credentials:\n  "
                f"{details}\n"
                "Copy .env.example to .env and fill in the keys."
            )


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
