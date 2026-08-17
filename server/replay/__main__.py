"""CLI: ``python -m server.replay recordings/samples/hello-turn.jsonl``."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from server.config import get_settings
from server.replay.replayer import ReplayRequest, run_replay


async def _print(event: dict) -> None:
    print(json.dumps(event, ensure_ascii=False, default=str), flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay a recorded voxwire turn with no live microphone.",
    )
    parser.add_argument("recording", help="JSONL path under RECORDINGS_DIR")
    parser.add_argument(
        "--mode",
        choices=("events", "full", "mock"),
        default="events",
        help="events: emit saved protocol frames; full: PCM through ASR→LLM→TTS; "
        "mock: skip ASR and inject transcript_final",
    )
    parser.add_argument(
        "--mock-llm",
        action="store_true",
        help="with --mode mock, replay recorded LLM/TTS instead of calling providers",
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    request = ReplayRequest(
        recordingPath=args.recording,
        mode=args.mode,
        mockLlm=args.mock_llm,
        sessionId="replay-cli",
    )

    async def run() -> dict:
        return await run_replay(request, _print, settings)

    try:
        metadata = asyncio.run(run())
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(metadata, ensure_ascii=False, default=str), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
