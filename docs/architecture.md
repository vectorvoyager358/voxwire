# Architecture

## Overview

voxwire is a realtime voice assistant. Audio is captured in the browser, streamed
over a WebSocket to a FastAPI orchestrator that pipes it through three external
stages — **ASR → LLM → TTS** — and streams transcript, tokens, audio, and latency
events back to the client.

```
┌──────────┐   audio_chunk    ┌─────────────────────────┐
│ Browser  │ ───────────────► │  FastAPI WS orchestrator │
│  client  │ ◄─────────────── │                          │
└──────────┘  transcript/     │   ┌──────┐ ┌──────┐ ┌──────┐
   mic +      llm_token/      │   │ ASR  │→│ LLM  │→│ TTS  │
   playback   tts_audio/      │   └──────┘ └──────┘ └──────┘
              latency_report  └─────────────────────────┘
```

Typed input (`text_turn`) skips ASR and starts at the LLM. Failures degrade the
turn instead of hanging the socket; see the [README](../README.md#failure-modes).

## Components

| Path | Responsibility |
|------|----------------|
| `server/app.py` | FastAPI app, `/health`, `POST /replay`, WebSocket route |
| `server/config.py` | Env/settings loader, lazy credential checks, timeouts, breaker |
| `server/ws/` | WebSocket session: ping/pong, idle timeout, dispatch to orchestrator |
| `server/pipeline/` | `PipelineOrchestrator` + structured error codes |
| `server/providers/` | ASR (Deepgram), LLM (Gemini), TTS (Cartesia) adapters |
| `server/latency/` | `LatencyTracker` + budget aggregation |
| `server/resilience/` | Stage deadlines, one transient retry, per-stage circuit breaker |
| `server/observability/` | Optional Langfuse traces (no-op when unset) |
| `server/replay/` | `TurnRecorder`, JSONL+PCM loader, event/full/mock replay, CLI |
| `client/` | Mic capture, playback, chat, replay controls, error banners, latency waterfall |
| `recordings/` | Live traces; `samples/` holds CI fixtures |

## Transport

- One WebSocket per session: `ws://<host>/ws/session/{sessionId}`.
- Messages are JSON envelopes. The event protocol is
  [`event-protocol.md`](event-protocol.md).
- Unknown client `type` values still get a generic `echo`.
- After `WS_IDLE_TIMEOUT_S` with no frames, the server closes the socket.

## Pipeline

`PipelineOrchestrator` owns per-session turn state. One utterance:

1. `audio_chunk`s are forwarded to a fresh ASR session; partials stream back.
2. `utterance_end` finalizes ASR, then streams LLM tokens, then synthesizes TTS
   from the **full** reply (TTS does not start until `llm_complete`).
3. `capture_summary`, `latency_report`, and `turn_complete` always fire.

Stages are wrapped in `run_with_timeout_and_retry` / `stream_with_retry`. Hard
failures increment a `StageBreakers` instance for that WebSocket session.

## Design principles

- **Stream everything**: partial transcripts, LLM tokens, and TTS audio flow as
  soon as they're available rather than waiting for the whole pipeline.
- **Measure at our boundaries**: latency is marked when we send a request /
  receive the first byte, not inside vendor SDKs. See
  [`latency-budget.md`](latency-budget.md).
- **Never hang**: every external call has a deadline; the turn still completes.
- **Degrade, then continue**: a failed stage emits `error` and skips work that
  depends on it. TTS failure is text-only, not a frozen UI.
- **Swappable providers**: stages are selected via env vars so a vendor can be
  replaced without touching orchestration code. Shipped adapters: Deepgram,
  Gemini, Cartesia.
