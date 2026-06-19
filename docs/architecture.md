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

## Components

| Path | Responsibility | Phase |
|------|----------------|-------|
| `server/app.py` | FastAPI app, `/health`, WebSocket route | 0 |
| `server/config.py` | Env/settings loader, lazy credential checks | 0 |
| `server/ws/` | WebSocket session handling (echo now, pipeline later) | 0→1 |
| `server/providers/` | ASR / LLM / TTS provider adapters | 1 |
| `server/pipeline/` | `PipelineOrchestrator` wiring stages per turn | 1 |
| `server/latency/` | `LatencyTracker` + budget aggregation | 2 |
| `server/replay/` | Load + replay recorded sessions | 3 |
| `client/` | Mic capture, playback, transcript + latency UI | 0→2 |

## Transport

- One WebSocket per session: `ws://<host>/ws/session/{sessionId}`.
- Messages are JSON envelopes. Phase 0 supports `ping`/`pong` and a generic
  `echo`. The full event protocol is defined in Phase 1 (`docs/event-protocol.md`).

## Design principles

- **Stream everything**: partial transcripts, LLM tokens, and TTS audio flow as
  soon as they're available rather than waiting for completion.
- **Measure at our boundaries**: latency is marked when we send a request / receive
  the first byte, not inside vendor SDKs (Phase 2).
- **Never hang**: every external call is wrapped with a timeout and a fallback
  (Phase 3).
- **Swappable providers**: stages are selected via env vars so any vendor can be
  replaced without touching orchestration code.
