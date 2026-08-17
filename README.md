# voxwire

A standalone **realtime voice assistant pipeline**: speak into the browser, get a
spoken reply back, with a per-turn **latency budget** and **production-grade
resilience** (timeouts, graceful degradation, circuit breakers, replay fixtures).

```
Browser mic → WebSocket → Server orchestrator
                              ├→ ASR  (speech → text, streaming)
                              ├→ LLM  (streaming tokens)
                              └→ TTS  (text → audio, streaming)
                         ← WebSocket ← audio + status + latency events
```

## Tech stack

| Layer | Choice | Why |
|-------|--------|-----|
| Server | **FastAPI + uvicorn** | First-class async + WebSockets |
| Transport | **WebSockets** | Continuous bidirectional streaming |
| Client | **Vite + TypeScript** (vanilla) | Fast dev server, typed, light (no framework) |
| ASR | **Deepgram** (`nova-3`) | Streaming Listen v1 with explicit finalize |
| LLM | **Gemini** (`gemini-2.5-flash`) | Token streaming on the free tier |
| TTS | **Cartesia** (`sonic-3`) | Low-latency streaming PCM |

Each stage is a small provider interface selected by env vars
(`ASR_PROVIDER`, `LLM_PROVIDER`, `TTS_PROVIDER`). The shipped adapters are
Deepgram / Gemini / Cartesia. Credentials load lazily: the server boots without
keys, and a stage fails fast with a structured `error` if its key is missing.

### Cost: runs free for development

The default stack can be built and demoed at **$0**, no credit card required:

| Stage | Provider | Free allowance |
|-------|----------|----------------|
| ASR | Deepgram | $200 free credit (~433 hrs of streaming), no card, no expiry |
| LLM | Gemini | Free tier on Flash models (~1,500 requests/day), no card |
| TTS | Cartesia | 20K credits/mo (~27 min of speech), no card, non-commercial |

## Local setup

### Prerequisites
- Python 3.10+
- Node 18+

### Server

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # or: pip install -r requirements.txt
cp .env.example .env         # fill in DEEPGRAM_API_KEY, GEMINI_API_KEY, CARTESIA_API_KEY
python -m server.app         # serves http://localhost:8000
```

- Health check: `curl http://localhost:8000/health`
- WebSocket: `ws://localhost:8000/ws/session/{id}`

### Client

```bash
cd client
npm install
npm run dev                  # serves http://localhost:5173
```

Open http://localhost:5173, click **Connect**, then **Hold to talk**. You should
see a streaming transcript, a spoken reply, and a latency waterfall for the turn.

Point the client at a remote API with `VITE_WS_BASE` (no trailing slash), e.g.
`VITE_WS_BASE=wss://example.run.app`.

### Docker

The image serves the API on port **8080**:

```bash
docker build -t voxwire .
docker run --env-file .env -p 8000:8080 voxwire
```

Then set `VITE_WS_BASE=ws://localhost:8000` when running the Vite client.

### Tests

```bash
python -m pytest -q
ruff check server tests
black --check server tests
```

Client typecheck + bundle: `npm run build` in `client/`.

## Repo layout

```
server/app.py              FastAPI, /health, WebSocket route
server/config.py           Env/settings, lazy credential checks
server/ws/                 Session handler (control messages + orchestrator)
server/pipeline/           PipelineOrchestrator + structured errors
server/providers/          ASR / LLM / TTS adapters
server/latency/            LatencyTracker + stage report
server/resilience/         Timeouts, one-shot retry, per-stage circuit breaker
server/observability/      Optional Langfuse traces
server/replay/             Per-turn recorder + offline JSONL replayer
client/                    Push-to-talk UI, playback, latency waterfall
recordings/                Live turn traces; samples/ holds CI fixtures
docs/                      architecture, event protocol, latency budget
tests/                     Server unit + integration tests
```

## Failure modes

Every turn still ends with `turn_complete`. A stage failure emits an `error`
event, marks the turn `degraded`, and skips downstream work that no longer
makes sense. The UI never freezes waiting on a hung provider.

| Trigger | Behavior |
|---------|----------|
| Missing API key (`CONFIG_ERROR`) | Stage skipped; structured error; turn completes degraded. In-session retry is allowed so repeated misses can open the breaker. |
| ASR timeout / provider down | No LLM or TTS. Empty/absent transcript. Banner + **type instead** field. |
| LLM timeout before first token | Fallback spoken copy is **not** synthesized (`ttsSkipped`). Banner asks the user to retry. |
| LLM timeout after partial tokens | Partial reply is shown; TTS still runs on what arrived. |
| TTS timeout / provider down | Reply stays on screen as text (`ttsSkipped: true`). Mic remains available. |
| Empty transcript | LLM and TTS skipped; turn still completes. |
| Typed fallback (`text_turn`) | ASR skipped; LLM + TTS run on the typed text. |
| Circuit breaker open | Stage skipped immediately (`BREAKER_OPEN`). ASR also blocks push-to-talk for the cooldown and keeps the text box up. |
| Client disconnect mid-turn | Server abandons the turn and closes provider sessions in `pipeline.close()`. |
| Client reconnects | New `sessionId`; chat UI is cleared. An ASR breaker cooldown in `sessionStorage` is restored so reconnect cannot bypass it. |
| WebSocket idle (default 5 min) | Server closes the socket (`idle timeout`). Client shows **Session lost — reconnect to continue.** |

Wire shapes live in [`docs/event-protocol.md`](docs/event-protocol.md).

### Timeout policy

Deadlines are wall-clock per stage, from settings (see `.env.example`):

| Setting | Default | What it covers |
|---------|---------|----------------|
| `ASR_TIMEOUT_S` | 15s | `provider.start()` and `session.finalize()` |
| `LLM_TTFT_TIMEOUT_S` | 10s | Time to first streamed token |
| `LLM_TIMEOUT_S` | 30s | Entire LLM stream |
| `TTS_TIMEOUT_S` | 15s | Entire TTS stream |
| `WS_IDLE_TIMEOUT_S` | 300s | No client frame on the socket |

Timeouts are **not** retried. They emit `error.code = TIMEOUT` and degrade the turn.

### Transient retry vs circuit breaker

These are sequential, not alternatives:

1. **One transient retry** — connection errors, HTTP 429/5xx, and similar blips get a single retry with 200–500 ms jitter, only if time remains in the stage deadline. 4xx (except 429) and timeouts are not retried.
2. **Circuit breaker** — after `BREAKER_FAILURE_THRESHOLD` consecutive **hard** failures (post-retry) on a stage, that stage opens for `BREAKER_COOLDOWN_S` (defaults: 3 failures, 60s). While open, the orchestrator skips the provider and emits `BREAKER_OPEN`. After cooldown, one half-open probe is allowed; success closes the breaker, failure re-opens it.

Retry absorbs a single blip. The breaker stops hammering a stage that is actually down.

### `recoverable` semantics

`error.recoverable` tells the client whether another attempt is useful **in this session**.

| `code` | Typical `recoverable` | Client |
|--------|----------------------|--------|
| `TIMEOUT`, `PROVIDER_DOWN`, `STREAM_FAILED` | `true` | Warn banner + **Try again**. ASR also shows the text box. |
| `CONFIG_ERROR` | `true` (asr/llm/tts) | Same as recoverable, so repeated attempts can trip the breaker. |
| `BREAKER_OPEN` | `false` | No retry button. ASR: mic disabled for `cooldownMs`, text box stays up. LLM/TTS: mic stays available (TTS-open means text-only replies). |
| `BAD_REQUEST` | `false` | Typed turn with empty text; no retry. |

`turn_complete.meta.degradedMode` lists the stages that failed (`["asr"]`, `["llm"]`, `["tts"]`, or combinations). `ttsSkipped` is independent: TTS may be skipped because it failed, or because there was nothing to speak.

### WebSocket session recovery

| Event | Behavior |
|-------|----------|
| Client disconnects mid-turn | Abandon in-flight turn server-side; release provider sessions |
| Client reconnects | New session id; clear UI conversation state |
| WS idle (5 min) | Server closes with reason; client shows disconnected |

There is no automatic reconnect. Click **Connect** again to mint a new session.

## Recordings and replay

Each completed turn writes:

- `recordings/{turnId}.pcm` — concatenated upstream PCM16 (omitted when there is no audio)
- `recordings/{turnId}.jsonl` — one `recording_meta` line, then every server-emitted protocol event

Checked-in fixtures:

- `recordings/samples/hello-turn.jsonl` (+ `hello-turn.pcm`) — happy-path turn with audio
- `recordings/samples/degraded-llm-config.jsonl` — degraded turn, no audio

Replay does not need a microphone. Paths must stay under `RECORDINGS_DIR`.
The client path is a **server** path (not a file on the user's laptop).

| Mode | UI label | What it does | Providers |
|------|----------|----------------|-----------|
| `events` | Events | Re-emit the saved JSONL frames (transcript, tokens, TTS chunks, latency). | None |
| `full` | Full pipeline | Feed saved PCM through ASR → LLM → TTS. If the PCM is silence **and** the recording has a `transcript_final`, inject that transcript instead (skip ASR). | ASR (unless silence fallback), LLM, TTS |
| `mock` | Mock (skip ASR) | Skip ASR; inject `transcript_final` into LLM → TTS. | LLM, TTS |
| `mock` + `mockLlm` | Mock + Mock LLM/TTS | Same as mock, but replay saved LLM tokens and TTS chunks. | None |

| What you replay | Events | Full pipeline | Mock | Mock + Mock LLM/TTS |
|-----------------|--------|---------------|------|---------------------|
| **Hello turn** (`samples/hello-turn.jsonl`) | Saved `hello` / `Hi there` (fixture TTS is a 4-byte stub, not real speech). | PCM is silence → uses saved `hello`, then live LLM → TTS. Needs LLM/TTS keys. | Saved `hello` → live LLM → TTS. | Saved `hello` / `Hi there` / stub audio. |
| **Degraded LLM** (`samples/degraded-llm-config.jsonl`) | Saved `offline transcript` plus a **replayed** `CONFIG_ERROR` banner (expected; not a live key failure). | Not offered in the UI (no `.pcm`). | Saved transcript → live LLM → TTS. | Replays saved degraded LLM/TTS (none). |
| **Live chat turn** (`{turnId}.jsonl` via **Replay** on the bubble or latency row) | Exact saved transcript, reply, and TTS. | Re-runs ASR on the real `.pcm` (needs keys). Empty bubbles if ASR hears nothing. | Saved transcript → live LLM → TTS (new reply). | Saved transcript, reply, and TTS. |
| **Custom path** (type `{turnId}.jsonl` under `recordings/`, not `samples/`) | Same as live chat turn for that file. | Same as live chat turn. | Same as live chat turn. | Same as live chat turn. |

Do **not** prefix live recordings with `samples/`. Bundled fixtures live in `samples/`; completed turns are `{turnId}.jsonl` at the recordings root. Clicking **Replay** on a bundled sample keeps that sample selected; live turns fill **Custom path…**.

Starting a replay stops in-flight TTS so the previous turn and the replay do not play at once.

```bash
python -m server.replay recordings/samples/hello-turn.jsonl
python -m server.replay recordings/samples/hello-turn.jsonl --mode full
python -m server.replay recordings/samples/hello-turn.jsonl --mode mock --mock-llm
```

Over a live session: `{ "type": "replay_start", "recordingPath": "samples/hello-turn.jsonl", "mode": "full" }`.

In the browser: **Connect**, pick a sample or a finished turn's **Replay**, choose a mode (table above), then play. The replayed turn is marked with a Replay badge.

HTTP (returns `{ metadata, mode, events }`):

```bash
curl -s http://localhost:8000/replay \
  -H 'content-type: application/json' \
  -d '{"recordingPath":"samples/hello-turn.jsonl","mode":"events"}'
```

## Observability

Set `LANGFUSE_ENABLED=true` plus `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY`
to emit one Langfuse trace per turn with ASR / LLM / TTS spans. When disabled
or unconfigured, tracing is a no-op so local dev and CI stay keyless.

Per-turn latency (server monotonic marks from `utterance_end`) is defined in
[`docs/latency-budget.md`](docs/latency-budget.md) and rendered in the client
waterfall plus last-N table, including felt latency (release → first playback).

## License

MIT — see [LICENSE](LICENSE).
