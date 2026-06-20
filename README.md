# voxwire

A standalone **realtime voice assistant pipeline**: speak into the browser, get a
spoken reply back, with a per-request **latency budget** and **production-grade
resilience** (timeouts, graceful degradation, replay).

```
Browser mic → WebSocket → Server orchestrator
                              ├→ ASR  (speech → text, streaming)
                              ├→ LLM  (streaming tokens)
                              └→ TTS  (text → audio, streaming)
                         ← WebSocket ← audio + status + latency events
```

This repo is built in phases (tracked as GitHub milestones/issues):

- **Phase 0 — Setup** ✅ scaffold, config, transport (health + WebSocket echo)
- **Phase 1 — Streaming pipeline** ⏳ ASR → LLM → TTS end to end
- **Phase 2 — Latency budget** ⏳ per-turn waterfall of where time goes
- **Phase 3 — Resilience & replay** ⏳ timeouts, fallbacks, recorded replay

## Tech stack

| Layer | Choice | Why |
|-------|--------|-----|
| Server | **FastAPI + uvicorn** | First-class async + WebSockets |
| Transport | **WebSockets** | Continuous bidirectional streaming |
| Client | **Vite + TypeScript** (vanilla) | Fast dev server, typed, light (no framework) |
| ASR | **Deepgram** (default) | Mature streaming API; swap to Whisper via `ASR_PROVIDER` |
| LLM | **Gemini** (default) | Free tier with token streaming; swap to OpenAI via `LLM_PROVIDER` |
| TTS | **Cartesia** (default) | Low-latency streaming audio; swap to ElevenLabs via `TTS_PROVIDER` |

Provider choices are swappable through env vars — see `.env.example`. Credentials
are loaded lazily: the server boots without keys (Phase 0 only needs transport),
and each pipeline stage fails fast with a clear message if its key is missing.

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
cp .env.example .env         
python -m server.app         # serves http://localhost:8000
```

- Health check: `curl http://localhost:8000/health`
- WebSocket: `ws://localhost:8000/ws/session/{id}` (echoes JSON; `ping` → `pong`)

### Client

```bash
cd client
npm install
npm run dev                  # serves http://localhost:5173
```

Open http://localhost:5173, click **Connect**, then **Send ping** — you should see
a `pong` round-trip in the log.

### Tests

```bash
pytest          # server smoke tests (health + WS echo)
ruff check .    # lint
black --check . # format check
```

## Repo layout

```
server/        FastAPI app, config, WebSocket + (later) pipeline/providers/latency/replay
client/        Vite + TS single-page client (connect, mic, transcript, latency UI)
recordings/    Replay fixtures (Phase 3)
docs/          architecture, event protocol, latency budget
tests/         server tests
```

## Roadmap

See the [project roadmap issue](https://github.com/vectorvoyager358/voxwire/issues/26)
and the per-phase milestones on GitHub.

## License

MIT — see [LICENSE](LICENSE).
