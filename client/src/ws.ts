export type ConnectionStatus = "disconnected" | "connecting" | "connected" | "error";

export interface VoxwireClientHandlers {
  onStatus: (status: ConnectionStatus) => void;
  onMessage: (data: unknown) => void;
}

export interface AudioFormat {
  encoding: string;
  sampleRate: number;
  channels: number;
}

const WS_BASE = import.meta.env.VITE_WS_BASE ?? "ws://localhost:8000";

/** Thin WebSocket wrapper for a single voxwire session. */
export class VoxwireClient {
  private ws: WebSocket | null = null;
  private readonly sessionId: string;
  private readonly handlers: VoxwireClientHandlers;

  constructor(handlers: VoxwireClientHandlers, sessionId = crypto.randomUUID()) {
    this.handlers = handlers;
    this.sessionId = sessionId;
  }

  get id(): string {
    return this.sessionId;
  }

  get connected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  connect(): void {
    if (this.ws) return;
    this.handlers.onStatus("connecting");
    const url = `${WS_BASE}/ws/session/${this.sessionId}`;
    const ws = new WebSocket(url);
    this.ws = ws;

    ws.onopen = () => this.handlers.onStatus("connected");
    ws.onclose = () => {
      this.ws = null;
      this.handlers.onStatus("disconnected");
    };
    ws.onerror = () => this.handlers.onStatus("error");
    ws.onmessage = (event) => {
      try {
        this.handlers.onMessage(JSON.parse(event.data));
      } catch {
        this.handlers.onMessage(event.data);
      }
    };
  }

  disconnect(): void {
    this.ws?.close();
    this.ws = null;
  }

  /** Build a protocol envelope with the common fields filled in. */
  private envelope(type: string, turnId: string | null, extra: Record<string, unknown> = {}) {
    return {
      type,
      sessionId: this.sessionId,
      turnId,
      timestamp: Date.now(),
      ...extra,
    };
  }

  private send(message: Record<string, unknown>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) {
      throw new Error("WebSocket is not open");
    }
    this.ws.send(JSON.stringify(message));
  }

  /** Announce the upstream audio format once, right after connecting. */
  sessionStart(audio: AudioFormat): void {
    this.send(
      this.envelope("session_start", null, {
        audio,
        client: { userAgent: navigator.userAgent, appVersion: "0.1.0" },
      }),
    );
  }

  /** Stream one slice of captured audio for the given turn. */
  audioChunk(turnId: string, seq: number, base64Data: string): void {
    this.send(this.envelope("audio_chunk", turnId, { seq, data: base64Data }));
  }

  /** Signal that the user released push-to-talk; no more chunks this turn. */
  utteranceEnd(turnId: string, totalChunks: number, captureMs?: number): void {
    const extra: Record<string, unknown> = { totalChunks };
    if (captureMs !== undefined) extra.captureMs = captureMs;
    this.send(this.envelope("utterance_end", turnId, extra));
  }

  /** Typed fallback when ASR is unavailable (issue #20). */
  textTurn(turnId: string, text: string): void {
    this.send(this.envelope("text_turn", turnId, { text }));
  }

  ping(): void {
    this.send(this.envelope("ping", null, { payload: { sentAt: Date.now() } }));
  }
}
