export type ConnectionStatus = "disconnected" | "connecting" | "connected" | "error";

export interface VoxwireClientHandlers {
  onStatus: (status: ConnectionStatus) => void;
  onMessage: (data: unknown) => void;
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

  send(message: Record<string, unknown>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) {
      throw new Error("WebSocket is not open");
    }
    this.ws.send(JSON.stringify(message));
  }

  ping(): void {
    this.send({ type: "ping", payload: { sentAt: Date.now() } });
  }
}
