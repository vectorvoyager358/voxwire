// TTS playback: queues incoming `tts_audio_chunk` events and plays them back as
// one continuous stream via the Web Audio API.
//
// The protocol (docs/event-protocol.md) sends downstream audio as raw
// pcm_s16le @ 24 kHz, base64 in `data`. Raw PCM can't go through
// `decodeAudioData` (that's for WAV/MP3/etc.), so we hand-build an AudioBuffer
// per chunk and schedule each one back-to-back against the AudioContext clock
// to avoid gaps or overlaps.

export const PLAYBACK_SAMPLE_RATE = 24000;

export interface PlaybackStartInfo {
  turnId: string;
  /** Client wall-clock time (ms) when the first chunk of the turn began. */
  at: number;
}

export type PlaybackStartHandler = (info: PlaybackStartInfo) => void;

export class TtsPlayer {
  private ctx: AudioContext | null = null;
  private nextStartTime = 0;
  private currentTurn: string | null = null;
  private turnStarted = false;
  private readonly onPlaybackStart?: PlaybackStartHandler;

  constructor(onPlaybackStart?: PlaybackStartHandler) {
    this.onPlaybackStart = onPlaybackStart;
  }

  /**
   * Create/resume the AudioContext. Call this from a user gesture (e.g. the
   * push-to-talk press) so browser autoplay policies allow later playback.
   */
  async unlock(): Promise<void> {
    const ctx = this.ensureContext();
    if (ctx.state === "suspended") await ctx.resume();
  }

  /**
   * Queue one TTS audio chunk for gapless playback. `_seq` is accepted for API
   * symmetry with the protocol but unused for now: chunks are assumed in order.
   */
  enqueue(turnId: string, _seq: number, base64Data: string, sampleRate = PLAYBACK_SAMPLE_RATE): void {
    const ctx = this.ensureContext();
    if (ctx.state === "suspended") void ctx.resume();

    if (turnId !== this.currentTurn) this.beginTurn(turnId, ctx);

    const buffer = this.decode(ctx, base64Data, sampleRate);
    if (buffer.length === 0) return;

    const source = ctx.createBufferSource();
    source.buffer = buffer;
    source.connect(ctx.destination);

    // Schedule right after the previously queued chunk; if we've fallen behind
    // (chunk arrived late), restart from "now" — a small audible gap, but never
    // an overlap. `seq` is currently advisory; chunks are assumed in order.
    const startAt = Math.max(this.nextStartTime, ctx.currentTime);
    source.start(startAt);
    this.nextStartTime = startAt + buffer.duration;

    if (!this.turnStarted) {
      this.turnStarted = true;
      this.onPlaybackStart?.({ turnId, at: Date.now() });
    }
  }

  /** Stop any scheduled audio and forget the current turn. */
  reset(): void {
    if (this.ctx) {
      void this.ctx.close().catch(() => undefined);
      this.ctx = null;
    }
    this.currentTurn = null;
    this.turnStarted = false;
    this.nextStartTime = 0;
  }

  private beginTurn(turnId: string, ctx: AudioContext): void {
    this.currentTurn = turnId;
    this.turnStarted = false;
    this.nextStartTime = ctx.currentTime;
  }

  private ensureContext(): AudioContext {
    if (!this.ctx) {
      const Ctx: typeof AudioContext =
        window.AudioContext ??
        (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
      this.ctx = new Ctx({ sampleRate: PLAYBACK_SAMPLE_RATE });
      this.nextStartTime = this.ctx.currentTime;
    }
    return this.ctx;
  }

  /** Decode base64 pcm_s16le into a mono AudioBuffer at the chunk's rate. */
  private decode(ctx: AudioContext, base64Data: string, sampleRate: number): AudioBuffer {
    const bytes = base64ToUint8Array(base64Data);
    // pcm_s16le is 2 bytes/sample; ignore a stray trailing byte if present.
    const sampleCount = bytes.byteLength >> 1;
    const view = new DataView(bytes.buffer, bytes.byteOffset, sampleCount * 2);

    const buffer = ctx.createBuffer(1, Math.max(sampleCount, 1), sampleRate);
    const channel = buffer.getChannelData(0);
    for (let i = 0; i < sampleCount; i++) {
      channel[i] = view.getInt16(i * 2, true) / 32768;
    }
    return buffer;
  }
}

function base64ToUint8Array(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}
