// Microphone capture for push-to-talk. Streams 16 kHz mono PCM16 chunks (as
// ArrayBuffers) produced by the AudioWorklet in public/pcm-worklet.js.

export const CAPTURE_SAMPLE_RATE = 16000;
export const CAPTURE_ENCODING = "pcm_s16le";
export const CAPTURE_CHANNELS = 1;
const CHUNK_MS = 100;

export type ChunkHandler = (pcm: ArrayBuffer) => void;

/** A user-friendly reason capture could not start. */
export type CaptureErrorReason = "denied" | "no-device" | "insecure" | "unsupported" | "unknown";

export class CaptureError extends Error {
  readonly reason: CaptureErrorReason;
  constructor(reason: CaptureErrorReason, message: string) {
    super(message);
    this.name = "CaptureError";
    this.reason = reason;
  }
}

export class AudioCapture {
  private ctx: AudioContext | null = null;
  private stream: MediaStream | null = null;
  private source: MediaStreamAudioSourceNode | null = null;
  private node: AudioWorkletNode | null = null;

  /** The actual sample rate the context runs at (should be 16 kHz). */
  get sampleRate(): number {
    return this.ctx?.sampleRate ?? CAPTURE_SAMPLE_RATE;
  }

  get active(): boolean {
    return this.node !== null;
  }

  /** Request mic access and begin emitting PCM16 chunks via `onChunk`. */
  async start(onChunk: ChunkHandler): Promise<void> {
    if (!window.isSecureContext) {
      throw new CaptureError(
        "insecure",
        "Microphone needs a secure context (https or localhost).",
      );
    }
    if (!navigator.mediaDevices?.getUserMedia) {
      throw new CaptureError("unsupported", "This browser does not support getUserMedia.");
    }

    try {
      this.stream = await navigator.mediaDevices.getUserMedia({
        audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true },
      });
    } catch (err) {
      throw this.toCaptureError(err);
    }

    const Ctx: typeof AudioContext =
      window.AudioContext ?? (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
    this.ctx = new Ctx({ sampleRate: CAPTURE_SAMPLE_RATE });
    if (this.ctx.state === "suspended") await this.ctx.resume();

    await this.ctx.audioWorklet.addModule(`${import.meta.env.BASE_URL}pcm-worklet.js`);

    this.source = this.ctx.createMediaStreamSource(this.stream);
    this.node = new AudioWorkletNode(this.ctx, "pcm-worklet", {
      numberOfInputs: 1,
      numberOfOutputs: 1,
      outputChannelCount: [1],
      processorOptions: { targetSampleRate: CAPTURE_SAMPLE_RATE, chunkMs: CHUNK_MS },
    });
    this.node.port.onmessage = (event: MessageEvent<ArrayBuffer>) => onChunk(event.data);

    this.source.connect(this.node);
    // Connect to destination so the graph pulls the node; the worklet writes
    // silence to its output, so nothing is actually played back.
    this.node.connect(this.ctx.destination);
  }

  /** Flush any buffered samples, then tear down the audio graph and mic. */
  async stop(): Promise<void> {
    this.node?.port.postMessage("flush");
    // Give the worklet one render tick to post the final (partial) chunk.
    await new Promise((resolve) => setTimeout(resolve, 30));

    this.source?.disconnect();
    this.node?.disconnect();
    this.stream?.getTracks().forEach((track) => track.stop());
    await this.ctx?.close().catch(() => undefined);

    this.source = null;
    this.node = null;
    this.stream = null;
    this.ctx = null;
  }

  private toCaptureError(err: unknown): CaptureError {
    const name = err instanceof DOMException ? err.name : "";
    if (name === "NotAllowedError" || name === "SecurityError") {
      return new CaptureError("denied", "Microphone permission was denied.");
    }
    if (name === "NotFoundError" || name === "OverconstrainedError") {
      return new CaptureError("no-device", "No microphone was found.");
    }
    return new CaptureError("unknown", err instanceof Error ? err.message : "Could not access microphone.");
  }
}

/** Base64-encode raw bytes for the protocol's JSON `data` field. */
export function arrayBufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}
