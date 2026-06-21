import { VoxwireClient, type ConnectionStatus } from "./ws";
import {
  AudioCapture,
  CaptureError,
  arrayBufferToBase64,
  CAPTURE_CHANNELS,
  CAPTURE_ENCODING,
  CAPTURE_SAMPLE_RATE,
} from "./audio";
import { TtsPlayer } from "./playback";

const connectBtn = document.getElementById("connect") as HTMLButtonElement;
const pingBtn = document.getElementById("ping") as HTMLButtonElement;
const talkBtn = document.getElementById("talk") as HTMLButtonElement;
const micError = document.getElementById("micError") as HTMLParagraphElement;
const dot = document.getElementById("dot") as HTMLSpanElement;
const statusText = document.getElementById("statusText") as HTMLSpanElement;
const logEl = document.getElementById("log") as HTMLDivElement;
const youEl = document.getElementById("you") as HTMLSpanElement;
const assistantEl = document.getElementById("assistant") as HTMLSpanElement;

function log(text: string, kind: "in" | "out" | "sys" = "sys"): void {
  const line = document.createElement("div");
  line.className = `log-${kind}`;
  const time = new Date().toLocaleTimeString();
  line.textContent = `[${time}] ${text}`;
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(status: ConnectionStatus): void {
  dot.className = `dot ${status}`;
  statusText.textContent = status;
  const connected = status === "connected";
  pingBtn.disabled = !connected;
  talkBtn.disabled = !connected;
  connectBtn.textContent = connected ? "Disconnect" : "Connect";
  connectBtn.disabled = status === "connecting";
}

let client: VoxwireClient | null = null;
const capture = new AudioCapture();
const player = new TtsPlayer((info) => log(`playback_start (turn ${info.turnId})`, "sys"));

interface ServerMessage {
  type?: string;
  turnId?: string;
  seq?: number;
  data?: string;
  sampleRate?: number;
  text?: string;
  stage?: string;
  message?: string;
}

// Conversation display: the user's transcript and the assistant's streamed reply.
let displayTurn: string | null = null;
let assistantText = "";

function clearConvo(): void {
  displayTurn = null;
  assistantText = "";
  youEl.textContent = "";
  youEl.classList.remove("partial");
  assistantEl.textContent = "";
}

// Reset both bubbles when a new turn's first message arrives.
function syncTurn(turnId?: string): void {
  if (turnId && turnId !== displayTurn) {
    displayTurn = turnId;
    assistantText = "";
    youEl.textContent = "";
    youEl.classList.remove("partial");
    assistantEl.textContent = "";
  }
}

function setYou(text: string, final: boolean): void {
  youEl.textContent = text;
  youEl.classList.toggle("partial", !final);
}

function handleMessage(data: unknown): void {
  const msg = data as ServerMessage;
  switch (msg?.type) {
    case "transcript_partial":
      syncTurn(msg.turnId);
      setYou(msg.text ?? "", false);
      return;
    case "transcript_final":
      syncTurn(msg.turnId);
      setYou(msg.text ?? "", true);
      log(`<- transcript_final: ${msg.text ?? ""}`, "in");
      return;
    case "llm_token":
      syncTurn(msg.turnId);
      assistantText += msg.text ?? "";
      assistantEl.textContent = assistantText;
      return;
    case "llm_complete":
      syncTurn(msg.turnId);
      assistantText = msg.text ?? assistantText;
      assistantEl.textContent = assistantText;
      log("<- llm_complete", "in");
      return;
    case "tts_audio_chunk":
      if (typeof msg.turnId === "string" && typeof msg.data === "string") {
        player.enqueue(msg.turnId, msg.seq ?? 0, msg.data, msg.sampleRate);
        if ((msg.seq ?? 0) === 0) log(`<- tts_audio_chunk stream (turn ${msg.turnId})`, "in");
      }
      return;
    case "turn_complete":
      log(`<- turn_complete (turn ${msg.turnId})`, "in");
      return;
    case "error":
      log(`<- error [${msg.stage ?? "?"}] ${msg.message ?? ""}`, "sys");
      return;
    default:
      log(`<- ${JSON.stringify(data)}`, "in");
  }
}

// Per-utterance state, set on push-to-talk press.
let turnId: string | null = null;
let seq = 0;
let holding = false;

connectBtn.addEventListener("click", () => {
  if (client) {
    void stopTalking();
    player.reset();
    clearConvo();
    client.disconnect();
    client = null;
    return;
  }
  client = new VoxwireClient({
    onStatus: (status) => {
      setStatus(status);
      if (status === "connected") {
        log(`connected (session ${client?.id})`, "sys");
        // Declare the upstream audio format as soon as the socket is open.
        client?.sessionStart({
          encoding: CAPTURE_ENCODING,
          sampleRate: CAPTURE_SAMPLE_RATE,
          channels: CAPTURE_CHANNELS,
        });
        log("-> session_start", "out");
      }
      if (status === "disconnected") log("disconnected", "sys");
      if (status === "error") log("connection error", "sys");
    },
    onMessage: handleMessage,
  });
  client.connect();
});

pingBtn.addEventListener("click", () => {
  if (!client) return;
  client.ping();
  log("-> ping", "out");
});

async function startTalking(): Promise<void> {
  if (!client?.connected || holding) return;
  holding = true;
  // This press is a user gesture: unlock playback so the TTS reply can sound.
  void player.unlock();
  turnId = crypto.randomUUID();
  seq = 0;
  micError.textContent = "";
  talkBtn.classList.add("recording");
  talkBtn.textContent = "Recording… release to send";

  try {
    await capture.start((pcm) => {
      // The capture may outlive the press by one flushed chunk; guard turnId.
      if (!client?.connected || turnId === null) return;
      client.audioChunk(turnId, seq, arrayBufferToBase64(pcm));
      seq += 1;
    });
    log(`-> audio capture started (turn ${turnId})`, "out");
  } catch (err) {
    holding = false;
    resetTalkButton();
    const message = err instanceof CaptureError ? err.message : "Could not start microphone.";
    micError.textContent = message;
    log(`mic error: ${message}`, "sys");
  }
}

async function stopTalking(): Promise<void> {
  if (!holding) return;
  holding = false;
  resetTalkButton();

  const endedTurn = turnId;
  await capture.stop(); // flushes the final partial chunk through onChunk
  turnId = null;

  if (client?.connected && endedTurn) {
    client.utteranceEnd(endedTurn, seq);
    log(`-> utterance_end (${seq} chunks)`, "out");
  }
}

function resetTalkButton(): void {
  talkBtn.classList.remove("recording");
  talkBtn.textContent = "Hold to talk";
}

// Pointer events cover mouse + touch; releasing or leaving the button ends the turn.
talkBtn.addEventListener("pointerdown", (event) => {
  event.preventDefault();
  void startTalking();
});
talkBtn.addEventListener("pointerup", () => void stopTalking());
talkBtn.addEventListener("pointerleave", () => void stopTalking());
talkBtn.addEventListener("pointercancel", () => void stopTalking());

setStatus("disconnected");
