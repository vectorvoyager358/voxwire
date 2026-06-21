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
const chatLog = document.getElementById("chatLog") as HTMLDivElement;
const chatScroll = document.getElementById("chatScroll") as HTMLDivElement;
const pipelineStateEl = document.getElementById("pipelineState") as HTMLParagraphElement;
const bannerEl = document.getElementById("banner") as HTMLParagraphElement;

type PipelineState = "idle" | "listening" | "thinking" | "speaking" | "error";

interface TurnCompleteMeta {
  degraded?: boolean;
  ttsSkipped?: boolean;
  ttsChunks?: number;
}

interface ServerMessage {
  type?: string;
  turnId?: string;
  seq?: number;
  data?: string;
  sampleRate?: number;
  text?: string;
  stage?: string;
  message?: string;
  recoverable?: boolean;
  meta?: TurnCompleteMeta;
}

interface ActiveTurn {
  turnId: string;
  root: HTMLDivElement;
  you: HTMLSpanElement;
  assistant: HTMLSpanElement;
  footnote: HTMLParagraphElement;
  assistantText: string;
}

/** In-flight or active push-to-talk capture for one utterance. */
interface TalkSession {
  turnId: string;
  seq: number;
  micReady: boolean;
}

function log(text: string, kind: "in" | "out" | "sys" = "sys"): void {
  const line = document.createElement("div");
  line.className = `log-${kind}`;
  const time = new Date().toLocaleTimeString();
  line.textContent = `[${time}] ${text}`;
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function setPipelineState(state: PipelineState): void {
  pipelineStateEl.textContent = state.charAt(0).toUpperCase() + state.slice(1);
  pipelineStateEl.className = `pipeline-state${state === "idle" ? "" : ` ${state}`}`;
}

function showBanner(text: string, kind: "error" | "warn" | "info" = "warn"): void {
  bannerEl.textContent = text;
  bannerEl.className = `banner visible ${kind}`;
}

function hideBanner(): void {
  bannerEl.textContent = "";
  bannerEl.className = "banner";
}

function setTalkEnabled(enabled: boolean): void {
  talkBtn.disabled = !enabled || !client?.connected;
}

function setStatus(status: ConnectionStatus): void {
  dot.className = `dot ${status}`;
  statusText.textContent = status;
  const connected = status === "connected";
  pingBtn.disabled = !connected;
  connectBtn.textContent = connected ? "Disconnect" : "Connect";
  connectBtn.disabled = status === "connecting";
  setTalkEnabled(connected && !pttBlocked);
}

let client: VoxwireClient | null = null;
const capture = new AudioCapture();
const player = new TtsPlayer((info) => {
  log(`playback_start (turn ${info.turnId})`, "sys");
  setPipelineState("speaking");
});

let activeTurn: ActiveTurn | null = null;
let pttBlocked = false;

function scrollChatToBottom(): void {
  chatScroll.scrollTop = chatScroll.scrollHeight;
}

function makeBubble(who: string, extraClass = ""): { root: HTMLDivElement; text: HTMLSpanElement } {
  const root = document.createElement("div");
  root.className = `bubble ${extraClass}`.trim();
  const label = document.createElement("span");
  label.className = "who";
  label.textContent = who;
  const text = document.createElement("span");
  text.className = "text";
  root.append(label, text);
  return { root, text };
}

function ensureTurn(turnId: string): ActiveTurn {
  if (activeTurn?.turnId === turnId) return activeTurn;

  const block = document.createElement("div");
  block.className = "turn";
  const youBubble = makeBubble("You", "you");
  const assistantBubble = makeBubble("Voxwire", "assistant");
  const footnote = document.createElement("p");
  footnote.className = "turn-footnote";
  footnote.hidden = true;
  block.append(youBubble.root, assistantBubble.root, footnote);
  chatLog.appendChild(block);
  scrollChatToBottom();

  activeTurn = {
    turnId,
    root: block,
    you: youBubble.text,
    assistant: assistantBubble.text,
    footnote,
    assistantText: "",
  };
  return activeTurn;
}

function cancelTurnBlock(turnId: string): void {
  if (activeTurn?.turnId !== turnId) return;
  activeTurn.root.remove();
  activeTurn = null;
}

function setYou(text: string, final: boolean): void {
  if (!activeTurn) return;
  activeTurn.you.textContent = text;
  activeTurn.you.classList.toggle("partial", !final);
  scrollChatToBottom();
}

function appendAssistant(text: string): void {
  if (!activeTurn) return;
  activeTurn.assistantText += text;
  activeTurn.assistant.textContent = activeTurn.assistantText;
  scrollChatToBottom();
}

function setAssistantComplete(text: string): void {
  if (!activeTurn) return;
  activeTurn.assistantText = text;
  activeTurn.assistant.textContent = text;
  scrollChatToBottom();
}

function finishTurn(meta?: TurnCompleteMeta): void {
  if (!activeTurn) return;
  const notes: string[] = [];
  if (meta?.degraded) notes.push("Turn completed with errors");
  if (meta?.ttsSkipped) notes.push("Reply shown as text only (audio skipped)");
  if (notes.length) {
    activeTurn.footnote.textContent = notes.join(" · ");
    activeTurn.footnote.hidden = false;
  }
  activeTurn = null;
  scrollChatToBottom();
}

function clearChat(): void {
  chatLog.replaceChildren();
  activeTurn = null;
}

function resetSessionUi(): void {
  clearChat();
  hideBanner();
  pttBlocked = false;
  setPipelineState("idle");
  setTalkEnabled(client?.connected ?? false);
}

function handleMessage(data: unknown): void {
  const msg = data as ServerMessage;
  switch (msg?.type) {
    case "transcript_partial":
      if (msg.turnId) ensureTurn(msg.turnId);
      setYou(msg.text ?? "", false);
      return;
    case "transcript_final":
      if (msg.turnId) ensureTurn(msg.turnId);
      setYou(msg.text ?? "", true);
      log(`<- transcript_final: ${msg.text ?? ""}`, "in");
      if (!(msg.text ?? "").trim()) {
        showBanner("No speech detected — hold the button longer and speak clearly.", "warn");
      }
      return;
    case "llm_token":
      if (msg.turnId) ensureTurn(msg.turnId);
      appendAssistant(msg.text ?? "");
      return;
    case "llm_complete":
      if (msg.turnId) ensureTurn(msg.turnId);
      setAssistantComplete(msg.text ?? activeTurn?.assistantText ?? "");
      log("<- llm_complete", "in");
      return;
    case "tts_audio_chunk":
      if (typeof msg.turnId === "string" && typeof msg.data === "string") {
        player.enqueue(msg.turnId, msg.seq ?? 0, msg.data, msg.sampleRate);
        if ((msg.seq ?? 0) === 0) log(`<- tts_audio_chunk stream (turn ${msg.turnId})`, "in");
      }
      return;
    case "turn_complete": {
      finishTurn(msg.meta);
      if (msg.meta?.degraded) {
        showBanner("Something went wrong during this turn. Check the reply above.", "warn");
        setPipelineState("error");
      } else {
        hideBanner();
        setPipelineState("idle");
      }
      log(`<- turn_complete (turn ${msg.turnId})`, "in");
      return;
    }
    case "error": {
      const stage = msg.stage ?? "unknown";
      const detail = msg.message ?? "An error occurred.";
      log(`<- error [${stage}] ${detail}`, "sys");
      setPipelineState("error");
      if (msg.recoverable === false) {
        pttBlocked = true;
        setTalkEnabled(false);
        showBanner(`${detail} Push-to-talk is disabled until you reconnect.`, "error");
      } else {
        showBanner(`${detail} Try again by holding the talk button.`, "warn");
      }
      return;
    }
    default:
      log(`<- ${JSON.stringify(data)}`, "in");
  }
}

let talkSession: TalkSession | null = null;
let intentionalDisconnect = false;

connectBtn.addEventListener("click", () => {
  if (client) {
    intentionalDisconnect = true;
    void stopTalking();
    player.reset();
    resetSessionUi();
    client.disconnect();
    client = null;
    setStatus("disconnected");
    return;
  }
  intentionalDisconnect = false;
  client = new VoxwireClient({
    onStatus: (status) => {
      setStatus(status);
      if (status === "connected") {
        hideBanner();
        pttBlocked = false;
        setPipelineState("idle");
        log(`connected (session ${client?.id})`, "sys");
        client?.sessionStart({
          encoding: CAPTURE_ENCODING,
          sampleRate: CAPTURE_SAMPLE_RATE,
          channels: CAPTURE_CHANNELS,
        });
        log("-> session_start", "out");
      }
      if (status === "disconnected") {
        void stopTalking();
        player.reset();
        if (!intentionalDisconnect) {
          showBanner("Session lost — reconnect to continue.", "error");
          setPipelineState("error");
        }
        clearChat();
        client = null;
        setStatus("disconnected");
        log("disconnected", "sys");
      }
      if (status === "error") {
        showBanner("Connection error — try reconnecting.", "error");
        setPipelineState("error");
        log("connection error", "sys");
      }
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
  if (!client?.connected || pttBlocked || talkSession) return;

  const session: TalkSession = { turnId: crypto.randomUUID(), seq: 0, micReady: false };
  talkSession = session;

  micError.textContent = "";
  hideBanner();
  ensureTurn(session.turnId);
  talkBtn.classList.add("recording");
  talkBtn.textContent = "Starting mic…";
  setPipelineState("listening");
  void player.unlock();

  try {
    await capture.start((pcm) => {
      if (!talkSession || talkSession !== session || !client?.connected) return;
      client.audioChunk(session.turnId, session.seq, arrayBufferToBase64(pcm));
      session.seq += 1;
    });

    session.micReady = true;

    // Released while waiting for getUserMedia / worklet setup.
    if (!talkSession || talkSession !== session) {
      await capture.stop();
      cancelTurnBlock(session.turnId);
      resetTalkButton();
      setPipelineState("idle");
      return;
    }

    talkBtn.textContent = "Recording… release to send";
    log(`-> audio capture started (turn ${session.turnId})`, "out");
  } catch (err) {
    talkSession = null;
    cancelTurnBlock(session.turnId);
    resetTalkButton();
    setPipelineState("idle");
    const message = err instanceof CaptureError ? err.message : "Could not start microphone.";
    micError.textContent = message;
    log(`mic error: ${message}`, "sys");
  }
}

async function stopTalking(): Promise<void> {
  const session = talkSession;
  if (!session) return;

  talkSession = null;
  resetTalkButton();
  await capture.stop();

  if (!session.micReady) {
    cancelTurnBlock(session.turnId);
    setPipelineState("idle");
    log("-> talk cancelled (mic not ready)", "sys");
    return;
  }

  if (session.seq === 0) {
    cancelTurnBlock(session.turnId);
    setPipelineState("idle");
    log("-> talk cancelled (no audio captured)", "sys");
    return;
  }

  if (client?.connected) {
    client.utteranceEnd(session.turnId, session.seq);
    log(`-> utterance_end (${session.seq} chunks)`, "out");
    setPipelineState("thinking");
  } else {
    setPipelineState("error");
  }
}

function resetTalkButton(): void {
  talkBtn.classList.remove("recording");
  talkBtn.textContent = "Hold to talk";
}

talkBtn.addEventListener("pointerdown", (event) => {
  event.preventDefault();
  talkBtn.setPointerCapture(event.pointerId);
  void startTalking();
});
talkBtn.addEventListener("pointerup", (event) => {
  if (talkBtn.hasPointerCapture(event.pointerId)) {
    talkBtn.releasePointerCapture(event.pointerId);
  }
  void stopTalking();
});
talkBtn.addEventListener("pointercancel", (event) => {
  if (talkBtn.hasPointerCapture(event.pointerId)) {
    talkBtn.releasePointerCapture(event.pointerId);
  }
  void stopTalking();
});

setStatus("disconnected");
setPipelineState("idle");
