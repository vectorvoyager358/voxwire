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
import { LatencyUi, type LatencyReport } from "./latency-ui";

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
const bannerEl = document.getElementById("banner") as HTMLDivElement;
const bannerStageEl = document.getElementById("bannerStage") as HTMLParagraphElement;
const bannerMessageEl = document.getElementById("bannerMessage") as HTMLParagraphElement;
const bannerActionsEl = document.getElementById("bannerActions") as HTMLDivElement;
const bannerRetryBtn = document.getElementById("bannerRetry") as HTMLButtonElement;
const textFallbackEl = document.getElementById("textFallback") as HTMLDivElement;
const textFallbackInput = document.getElementById("textFallbackInput") as HTMLInputElement;
const textFallbackSend = document.getElementById("textFallbackSend") as HTMLButtonElement;
const latencyStats = document.getElementById("latencyStats") as HTMLDivElement;
const latencyWaterfall = document.getElementById("latencyWaterfall") as HTMLDivElement;
const latencyTableBody = document.getElementById("latencyTableBody") as HTMLTableSectionElement;

interface LatencySummaryMeta {
  totalMs?: number;
  bottleneckStage?: string | null;
  failedStage?: string | null;
  degraded?: boolean;
}

interface TurnCompleteMeta {
  degraded?: boolean;
  ttsSkipped?: boolean;
  ttsChunks?: number;
  latency?: LatencySummaryMeta;
  latency_report?: LatencyReport;
}

interface ServerMessage {
  type?: string;
  turnId?: string;
  seq?: number;
  data?: string;
  sampleRate?: number;
  text?: string;
  stage?: string;
  code?: string;
  message?: string;
  recoverable?: boolean;
  meta?: TurnCompleteMeta | LatencySummaryMeta;
  totalMs?: number;
  bottleneckStage?: string | null;
  failedStage?: string | null;
  stages?: LatencyReport["stages"];
}

type PipelineState = "idle" | "listening" | "thinking" | "speaking" | "error";

interface ActiveTurn {
  turnId: string;
  root: HTMLDivElement;
  you: HTMLSpanElement;
  youCopy: HTMLButtonElement;
  assistant: HTMLSpanElement;
  assistantCopy: HTMLButtonElement;
  footnote: HTMLParagraphElement;
  assistantText: string;
}

/** In-flight or active push-to-talk capture for one utterance. */
interface TalkSession {
  turnId: string;
  seq: number;
  micReady: boolean;
  captureStartAt: number;
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
  bannerStageEl.hidden = true;
  bannerMessageEl.textContent = text;
  bannerActionsEl.hidden = true;
  bannerEl.className = `banner visible ${kind}`;
  setTextFallbackVisible(false);
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
const latencyUi = new LatencyUi(latencyStats, latencyWaterfall, latencyTableBody);
const utteranceEndAt = new Map<string, number>();
const feltLatencyMs = new Map<string, number>();

const player = new TtsPlayer((info) => {
  const endAt = utteranceEndAt.get(info.turnId);
  if (endAt != null) {
    feltLatencyMs.set(info.turnId, Math.max(0, info.at - endAt));
  }
  log(`playback_start (turn ${info.turnId})`, "sys");
  setPipelineState("speaking");
});

let activeTurn: ActiveTurn | null = null;
let pttBlocked = false;
let showTextFallback = false;
let lastRecoverableError: { stage: string; message: string } | null = null;

const STAGE_LABELS: Record<string, string> = {
  asr: "Speech recognition",
  llm: "Assistant",
  tts: "Audio",
  orchestrator: "Pipeline",
  transport: "Connection",
};

function stageLabel(stage: string): string {
  return STAGE_LABELS[stage] ?? stage;
}

function hideBanner(): void {
  bannerStageEl.hidden = true;
  bannerMessageEl.textContent = "";
  bannerActionsEl.hidden = true;
  bannerEl.className = "banner";
  lastRecoverableError = null;
}

function showErrorBanner(
  stage: string,
  message: string,
  options: { recoverable?: boolean; kind?: "error" | "warn" | "info"; showRetry?: boolean } = {},
): void {
  const recoverable = options.recoverable ?? true;
  const kind = options.kind ?? (recoverable ? "warn" : "error");
  bannerStageEl.textContent = stageLabel(stage);
  bannerStageEl.hidden = false;
  bannerMessageEl.textContent = message;
  bannerEl.className = `banner visible ${kind}`;

  const showRetry = options.showRetry ?? recoverable;
  bannerActionsEl.hidden = !showRetry;
  bannerRetryBtn.hidden = !showRetry;

  if (recoverable) {
    lastRecoverableError = { stage, message };
  } else {
    lastRecoverableError = null;
  }

  showTextFallback = recoverable && stage === "asr";
  textFallbackEl.classList.toggle("visible", showTextFallback);
}

function setTextFallbackVisible(visible: boolean): void {
  showTextFallback = visible;
  textFallbackEl.classList.toggle("visible", visible);
  if (!visible) textFallbackInput.value = "";
}

function scrollChatToBottom(): void {
  chatScroll.scrollTop = chatScroll.scrollHeight;
}

const COPY_ICON = `<svg aria-hidden="true" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>`;

const COPIED_ICON = `<svg aria-hidden="true" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>`;

function setCopyButtonIcon(btn: HTMLButtonElement, copied = false): void {
  btn.innerHTML = copied ? COPIED_ICON : COPY_ICON;
}

async function copyMessage(btn: HTMLButtonElement, textEl: HTMLSpanElement): Promise<void> {
  const text = textEl.textContent?.trim();
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const area = document.createElement("textarea");
    area.value = text;
    area.style.position = "fixed";
    area.style.left = "-9999px";
    document.body.appendChild(area);
    area.select();
    document.execCommand("copy");
    area.remove();
  }
  setCopyButtonIcon(btn, true);
  btn.classList.add("copied");
  window.setTimeout(() => {
    setCopyButtonIcon(btn, false);
    btn.classList.remove("copied");
  }, 1500);
}

function syncCopyButton(btn: HTMLButtonElement, text: string): void {
  btn.disabled = !text.trim();
}

function makeBubble(
  who: string,
  extraClass = "",
): { root: HTMLDivElement; text: HTMLSpanElement; copyBtn: HTMLButtonElement } {
  const root = document.createElement("div");
  root.className = `bubble ${extraClass}`.trim();

  const head = document.createElement("div");
  head.className = "bubble-head";

  const label = document.createElement("span");
  label.className = "who";
  label.textContent = who;

  const text = document.createElement("span");
  text.className = "text";

  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.className = "copy-btn";
  copyBtn.setAttribute("aria-label", `Copy ${who} message`);
  copyBtn.title = "Copy";
  setCopyButtonIcon(copyBtn);
  copyBtn.disabled = true;
  copyBtn.addEventListener("click", () => void copyMessage(copyBtn, text));

  head.append(label, copyBtn);
  root.append(head, text);
  return { root, text, copyBtn };
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
    youCopy: youBubble.copyBtn,
    assistant: assistantBubble.text,
    assistantCopy: assistantBubble.copyBtn,
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
  syncCopyButton(activeTurn.youCopy, text);
  scrollChatToBottom();
}

function appendAssistant(text: string): void {
  if (!activeTurn) return;
  activeTurn.assistantText += text;
  activeTurn.assistant.textContent = activeTurn.assistantText;
  syncCopyButton(activeTurn.assistantCopy, activeTurn.assistantText);
  scrollChatToBottom();
}

function setAssistantComplete(text: string): void {
  if (!activeTurn) return;
  activeTurn.assistantText = text;
  activeTurn.assistant.textContent = text;
  syncCopyButton(activeTurn.assistantCopy, text);
  scrollChatToBottom();
}

function finishTurn(meta?: TurnCompleteMeta): void {
  if (!activeTurn) return;
  activeTurn.footnote.replaceChildren();
  if (meta?.degraded) {
    const badge = document.createElement("span");
    badge.className = "turn-badge degraded";
    badge.textContent = "Degraded turn";
    activeTurn.footnote.appendChild(badge);
  }
  if (meta?.ttsSkipped) {
    const badge = document.createElement("span");
    badge.className = "turn-badge tts-skipped";
    badge.textContent = "Text only — no audio";
    activeTurn.footnote.appendChild(badge);
  }
  activeTurn.footnote.hidden = activeTurn.footnote.childElementCount === 0;
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
  setTextFallbackVisible(false);
  pttBlocked = false;
  utteranceEndAt.clear();
  feltLatencyMs.clear();
  latencyUi.clear();
  setPipelineState("idle");
  setTalkEnabled(client?.connected ?? false);
}

function ingestLatencyReport(
  turnId: string,
  report: LatencyReport,
  degraded: boolean,
): void {
  latencyUi.recordTurn(turnId, report, feltLatencyMs.get(turnId) ?? null, degraded);
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
      if (!(msg.text ?? "").trim() && !lastRecoverableError) {
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
    case "latency_report": {
      if (typeof msg.turnId !== "string" || msg.totalMs == null || !msg.stages) return;
      const report: LatencyReport = {
        totalMs: msg.totalMs,
        bottleneckStage: msg.bottleneckStage,
        failedStage: msg.failedStage,
        stages: msg.stages,
        meta: msg.meta,
      };
      ingestLatencyReport(
        msg.turnId,
        report,
        (msg.meta as LatencySummaryMeta | undefined)?.degraded ?? msg.failedStage != null,
      );
      log(`<- latency_report total=${msg.totalMs}ms`, "in");
      return;
    }
    case "turn_complete": {
      finishTurn(msg.meta);
      if (msg.meta?.degraded && lastRecoverableError) {
        showErrorBanner(lastRecoverableError.stage, lastRecoverableError.message, {
          recoverable: true,
          kind: "warn",
          showRetry: true,
        });
        setPipelineState("error");
      } else if (msg.meta?.degraded) {
        showBanner("Turn completed with errors.", "warn");
        setPipelineState("error");
      } else {
        hideBanner();
        setTextFallbackVisible(false);
        setPipelineState("idle");
      }
      log(`<- turn_complete (turn ${msg.turnId})`, "in");
      return;
    }
    case "error": {
      const stage = msg.stage ?? "orchestrator";
      const detail = msg.message ?? "An error occurred.";
      const recoverable = msg.recoverable !== false;
      log(`<- error [${stage}] ${msg.code ?? "?"} ${detail}`, "sys");
      setPipelineState("error");
      if (!recoverable) {
        pttBlocked = true;
        setTalkEnabled(false);
        setTextFallbackVisible(false);
        showErrorBanner(stage, detail, { recoverable: false, kind: "error", showRetry: false });
      } else {
        showErrorBanner(stage, detail, { recoverable: true, kind: "warn", showRetry: true });
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

  hideBanner();
  setTextFallbackVisible(false);

  const session: TalkSession = {
    turnId: crypto.randomUUID(),
    seq: 0,
    micReady: false,
    captureStartAt: Date.now(),
  };
  talkSession = session;

  micError.textContent = "";
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
    const captureMs = Math.max(0, Date.now() - session.captureStartAt);
    utteranceEndAt.set(session.turnId, Date.now());
    client.utteranceEnd(session.turnId, session.seq, captureMs);
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

bannerRetryBtn.addEventListener("click", () => {
  hideBanner();
  setPipelineState("idle");
  talkBtn.focus();
});

async function sendTextFallback(): Promise<void> {
  if (!client?.connected || pttBlocked) return;
  const text = textFallbackInput.value.trim();
  if (!text) return;

  const turnId = crypto.randomUUID();
  hideBanner();
  setTextFallbackVisible(false);
  ensureTurn(turnId);
  setYou(text, true);
  setPipelineState("thinking");
  client.textTurn(turnId, text);
  log(`-> text_turn (${text.slice(0, 40)}…)`, "out");
}

textFallbackSend.addEventListener("click", () => void sendTextFallback());
textFallbackInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") void sendTextFallback();
});

setStatus("disconnected");
setPipelineState("idle");
latencyUi.clear();
