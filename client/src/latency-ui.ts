/** Latency waterfall + last-N table (issue #17). */

export interface LatencyStages {
  clientCaptureMs?: number | null;
  audioUploadMs?: number | null;
  asrFirstPartialMs?: number | null;
  asrFinalMs?: number | null;
  llmTtftMs?: number | null;
  llmCompleteMs?: number | null;
  ttsTtfbMs?: number | null;
  ttsCompleteMs?: number | null;
  orchestrationOverheadMs?: number | null;
}

export interface LatencyReport {
  totalMs: number;
  bottleneckStage?: string | null;
  failedStage?: string | null;
  stages: LatencyStages;
  meta?: {
    totalMs?: number;
    bottleneckStage?: string | null;
    failedStage?: string | null;
    degraded?: boolean;
  };
}

export interface TurnLatencyRow {
  turnId: string;
  totalMs: number;
  bottleneckStage: string | null;
  feltMs: number | null;
  degraded: boolean;
  failedStage: string | null;
  report: LatencyReport;
}

interface TimelineSegment {
  label: string;
  stage: string;
  durationMs: number;
  cssVar: string;
}

const MAX_TURNS = 10;

const STAGE_LABELS: Record<string, string> = {
  asr: "ASR",
  llm: "LLM",
  tts: "TTS",
  upload: "Upload",
  overhead: "Overhead",
};

const STAGE_DESCRIPTIONS: Record<string, string> = {
  asr: "Speech recognition",
  llm: "LLM reply (through first token)",
  tts: "Text-to-speech (first audio byte)",
  overhead: "Pipeline finish",
  upload: "Audio upload",
};

export function formatMs(ms: number | null | undefined): string {
  if (ms == null || ms < 0) return "—";
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.round(ms)}ms`;
}

function shortTurnId(turnId: string): string {
  return turnId.length > 8 ? `${turnId.slice(0, 8)}…` : turnId;
}

/** Non-overlapping server segments that sum to ``totalMs`` (milestone gaps from T₀). */
export function buildServerTimeline(stages: LatencyStages, totalMs: number): TimelineSegment[] {
  if (totalMs <= 0) return [];

  type Milestone = { ms: number; label: string; stage: string; cssVar: string };
  const milestones: Milestone[] = [];

  const add = (ms: number | null | undefined, label: string, stage: string, cssVar: string) => {
    if (ms == null || ms <= 0) return;
    milestones.push({ ms: Math.min(ms, totalMs), label, stage, cssVar });
  };

  add(stages.asrFinalMs, "ASR", "asr", "--lat-asr");
  add(stages.llmTtftMs, "LLM", "llm", "--lat-llm");
  add(stages.ttsTtfbMs, "TTS start", "tts", "--lat-tts");

  milestones.sort((a, b) => a.ms - b.ms);

  const unique: Milestone[] = [];
  for (const m of milestones) {
    const last = unique[unique.length - 1];
    if (last && last.ms === m.ms) {
      unique[unique.length - 1] = m;
    } else if (!last || m.ms > last.ms) {
      unique.push(m);
    }
  }

  const ticks = [0, ...unique.map((m) => m.ms)];
  if (ticks[ticks.length - 1] < totalMs) ticks.push(totalMs);

  const segments: TimelineSegment[] = [];
  for (let i = 0; i < ticks.length - 1; i++) {
    const durationMs = ticks[i + 1] - ticks[i];
    if (durationMs <= 0) continue;

    const reached = unique.find((m) => m.ms === ticks[i + 1]);
    if (reached) {
      segments.push({
        label: reached.label,
        stage: reached.stage,
        durationMs,
        cssVar: reached.cssVar,
      });
    } else {
      segments.push({
        label: "Finish",
        stage: "overhead",
        durationMs,
        cssVar: "--lat-overhead",
      });
    }
  }
  return segments;
}

/** Slowest pipeline segment on the server timeline (matches bar widths). */
export function bottleneckFromTimeline(
  stages: LatencyStages,
  totalMs: number,
): string | null {
  const segments = buildServerTimeline(stages, totalMs);
  const pipeline = segments.filter((s) => s.stage !== "overhead");
  const pool = pipeline.length > 0 ? pipeline : segments;
  let best: string | null = null;
  let bestMs = -1;
  for (const seg of pool) {
    if (seg.durationMs > bestMs) {
      bestMs = seg.durationMs;
      best = seg.stage;
    }
  }
  if (best != null) return best;
  if (stages.ttsCompleteMs != null) return "tts";
  if (stages.llmCompleteMs != null) return "llm";
  if (stages.asrFinalMs != null) return "asr";
  return null;
}

export class LatencyUi {
  private readonly history: TurnLatencyRow[] = [];

  constructor(
    private readonly statsEl: HTMLElement,
    private readonly waterfallEl: HTMLElement,
    private readonly tableBody: HTMLElement,
  ) {}

  recordTurn(
    turnId: string,
    report: LatencyReport,
    feltMs: number | null,
    degraded: boolean,
  ): void {
    const bottleneckStage =
      bottleneckFromTimeline(report.stages, report.totalMs) ??
      report.bottleneckStage ??
      report.meta?.bottleneckStage ??
      null;
    const row: TurnLatencyRow = {
      turnId,
      totalMs: report.totalMs,
      bottleneckStage,
      feltMs,
      degraded,
      failedStage: report.failedStage ?? report.meta?.failedStage ?? null,
      report,
    };
    this.history.unshift(row);
    if (this.history.length > MAX_TURNS) this.history.length = MAX_TURNS;
    this.renderLatest(row);
    this.renderTable();
  }

  clear(): void {
    this.history.length = 0;
    this.statsEl.replaceChildren();
    const hint = document.createElement("p");
    hint.className = "latency-hint";
    hint.textContent = "Complete a turn to see latency.";
    this.statsEl.appendChild(hint);
    this.waterfallEl.replaceChildren();
    this.tableBody.replaceChildren();
  }

  private renderLatest(row: TurnLatencyRow): void {
    this.renderStats(row);
    this.renderWaterfall(row);
  }

  private renderStats(row: TurnLatencyRow): void {
    const bn = row.bottleneckStage;
    const bnLabel = bn ? (STAGE_LABELS[bn] ?? bn.toUpperCase()) : "—";

    this.statsEl.replaceChildren();

    const grid = document.createElement("div");
    grid.className = "latency-stat-grid";

    grid.append(
      this.statCard("Server total", formatMs(row.totalMs), "After you release"),
      this.statCard("Felt latency", formatMs(row.feltMs), "Release → first audio"),
      this.statCard("Bottleneck", bnLabel, bn ? "Slowest stage" : undefined, bn != null),
    );

    if (row.report.stages.clientCaptureMs != null) {
      grid.append(this.statCard("Hold time", formatMs(row.report.stages.clientCaptureMs), "While button pressed"));
    }
    if (row.failedStage) {
      grid.append(this.statCard("Failed at", row.failedStage.toUpperCase(), "Degraded turn", true));
    }

    this.statsEl.appendChild(grid);
  }

  private statCard(
    label: string,
    value: string,
    hint?: string,
    highlight = false,
  ): HTMLElement {
    const card = document.createElement("div");
    card.className = `latency-stat${highlight ? " highlight" : ""}`;
    card.innerHTML = `<span class="latency-stat-label">${label}</span>
      <span class="latency-stat-value">${value}</span>`;
    if (hint) {
      const sub = document.createElement("span");
      sub.className = "latency-stat-hint";
      sub.textContent = hint;
      card.appendChild(sub);
    }
    return card;
  }

  private segmentBar(
    seg: TimelineSegment,
    totalMs: number,
    bottleneck: string | null,
    index: number,
    count: number,
  ): HTMLDivElement {
    const pct = (seg.durationMs / totalMs) * 100;
    const isBottleneck = bottleneck != null && seg.stage === bottleneck;
    const bar = document.createElement("div");
    bar.className = "latency-seg";
    if (isBottleneck) bar.classList.add("bottleneck");
    bar.style.width = `${pct}%`;
    bar.style.background = `var(${seg.cssVar})`;
    bar.tabIndex = 0;

    const tip = document.createElement("span");
    tip.className = "latency-seg-tooltip";
    tip.setAttribute("role", "tooltip");

    const title = document.createElement("strong");
    title.textContent = seg.label;
    tip.appendChild(title);

    const desc = STAGE_DESCRIPTIONS[seg.stage];
    if (desc) {
      const descEl = document.createElement("span");
      descEl.className = "latency-seg-tooltip-desc";
      descEl.textContent = desc;
      tip.appendChild(descEl);
    }

    const meta = document.createElement("span");
    meta.className = "latency-seg-tooltip-meta";
    meta.textContent = `${formatMs(seg.durationMs)} · ${pct.toFixed(0)}% of server total`;
    tip.appendChild(meta);

    if (isBottleneck) {
      const bn = document.createElement("span");
      bn.className = "latency-seg-tooltip-bn";
      bn.textContent = "Bottleneck";
      tip.appendChild(bn);
    }

    bar.appendChild(tip);

    const label = document.createElement("span");
    label.className = "latency-seg-label";
    label.textContent = pct >= 14 ? `${seg.label} ${formatMs(seg.durationMs)}` : formatMs(seg.durationMs);
    bar.appendChild(label);

    if (index === 0) bar.classList.add("seg-first");
    if (index === count - 1) bar.classList.add("seg-last");

    return bar;
  }

  private renderWaterfall(row: TurnLatencyRow): void {
    const { stages } = row.report;
    const bottleneck = row.bottleneckStage;
    const segments = buildServerTimeline(stages, row.totalMs);

    this.waterfallEl.replaceChildren();

    if (stages.clientCaptureMs != null && stages.clientCaptureMs > 0) {
      this.waterfallEl.appendChild(this.captureRow(stages.clientCaptureMs, row.totalMs));
    }

    const block = document.createElement("div");
    block.className = "latency-timeline-block";

    const heading = document.createElement("p");
    heading.className = "latency-timeline-label";
    heading.textContent = "Server pipeline (from release)";
    block.appendChild(heading);

    if (segments.length === 0) {
      const empty = document.createElement("p");
      empty.className = "latency-empty";
      empty.textContent = "No server timings recorded.";
      block.appendChild(empty);
    } else {
      const track = document.createElement("div");
      track.className = "latency-track";
      track.setAttribute("role", "img");
      track.setAttribute(
        "aria-label",
        segments.map((s) => `${s.label} ${formatMs(s.durationMs)}`).join(", "),
      );

      segments.forEach((seg, i) => {
        track.appendChild(this.segmentBar(seg, row.totalMs, bottleneck, i, segments.length));
      });
      block.appendChild(track);

      const legend = document.createElement("p");
      legend.className = "latency-legend";
      const parts: string[] = [];
      if (stages.asrFinalMs != null) parts.push(`ASR @ ${formatMs(stages.asrFinalMs)}`);
      if (stages.llmTtftMs != null) parts.push(`LLM TTFT @ ${formatMs(stages.llmTtftMs)}`);
      if (stages.ttsTtfbMs != null) parts.push(`TTS @ ${formatMs(stages.ttsTtfbMs)}`);
      legend.textContent = parts.length
        ? `Milestones from release: ${parts.join(" · ")}`
        : "";
      if (parts.length) block.appendChild(legend);
    }

    this.waterfallEl.appendChild(block);
  }

  private captureRow(captureMs: number, serverTotalMs: number): HTMLElement {
    const wrap = document.createElement("div");
    wrap.className = "latency-timeline-block capture";

    const heading = document.createElement("p");
    heading.className = "latency-timeline-label";
    heading.textContent = "While holding button";
    wrap.appendChild(heading);

    const track = document.createElement("div");
    track.className = "latency-track capture-track";
    const maxMs = Math.max(captureMs, serverTotalMs);
    const pct = Math.min(100, (captureMs / maxMs) * 100);

    const bar = document.createElement("div");
    bar.className = "latency-seg capture-seg seg-first seg-last";
    bar.style.width = `${pct}%`;
    bar.tabIndex = 0;

    const tip = document.createElement("span");
    tip.className = "latency-seg-tooltip";
    tip.setAttribute("role", "tooltip");
    const title = document.createElement("strong");
    title.textContent = "Mic capture";
    tip.appendChild(title);
    const descEl = document.createElement("span");
    descEl.className = "latency-seg-tooltip-desc";
    descEl.textContent = "While holding push-to-talk";
    tip.appendChild(descEl);
    const meta = document.createElement("span");
    meta.className = "latency-seg-tooltip-meta";
    meta.textContent = formatMs(captureMs);
    tip.appendChild(meta);
    bar.appendChild(tip);

    const label = document.createElement("span");
    label.className = "latency-seg-label";
    label.textContent = formatMs(captureMs);
    bar.appendChild(label);
    track.appendChild(bar);
    wrap.appendChild(track);
    return wrap;
  }

  private renderTable(): void {
    this.tableBody.replaceChildren();
    for (const row of this.history) {
      const tr = document.createElement("tr");
      if (row.degraded) tr.classList.add("degraded");

      const bn = row.bottleneckStage;
      const bnLabel = bn ? (STAGE_LABELS[bn] ?? bn) : "—";

      for (const text of [shortTurnId(row.turnId), formatMs(row.totalMs), formatMs(row.feltMs), bnLabel]) {
        const td = document.createElement("td");
        td.textContent = text;
        if (text === bnLabel && bn) td.classList.add("bottleneck-cell");
        tr.appendChild(td);
      }
      this.tableBody.appendChild(tr);
    }
  }
}
