/** Bundled server-side recordings for the replay dropdown (issue #23). */

export type ReplayMode = "events" | "full" | "mock";

export interface BundledSample {
  label: string;
  path: string;
  /** False when the fixture has no sibling ``.pcm`` (Full pipeline cannot run). */
  hasAudio: boolean;
}

export const CUSTOM_SAMPLE = "__custom__";

/** Recordings shipped under ``recordings/samples/`` on the server. */
export const BUNDLED_SAMPLES: BundledSample[] = [
  { label: "Hello turn (with audio)", path: "samples/hello-turn.jsonl", hasAudio: true },
  {
    label: "Degraded LLM (no audio)",
    path: "samples/degraded-llm-config.jsonl",
    hasAudio: false,
  },
];

export function isBundledSamplePath(path: string): boolean {
  return BUNDLED_SAMPLES.some((sample) => sample.path === path);
}

export function bundledSampleForPath(path: string): BundledSample | undefined {
  const relative = normalizeRecordingPath(path);
  return BUNDLED_SAMPLES.find((sample) => sample.path === relative);
}

/** Custom paths may still have audio on disk; only known no-audio fixtures disable Full. */
export function fullReplayAvailable(path: string): boolean {
  const sample = bundledSampleForPath(path);
  return sample == null || sample.hasAudio;
}

/** Path relative to RECORDINGS_DIR. Live turns are at the root, fixtures under samples/. */
export function normalizeRecordingPath(path: string): string {
  let relative = path.trim().replace(/\\/g, "/");
  if (relative.startsWith("./")) relative = relative.slice(2);
  if (relative.startsWith("recordings/")) relative = relative.slice("recordings/".length);
  return relative;
}

export function recordingPathForSample(sample: string, customPath: string): string {
  if (sample === CUSTOM_SAMPLE) return normalizeRecordingPath(customPath);
  return normalizeRecordingPath(sample);
}

/** Live turns are stored as ``{turnId}.jsonl`` under RECORDINGS_DIR. */
export function recordingPathForTurnId(turnId: string): string {
  const id = normalizeRecordingPath(turnId);
  if (!id) return "";
  return id.endsWith(".jsonl") ? id : `${id}.jsonl`;
}

export function replayedErrorBanner(stageLabel: string, message: string): string {
  return (
    `Replayed degraded turn — ${stageLabel}: ${message} ` +
    "(saved fixture; live API keys were not used.)"
  );
}

export function replayStartPayload(options: {
  recordingPath: string;
  mode: ReplayMode;
  mockLlm: boolean;
  turnId: string;
}): {
  recordingPath: string;
  mode: ReplayMode;
  mockLlm: boolean;
  turnId: string;
} {
  return {
    recordingPath: normalizeRecordingPath(options.recordingPath),
    mode: options.mode,
    mockLlm: options.mode === "mock" && options.mockLlm,
    turnId: options.turnId,
  };
}
