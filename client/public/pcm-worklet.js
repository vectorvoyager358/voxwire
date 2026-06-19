// AudioWorklet processor: converts mic audio into 16 kHz mono PCM signed-16-bit
// little-endian frames and posts them to the main thread in ~100 ms chunks.
//
// Why this exists: the event protocol (docs/event-protocol.md) mandates raw
// pcm_s16le @ 16 kHz upstream. MediaRecorder only yields compressed WebM/Opus,
// so we tap raw float samples here, (re)sample to 16 kHz, and quantize to int16.
//
// We try to run the AudioContext at 16 kHz directly; if the browser ignores that
// and runs at the device rate (e.g. 48 kHz), we linear-resample as a fallback so
// the wire format is always exactly 16 kHz.

class PCMWorkletProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const opts = (options && options.processorOptions) || {};
    this.targetRate = opts.targetSampleRate || 16000;
    const chunkMs = opts.chunkMs || 100;
    this.chunkSamples = Math.round((this.targetRate * chunkMs) / 1000);

    this.buffer = new Int16Array(this.chunkSamples);
    this.count = 0;

    // `sampleRate` is a global provided by AudioWorkletGlobalScope (the real
    // context rate). ratio > 1 means we must downsample.
    this.ratio = sampleRate / this.targetRate;
    this.inIndex = 0; // global index of the next incoming sample
    this.outPos = 0; // fractional read position (in input-sample units)
    this.prev = 0; // last sample of the previous block, for interpolation

    this.port.onmessage = (event) => {
      if (event.data === "flush") this.flush();
    };
  }

  pushSample(sample) {
    const clamped = Math.max(-1, Math.min(1, sample));
    this.buffer[this.count++] = clamped < 0 ? clamped * 0x8000 : clamped * 0x7fff;
    if (this.count === this.chunkSamples) {
      this.port.postMessage(this.buffer.buffer.slice(0));
      this.count = 0;
    }
  }

  flush() {
    if (this.count > 0) {
      this.port.postMessage(this.buffer.slice(0, this.count).buffer);
      this.count = 0;
    }
  }

  process(inputs) {
    const input = inputs[0];
    const channel = input && input[0];
    if (!channel || channel.length === 0) return true;

    if (this.ratio === 1) {
      for (let i = 0; i < channel.length; i++) this.pushSample(channel[i]);
    } else {
      const base = this.inIndex;
      const lastAvail = base + channel.length - 1;
      if (this.outPos < base) this.outPos = base;
      while (true) {
        const i0 = Math.floor(this.outPos);
        const i1 = i0 + 1;
        if (i1 > lastAvail) break; // need next block's first sample to interpolate
        const frac = this.outPos - i0;
        const s0 = i0 === base - 1 ? this.prev : channel[i0 - base];
        const s1 = channel[i1 - base];
        this.pushSample(s0 + (s1 - s0) * frac);
        this.outPos += this.ratio;
      }
    }

    this.prev = channel[channel.length - 1];
    this.inIndex += channel.length;
    return true;
  }
}

registerProcessor("pcm-worklet", PCMWorkletProcessor);
