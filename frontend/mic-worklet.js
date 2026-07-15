// Runs on the audio rendering thread. Downsamples the mic input to 16kHz PCM16
// (what Cartesia's realtime STT expects) and does simple energy-based voice
// activity detection so the main thread knows when the caller stopped talking.
class MicProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const opts = options.processorOptions || {};
    this.targetSampleRate = opts.targetSampleRate || 16000;
    this.energyThreshold = opts.energyThreshold ?? 0.02;
    this.speechStartMs = opts.speechStartMs ?? 150;
    this.silenceHangoverMs = opts.silenceHangoverMs ?? 1300;
    this.chunkSamples = Math.round(this.targetSampleRate * 0.1); // 100ms chunks

    this.hist = []; // small rolling buffer of native-rate samples for interpolation
    this.nativePos = 0; // fractional read position into hist, in native-sample units
    this.pending = []; // resampled int16 samples waiting to be flushed as a chunk

    this.speaking = false;
    this.speechMs = 0;
    this.silenceMs = 0;
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || input.length === 0 || !input[0] || input[0].length === 0) {
      return true;
    }
    const channel = input[0];
    const nativeRate = sampleRate; // AudioWorkletGlobalScope global

    // --- VAD on the raw native-rate block ---
    let sumSquares = 0;
    for (let i = 0; i < channel.length; i++) sumSquares += channel[i] * channel[i];
    const rms = Math.sqrt(sumSquares / channel.length);
    const blockMs = (channel.length / nativeRate) * 1000;

    if (rms > this.energyThreshold) {
      this.speechMs += blockMs;
      this.silenceMs = 0;
      if (!this.speaking && this.speechMs >= this.speechStartMs) {
        this.speaking = true;
        this.port.postMessage({ type: "vad", event: "speech_start" });
      }
    } else {
      this.silenceMs += blockMs;
      this.speechMs = 0;
      if (this.speaking && this.silenceMs >= this.silenceHangoverMs) {
        this.speaking = false;
        this.port.postMessage({ type: "vad", event: "speech_end" });
      }
    }

    // --- linear-interpolation resample down to targetSampleRate ---
    for (let i = 0; i < channel.length; i++) this.hist.push(channel[i]);

    const step = nativeRate / this.targetSampleRate;
    while (this.nativePos + 1 < this.hist.length) {
      const idx = Math.floor(this.nativePos);
      const frac = this.nativePos - idx;
      const sample = this.hist[idx] * (1 - frac) + this.hist[idx + 1] * frac;
      const clamped = Math.max(-1, Math.min(1, sample));
      this.pending.push(Math.round(clamped * 32767));
      this.nativePos += step;
    }
    const consumed = Math.floor(this.nativePos);
    if (consumed > 0) {
      this.hist = this.hist.slice(consumed);
      this.nativePos -= consumed;
    }

    while (this.pending.length >= this.chunkSamples) {
      const chunk = this.pending.slice(0, this.chunkSamples);
      this.pending = this.pending.slice(this.chunkSamples);
      const int16 = new Int16Array(chunk);
      this.port.postMessage({ type: "audio", buffer: int16.buffer }, [int16.buffer]);
    }

    return true;
  }
}

registerProcessor("mic-processor", MicProcessor);
