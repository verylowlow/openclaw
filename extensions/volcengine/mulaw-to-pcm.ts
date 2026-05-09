/**
 * G.711 μ-law → PCM 16-bit + 8kHz → 16kHz upsample.
 *
 * Twilio Media Streams sends G.711 μ-law at 8kHz mono.
 * Volcengine ASR expects PCM 16-bit little-endian at 16kHz mono.
 */

/**
 * Decode a single μ-law byte to 16-bit linear PCM (ITU-T G.711).
 */
function mulawToLinear16(muLawByte: number): number {
  // Invert bits (μ-law uses inverted bit order)
  muLawByte = ~muLawByte & 0xff;

  const sign = muLawByte & 0x80;
  const exponent = (muLawByte >> 4) & 0x07;
  const mantissa = muLawByte & 0x0f;

  let sample = ((mantissa << 3) + 0x84) << exponent;
  sample -= 0x84;

  return sign ? -sample : sample;
}

/**
 * Upsample 8kHz int16 samples to 16kHz using linear interpolation.
 * Output length = 2 × input length (last sample duplicated).
 */
function upsample8kTo16k(samples: Int16Array): Int16Array {
  const n = samples.length;
  const out = new Int16Array(n * 2);

  for (let i = 0; i < n; i++) {
    out[i * 2] = samples[i];
    if (i < n - 1) {
      out[i * 2 + 1] = (samples[i] + samples[i + 1]) >> 1;
    } else {
      out[i * 2 + 1] = samples[i];
    }
  }

  return out;
}

/**
 * Convert a Buffer of G.711 μ-law 8kHz bytes to PCM 16-bit little-endian 16kHz.
 */
export function convertMulawToPcm16k(muLawBuffer: Buffer): Buffer {
  // Step 1: μ-law → int16 (8kHz)
  const n = muLawBuffer.length;
  const pcm8k = new Int16Array(n);
  for (let i = 0; i < n; i++) {
    pcm8k[i] = mulawToLinear16(muLawBuffer[i]!);
  }

  // Step 2: 8kHz → 16kHz upsample
  const pcm16k = upsample8kTo16k(pcm8k);

  // Step 3: Int16Array → Buffer (little-endian)
  const buf = Buffer.allocUnsafe(pcm16k.length * 2);
  for (let i = 0; i < pcm16k.length; i++) {
    buf.writeInt16LE(pcm16k[i]!, i * 2);
  }

  return buf;
}
