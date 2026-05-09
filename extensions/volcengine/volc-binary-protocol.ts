/**
 * Volcengine ASR v3 WebSocket binary protocol codec.
 *
 * Reference: https://www.volcengine.com/docs/6561/1354869
 * Aligned with newcallcall/src/newcallcall/ai/stt_volc.py
 */

import { gzipSync, gunzipSync } from "node:zlib";

// ── Protocol constants ──────────────────────────────────────────

const PROTO_VERSION = 0b0001;
const PROTO_HEADER_SIZE = 0b0001; // 1 × 4 = 4 bytes

const MSG_CLIENT_FULL = 0b0001;
const MSG_CLIENT_AUDIO = 0b0010;
const MSG_SERVER_FULL = 0b1001;
const MSG_SERVER_ERROR = 0b1111;

const FLAG_NO_SEQ = 0b0000;
const FLAG_POS_SEQ = 0b0001;
const FLAG_LAST_PKG = 0b0010;
const FLAG_NEG_SEQ = 0b0011;

const SERIAL_NONE = 0b0000;
const SERIAL_JSON = 0b0001;

const COMPRESS_NONE = 0b0000;
const COMPRESS_GZIP = 0b0001;

// ── Types ───────────────────────────────────────────────────────

export type VolcResponse = {
  text: string;
  isLast: boolean;
  errorCode: number;
  errorMessage: string;
  payload?: Record<string, unknown>;
};

// ── Frame builders ──────────────────────────────────────────────

function buildHeader(msgType: number, flags: number, serial: number, compress: number): Buffer {
  return Buffer.from([
    (PROTO_VERSION << 4) | PROTO_HEADER_SIZE,
    (msgType << 4) | flags,
    (serial << 4) | compress,
    0x00, // reserved
  ]);
}

/**
 * Build a full client request frame (JSON config, gzip compressed).
 * Layout: [4B header] [4B seq BE int32] [4B payloadSize BE uint32] [gzip'd JSON]
 */
export function buildVolcFullClientRequest(seq: number, payload: Record<string, unknown>): Buffer {
  const header = buildHeader(MSG_CLIENT_FULL, FLAG_POS_SEQ, SERIAL_JSON, COMPRESS_GZIP);
  const raw = Buffer.from(JSON.stringify(payload), "utf-8");
  const compressed = gzipSync(raw);
  const buf = Buffer.concat([
    header,
    seqToBuffer(seq),
    sizeToBuffer(compressed.length),
    compressed,
  ]);
  return buf;
}

/**
 * Build an audio-only request frame (raw PCM, gzip compressed).
 * Layout: [4B header] [4B seq BE int32] [4B payloadSize BE uint32] [gzip'd PCM]
 * When isLast=true, flags=NEG_SEQ and seq becomes negative.
 */
export function buildVolcAudioRequest(seq: number, audio: Buffer, isLast = false): Buffer {
  const flags = isLast ? FLAG_NEG_SEQ : FLAG_POS_SEQ;
  const header = buildHeader(MSG_CLIENT_AUDIO, flags, SERIAL_NONE, COMPRESS_GZIP);
  const actualSeq = isLast ? -seq : seq;
  const compressed = gzipSync(audio);
  return Buffer.concat([
    header,
    seqToBuffer(actualSeq),
    sizeToBuffer(compressed.length),
    compressed,
  ]);
}

function seqToBuffer(seq: number): Buffer {
  const b = Buffer.allocUnsafe(4);
  b.writeInt32BE(seq, 0);
  return b;
}

function sizeToBuffer(size: number): Buffer {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32BE(size, 0);
  return b;
}

// ── Response parser ─────────────────────────────────────────────

/**
 * Parse a server binary response.
 * Returns a VolcResponse with errorCode=0 on success,
 * -1 on parse failure, or the server error code.
 */
export function parseVolcResponse(data: Buffer): VolcResponse {
  const resp: VolcResponse = {
    text: "",
    isLast: false,
    errorCode: 0,
    errorMessage: "",
  };

  if (data.length < 4) {
    return { ...resp, errorCode: -1, errorMessage: "Response too short" };
  }

  const headerSize = (data[0] & 0x0f) * 4;
  const msgType = data[1] >> 4;
  const flags = data[1] & 0x0f;
  const serial = data[2] >> 4;
  const compress = data[2] & 0x0f;

  let payload = data.subarray(headerSize);

  // Skip sequence number if present
  if (flags & 0x01 && payload.length >= 4) {
    payload = payload.subarray(4);
  }

  resp.isLast = Boolean(flags & 0x02);

  // ── Error response ──
  if (msgType === MSG_SERVER_ERROR) {
    if (payload.length < 8) {
      return { ...resp, errorCode: -1, errorMessage: "Malformed error frame" };
    }
    resp.errorCode = payload.readInt32BE(0);
    const msgSize = payload.readUInt32BE(4);
    resp.errorMessage = payload.subarray(8, 8 + msgSize).toString("utf-8", 0, msgSize);
    return resp;
  }

  // ── Full server response ──
  if (msgType === MSG_SERVER_FULL) {
    if (payload.length < 4) {
      return resp;
    }
    const psize = payload.readUInt32BE(0);
    payload = payload.subarray(4, 4 + psize);
  } else {
    // Unknown / unsupported message type
    return resp;
  }

  if (!payload || payload.length === 0) {
    return resp;
  }

  // Decompress
  if (compress === COMPRESS_GZIP) {
    try {
      payload = gunzipSync(payload);
    } catch {
      return { ...resp, errorCode: -1, errorMessage: "Gzip decompress failed" };
    }
  }

  // Deserialize
  if (serial === SERIAL_JSON) {
    try {
      const obj = JSON.parse(payload.toString("utf-8")) as Record<string, unknown>;
      resp.payload = obj;
      resp.text = extractText(obj);
    } catch {
      return { ...resp, errorCode: -1, errorMessage: "JSON parse failed" };
    }
  }

  return resp;
}

function extractText(resp: Record<string, unknown>): string {
  const result = resp.result;
  if (result && typeof result === "object" && !Array.isArray(result)) {
    const text = (result as Record<string, unknown>).text;
    if (typeof text === "string") {
      return text.trim();
    }
  }
  if (typeof result === "string") {
    return result.trim();
  }
  return "";
}
