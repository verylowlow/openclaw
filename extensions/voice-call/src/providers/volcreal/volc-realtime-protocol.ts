/**
 * Volcengine Realtime API WebSocket binary protocol codec.
 *
 * Reference: https://www.volcengine.com/docs/6561/1354869
 * Aligned with Python official example: protocol.py + realtime_dialog_client.py
 *
 * Protocol layout:
 *   [header 4B] [optional event 4B BE] [optional session_id_size 4B BE] [session_id bytes]
 *   [payload_size 4B BE] [payload bytes]
 */

import { gzipSync, gunzipSync } from "node:zlib";

// ── Protocol constants ──────────────────────────────────────────

const PROTO_VERSION = 0b0001;
const DEFAULT_HEADER_SIZE = 0b0001; // 1 × 4 = 4 bytes

const CLIENT_FULL_REQUEST = 0b0001;
const CLIENT_AUDIO_ONLY_REQUEST = 0b0010;

const SERVER_FULL_RESPONSE = 0b1001;
const SERVER_ACK = 0b1011;
const SERVER_ERROR_RESPONSE = 0b1111;

// Message Type Specific Flags
const NO_SEQUENCE = 0b0000;
const POS_SEQUENCE = 0b0001;
const NEG_SEQUENCE = 0b0010;
const NEG_SEQUENCE_1 = 0b0011;
const MSG_WITH_EVENT = 0b0100;

// Message Serialization
const NO_SERIALIZATION = 0b0000;
const SERIAL_JSON = 0b0001;

// Message Compression
const NO_COMPRESSION = 0b0000;
const GZIP = 0b0001;

// ── Types ───────────────────────────────────────────────────────

export type VolcRealtimeEvent =
  | 1 // StartConnection
  | 2 // FinishConnection
  | 50 // ConnectionStarted
  | 100 // StartSession
  | 102 // FinishSession
  | 150 // SessionStarted
  | 200 // TaskRequest
  | 300 // SayHello
  | 352 // TTSResponse
  | 359 // TTSEnded
  | 450 // ASRInfo
  | 451 // ASRResponse
  | 459 // ASREnded
  | 500 // ChatTTSText
  | 501 // ChatTextQuery
  | 502 // ChatRAGText
  | 510 // ConversationCreate
  | 550 // ChatResponse
  | 559 // ChatEnded
  | 599; // DialogCommonError

export type VolcRealtimeParsedResponse = {
  messageType: "SERVER_FULL_RESPONSE" | "SERVER_ACK" | "SERVER_ERROR_RESPONSE" | "UNKNOWN";
  event?: number;
  seq?: number;
  sessionId?: string;
  payloadSize: number;
  payloadMsg?: unknown;
  code?: number;
};

// ── Header builder ──────────────────────────────────────────────

function generateHeader(options: {
  messageType?: number;
  messageTypeSpecificFlags?: number;
  serialMethod?: number;
  compressionType?: number;
  reservedData?: number;
  extensionHeader?: Buffer;
}): Buffer {
  const {
    messageType = CLIENT_FULL_REQUEST,
    messageTypeSpecificFlags = MSG_WITH_EVENT,
    serialMethod = SERIAL_JSON,
    compressionType = GZIP,
    reservedData = 0x00,
    extensionHeader = Buffer.alloc(0),
  } = options;

  const headerSize = Math.floor(extensionHeader.length / 4) + 1;
  const header = Buffer.alloc(headerSize * 4);
  header[0] = (PROTO_VERSION << 4) | headerSize;
  header[1] = (messageType << 4) | messageTypeSpecificFlags;
  header[2] = (serialMethod << 4) | compressionType;
  header[3] = reservedData;

  if (extensionHeader.length > 0) {
    extensionHeader.copy(header, 4);
  }

  return header;
}

// ── Frame builders ──────────────────────────────────────────────

/**
 * Build a control frame with JSON payload (gzip compressed).
 * Used for: StartConnection(1), StartSession(100), FinishSession(102), FinishConnection(2)
 */
export function buildVolcRealtimeControlFrame(
  event: number,
  payload: Record<string, unknown>,
  sessionId?: string,
): Buffer {
  const header = generateHeader({
    messageType: CLIENT_FULL_REQUEST,
    messageTypeSpecificFlags: MSG_WITH_EVENT,
    serialMethod: SERIAL_JSON,
    compressionType: GZIP,
  });

  const parts: Buffer[] = [header];

  // Event (4 bytes BE)
  const eventBuf = Buffer.allocUnsafe(4);
  eventBuf.writeUInt32BE(event, 0);
  parts.push(eventBuf);

  // Session ID (optional)
  if (sessionId !== undefined) {
    const sidBuf = Buffer.from(sessionId, "utf-8");
    const sidSizeBuf = Buffer.allocUnsafe(4);
    sidSizeBuf.writeUInt32BE(sidBuf.length, 0);
    parts.push(sidSizeBuf, sidBuf);
  }

  // Payload (gzip compressed JSON)
  const raw = Buffer.from(JSON.stringify(payload), "utf-8");
  const compressed = gzipSync(raw);
  const payloadSizeBuf = Buffer.allocUnsafe(4);
  payloadSizeBuf.writeUInt32BE(compressed.length, 0);
  parts.push(payloadSizeBuf, compressed);

  return Buffer.concat(parts);
}

/**
 * Build an audio-only request frame (raw PCM, gzip compressed).
 * Used for: TaskRequest(200)
 */
export function buildVolcRealtimeAudioFrame(
  event: number,
  audio: Buffer,
  sessionId: string,
): Buffer {
  const header = generateHeader({
    messageType: CLIENT_AUDIO_ONLY_REQUEST,
    messageTypeSpecificFlags: MSG_WITH_EVENT,
    serialMethod: NO_SERIALIZATION,
    compressionType: GZIP,
  });

  // Event (4 bytes BE)
  const eventBuf = Buffer.allocUnsafe(4);
  eventBuf.writeUInt32BE(event, 0);

  // Session ID
  const sidBuf = Buffer.from(sessionId, "utf-8");
  const sidSizeBuf = Buffer.allocUnsafe(4);
  sidSizeBuf.writeUInt32BE(sidBuf.length, 0);

  // Payload (gzip compressed audio)
  const compressed = gzipSync(audio);
  const payloadSizeBuf = Buffer.allocUnsafe(4);
  payloadSizeBuf.writeUInt32BE(compressed.length, 0);

  return Buffer.concat([header, eventBuf, sidSizeBuf, sidBuf, payloadSizeBuf, compressed]);
}

// ── Response parser ─────────────────────────────────────────────

/**
 * Parse a server binary response.
 */
export function parseVolcRealtimeResponse(data: Buffer): VolcRealtimeParsedResponse {
  if (data.length < 4) {
    return { messageType: "UNKNOWN", payloadSize: 0 };
  }

  const protocolVersion = data[0] >> 4;
  const headerSize = data[0] & 0x0f;
  const messageType = data[1] >> 4;
  const messageTypeSpecificFlags = data[1] & 0x0f;
  const serializationMethod = data[2] >> 4;
  const messageCompression = data[2] & 0x0f;
  let payload = data.subarray(headerSize * 4);

  const result: VolcRealtimeParsedResponse = {
    messageType: "UNKNOWN",
    payloadSize: 0,
  };

  // ── Error response ──
  if (messageType === SERVER_ERROR_RESPONSE) {
    result.messageType = "SERVER_ERROR_RESPONSE";
    if (payload.length >= 4) {
      result.code = payload.readUInt32BE(0);
    }
    if (payload.length >= 8) {
      const payloadSize = payload.readUInt32BE(4);
      result.payloadSize = payloadSize;
      const payloadMsg = payload.subarray(8);
      if (messageCompression === GZIP && payloadMsg.length > 0) {
        try {
          result.payloadMsg = gunzipSync(payloadMsg);
        } catch {
          result.payloadMsg = payloadMsg;
        }
      } else {
        result.payloadMsg = payloadMsg;
      }
    }
    return result;
  }

  // ── Full server response or ACK ──
  if (messageType === SERVER_FULL_RESPONSE || messageType === SERVER_ACK) {
    result.messageType = messageType === SERVER_ACK ? "SERVER_ACK" : "SERVER_FULL_RESPONSE";

    let start = 0;

    // Sequence number (NEG_SEQUENCE flag)
    if (messageTypeSpecificFlags & NEG_SEQUENCE) {
      if (payload.length >= 4) {
        result.seq = payload.readUInt32BE(0);
      }
      start += 4;
    }

    // Event (MSG_WITH_EVENT flag)
    if (messageTypeSpecificFlags & MSG_WITH_EVENT) {
      if (payload.length >= start + 4) {
        result.event = payload.readUInt32BE(start);
      }
      start += 4;
    }

    payload = payload.subarray(start);

    // Session ID
    if (payload.length >= 4) {
      const sessionIdSize = payload.readInt32BE(0);
      if (sessionIdSize > 0 && payload.length >= 4 + sessionIdSize) {
        result.sessionId = payload.subarray(4, 4 + sessionIdSize).toString("utf-8");
      }
      payload = payload.subarray(4 + sessionIdSize);
    }

    // Payload
    if (payload.length >= 4) {
      const payloadSize = payload.readUInt32BE(0);
      result.payloadSize = payloadSize;
      let payloadMsg = payload.subarray(4);

      if (payloadMsg.length > 0 && messageCompression === GZIP) {
        try {
          payloadMsg = gunzipSync(payloadMsg);
        } catch {
          // keep compressed payload on failure
        }
      }

      if (payloadMsg.length > 0 && serializationMethod === SERIAL_JSON) {
        try {
          result.payloadMsg = JSON.parse(payloadMsg.toString("utf-8"));
        } catch {
          result.payloadMsg = payloadMsg.toString("utf-8");
        }
      } else if (payloadMsg.length > 0) {
        result.payloadMsg = payloadMsg;
      }
    }

    return result;
  }

  return result;
}
