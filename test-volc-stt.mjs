/**
 * Test Volcengine STT (bigmodel_async) with same credentials.
 * If this works but realtime doesn't, confirms realtime service not activated.
 */

import { randomUUID } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { WebSocket } from "ws";

const APP_ID = "6897139964";
const ACCESS_TOKEN = "bsNydZqpKWMKuzLh-8BTVW25uVqyvqgU";
const WS_URL = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async";

function buildFullClientRequest(seq, payload) {
  const header = Buffer.alloc(4);
  header[0] = (0b0001 << 4) | 1;
  header[1] = (0b0001 << 4) | 0b0100;
  header[2] = (0b0001 << 4) | 0b0001;
  header[3] = 0x00;

  const seqBuf = Buffer.allocUnsafe(4);
  seqBuf.writeUInt32BE(seq, 0);

  const raw = Buffer.from(JSON.stringify(payload), "utf-8");
  const compressed = gzipSync(raw);
  const sizeBuf = Buffer.allocUnsafe(4);
  sizeBuf.writeUInt32BE(compressed.length, 0);

  return Buffer.concat([header, seqBuf, sizeBuf, compressed]);
}

function parseResponse(data) {
  if (data.length < 4) return null;
  const headerSize = data[0] & 0x0f;
  const msgType = data[1] >> 4;
  const flags = data[1] & 0x0f;
  const serial = data[2] >> 4;
  const compression = data[2] & 0x0f;
  let payload = data.subarray(headerSize * 4);

  let start = 0;
  if (flags & 0b0010) start += 4;
  if (flags & 0b0100) start += 4;
  payload = payload.subarray(start);

  if (payload.length >= 4) {
    const sidSize = payload.readInt32BE(0);
    payload = payload.subarray(4 + Math.max(0, sidSize));
  }
  if (payload.length >= 4) {
    const psize = payload.readUInt32BE(0);
    let pmsg = payload.subarray(4);
    if (compression === 0b0001 && pmsg.length > 0) {
      try {
        pmsg = gunzipSync(pmsg);
      } catch {}
    }
    if (serial === 0b0001 && pmsg.length > 0) {
      try {
        return JSON.parse(pmsg.toString("utf-8"));
      } catch {
        return pmsg.toString();
      }
    }
    return pmsg;
  }
  return null;
}

async function test() {
  console.log("=== Volcengine STT Test ===");
  console.log("URL:", WS_URL);

  const reqId = randomUUID();
  const headers = {
    "X-Api-App-Key": APP_ID,
    "X-Api-Access-Key": ACCESS_TOKEN,
    "X-Api-Resource-Id": "volc.seedasr.sauc.duration",
    "X-Api-Request-Id": reqId,
    "X-Api-Sequence": "-1",
  };
  console.log("Headers:", JSON.stringify(headers, null, 2));

  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL, { headers });
    const timeout = setTimeout(() => {
      ws.terminate();
      reject(new Error("Timeout"));
    }, 15000);

    ws.on("upgrade", (res) => {
      console.log("[UPGRADE] Status:", res.statusCode, res.statusMessage);
    });

    ws.on("open", () => {
      console.log("[OPEN] Connected!");
      ws.send(
        buildFullClientRequest(1, {
          user: { uid: "test" },
          audio: { format: "pcm", codec: "raw", rate: 16000, bits: 16, channel: 1 },
          request: {
            model_name: "bigmodel",
            enable_itn: true,
            enable_punc: true,
            result_type: "full",
            show_utterances: true,
            enable_nonstream: true,
            end_window_size: 400,
          },
        }),
      );
      console.log("[SEND] FullClientRequest sent");
    });

    ws.on("message", (data) => {
      const resp = parseResponse(data);
      console.log("[MESSAGE]:", JSON.stringify(resp, null, 2));
      if (resp && (resp.result || resp.text !== undefined)) {
        console.log("[OK] STT service responding — credentials are valid!");
        clearTimeout(timeout);
        ws.close();
        resolve("STT_OK");
      }
    });

    ws.on("error", (err) => {
      console.error("[ERROR]", err.message);
      clearTimeout(timeout);
      reject(err);
    });

    ws.on("close", () => {
      console.log("[CLOSE]");
    });
  });
}

test()
  .then((r) => console.log("\n=== RESULT:", r, "==="))
  .catch((err) => console.error("\n=== FAILED:", err.message, "==="));
