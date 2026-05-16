/**
 * Standalone test for Volcengine Realtime Dialogue API connectivity.
 * Tests multiple auth methods to find what works.
 */

import { randomUUID } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { WebSocket } from "ws";

// ── Config (from openclaw.json) ─────────────────────────────────
const APP_ID = "6897139964";
const ACCESS_TOKEN = "bsNydZqpKWMKuzLh-8BTVW25uVqyvqgU";
const RESOURCE_ID = "volc.speech.dialog";
const APP_KEY = "PlgvMymc7f3tQnJ6";
const WS_URL = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue";

// ── Protocol constants ──────────────────────────────────────────
const CLIENT_FULL_REQUEST = 0b0001;
const MSG_WITH_EVENT = 0b0100;
const SERIAL_JSON = 0b0001;
const GZIP = 0b0001;
const SERVER_FULL_RESPONSE = 0b1001;
const SERVER_ACK = 0b1011;
const SERVER_ERROR_RESPONSE = 0b1111;

function generateHeader(opts = {}) {
  const {
    messageType = CLIENT_FULL_REQUEST,
    messageTypeSpecificFlags = MSG_WITH_EVENT,
    serialMethod = SERIAL_JSON,
    compressionType = GZIP,
  } = opts;
  const header = Buffer.alloc(4);
  header[0] = (0b0001 << 4) | 1;
  header[1] = (messageType << 4) | messageTypeSpecificFlags;
  header[2] = (serialMethod << 4) | compressionType;
  header[3] = 0x00;
  return header;
}

function buildControlFrame(event, payload, sessionId) {
  const parts = [generateHeader()];
  const eventBuf = Buffer.allocUnsafe(4);
  eventBuf.writeUInt32BE(event, 0);
  parts.push(eventBuf);
  if (sessionId !== undefined) {
    const sidBuf = Buffer.from(sessionId, "utf-8");
    const sidSizeBuf = Buffer.allocUnsafe(4);
    sidSizeBuf.writeUInt32BE(sidBuf.length, 0);
    parts.push(sidSizeBuf, sidBuf);
  }
  const raw = Buffer.from(JSON.stringify(payload), "utf-8");
  const compressed = gzipSync(raw);
  const payloadSizeBuf = Buffer.allocUnsafe(4);
  payloadSizeBuf.writeUInt32BE(compressed.length, 0);
  parts.push(payloadSizeBuf, compressed);
  return Buffer.concat(parts);
}

function parseResponse(data) {
  if (data.length < 4) return { messageType: "UNKNOWN" };
  const headerSize = data[0] & 0x0f;
  const messageType = data[1] >> 4;
  const msgFlags = data[1] & 0x0f;
  const serialMethod = data[2] >> 4;
  const compression = data[2] & 0x0f;
  let payload = data.subarray(headerSize * 4);

  if (messageType === SERVER_ERROR_RESPONSE) {
    const code = payload.length >= 4 ? payload.readUInt32BE(0) : undefined;
    return { messageType: "SERVER_ERROR_RESPONSE", code };
  }

  if (messageType === SERVER_FULL_RESPONSE || messageType === SERVER_ACK) {
    const result = {
      messageType: messageType === SERVER_ACK ? "SERVER_ACK" : "SERVER_FULL_RESPONSE",
    };
    let start = 0;
    if (msgFlags & 0b0010) {
      start += 4;
    }
    if (msgFlags & 0b0100) {
      if (payload.length >= start + 4) {
        result.event = payload.readUInt32BE(start);
      }
      start += 4;
    }
    payload = payload.subarray(start);
    if (payload.length >= 4) {
      const sidSize = payload.readInt32BE(0);
      payload = payload.subarray(4 + Math.max(0, sidSize));
    }
    if (payload.length >= 4) {
      const psize = payload.readUInt32BE(0);
      let pmsg = payload.subarray(4);
      if (pmsg.length > 0 && compression === GZIP) {
        try {
          pmsg = gunzipSync(pmsg);
        } catch {}
      }
      if (pmsg.length > 0 && serialMethod === SERIAL_JSON) {
        try {
          result.payloadMsg = JSON.parse(pmsg.toString("utf-8"));
        } catch {
          result.payloadMsg = pmsg.toString("utf-8");
        }
      } else {
        result.payloadMsg = pmsg;
      }
    }
    return result;
  }
  return { messageType: "UNKNOWN" };
}

// ── Test runner ─────────────────────────────────────────────────

async function testWithHeaders(label, headers) {
  console.log(`\n========== TEST: ${label} ==========`);
  console.log("Headers:", JSON.stringify(headers, null, 2));

  return new Promise((resolve) => {
    const ws = new WebSocket(WS_URL, { headers });
    let resolved = false;

    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        console.log("[TIMEOUT] 15s elapsed");
        ws.terminate();
        resolve({ label, ok: false, error: "timeout" });
      }
    }, 15000);

    ws.on("upgrade", (response) => {
      console.log("[UPGRADE] Status:", response.statusCode, response.statusMessage);
      if (response.statusCode === 101) {
        console.log("[UPGRADE] -> 101 Switching Protocols (success!)");
      }
    });

    ws.on("open", () => {
      console.log("[OPEN] WebSocket connected!");
      ws.send(buildControlFrame(1, {}));
    });

    ws.on("message", (data) => {
      const resp = parseResponse(data);
      console.log("[MESSAGE] Type:", resp.messageType, "Event:", resp.event);
      if (resp.payloadMsg) {
        console.log("[MESSAGE] Payload:", JSON.stringify(resp.payloadMsg, null, 2));
      }

      if (resp.event === 50) {
        console.log("[OK] ConnectionStarted(50) — AUTH PASSED!");
        clearTimeout(timeout);
        resolved = true;
        ws.close();
        resolve({ label, ok: true });
      }
    });

    ws.on("error", (err) => {
      console.error("[ERROR]", err.message);
      if (!resolved) {
        clearTimeout(timeout);
        resolved = true;
        ws.terminate();
        resolve({ label, ok: false, error: err.message });
      }
    });

    ws.on("close", (code, reason) => {
      console.log("[CLOSE] Code:", code, "Reason:", reason.toString());
    });
  });
}

async function main() {
  console.log("=== VolcReal Connectivity Test Suite ===");
  console.log("URL:", WS_URL);
  console.log("AppId:", APP_ID);
  console.log("AccessToken:", ACCESS_TOKEN.slice(0, 8) + "...");

  const connectId = randomUUID();

  // Test 1: Original headers (X-Api-App-Id uppercase D)
  const r1 = await testWithHeaders("Original (X-Api-App-Id)", {
    "X-Api-App-Id": APP_ID,
    "X-Api-Access-Key": ACCESS_TOKEN,
    "X-Api-Resource-Id": RESOURCE_ID,
    "X-Api-App-Key": APP_KEY,
    "X-Api-Connect-Id": connectId,
  });

  // Test 2: Lowercase 'id' (X-Api-App-ID)
  const r2 = await testWithHeaders("Lowercase ID (X-Api-App-ID)", {
    "X-Api-App-ID": APP_ID,
    "X-Api-Access-Key": ACCESS_TOKEN,
    "X-Api-Resource-Id": RESOURCE_ID,
    "X-Api-App-Key": APP_KEY,
    "X-Api-Connect-Id": connectId,
  });

  // Test 3: With User-Agent
  const r3 = await testWithHeaders("With User-Agent", {
    "X-Api-App-Id": APP_ID,
    "X-Api-Access-Key": ACCESS_TOKEN,
    "X-Api-Resource-Id": RESOURCE_ID,
    "X-Api-App-Key": APP_KEY,
    "X-Api-Connect-Id": connectId,
    "User-Agent": "volcengine-audio/1.0",
  });

  // Test 4: With Authorization Bearer
  const r4 = await testWithHeaders("With Authorization Bearer", {
    "X-Api-App-Id": APP_ID,
    "X-Api-Access-Key": ACCESS_TOKEN,
    "X-Api-Resource-Id": RESOURCE_ID,
    "X-Api-App-Key": APP_KEY,
    "X-Api-Connect-Id": connectId,
    Authorization: `Bearer ${ACCESS_TOKEN}`,
  });

  // Test 5: Only Authorization Bearer (no X-Api headers)
  const r5 = await testWithHeaders("Only Authorization Bearer", {
    Authorization: `Bearer ${ACCESS_TOKEN}`,
    "X-Api-Connect-Id": connectId,
  });

  // Test 6: X-Api-Key instead of X-Api-Access-Key
  const r6 = await testWithHeaders("X-Api-Key instead of Access-Key", {
    "X-Api-App-Id": APP_ID,
    "X-Api-Key": ACCESS_TOKEN,
    "X-Api-Resource-Id": RESOURCE_ID,
    "X-Api-App-Key": APP_KEY,
    "X-Api-Connect-Id": connectId,
  });

  console.log("\n========== SUMMARY ==========");
  for (const r of [r1, r2, r3, r4, r5, r6]) {
    console.log(`${r.ok ? "✅" : "❌"} ${r.label}: ${r.ok ? "PASS" : r.error}`);
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
