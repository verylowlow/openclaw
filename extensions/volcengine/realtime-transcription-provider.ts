import crypto from "node:crypto";
import type {
  RealtimeTranscriptionProviderConfig,
  RealtimeTranscriptionProviderPlugin,
  RealtimeTranscriptionSession,
  RealtimeTranscriptionSessionCreateRequest,
} from "openclaw/plugin-sdk/realtime-transcription";
import { normalizeResolvedSecretInputString } from "openclaw/plugin-sdk/secret-input";
import { trimToUndefined } from "openclaw/plugin-sdk/speech-core";
import { WebSocket } from "ws";
import { convertMulawToPcm16k } from "./mulaw-to-pcm.js";
import {
  buildVolcAudioRequest,
  buildVolcFullClientRequest,
  parseVolcResponse,
} from "./volc-binary-protocol.js";

// ── Config types ────────────────────────────────────────────────

type VolcRealtimeConfig = {
  appId?: string;
  accessToken: string;
  resourceId: string;
  language?: string;
  enableItN: boolean;
  enablePunc: boolean;
  enableDdc: boolean;
  model: string;
  resultType: string;
  showUtterances: boolean;
  enableNonstream: boolean;
};

// ── Constants ───────────────────────────────────────────────────

// Use bigmodel_async (bidirectional streaming optimized) endpoint.
// This is the officially recommended endpoint for real-time scenarios.
// NOTE: bidirectional streaming does NOT support the "language" field.
const VOLCENGINE_WS_URL = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async";

const VOLCENGINE_CONNECT_TIMEOUT_MS = 10_000;

// 200ms @ 16kHz × 16bit × 1ch = 6400 bytes (official recommended chunk size)
const AUDIO_CHUNK_BYTES = 6400;

// ── Config helpers ──────────────────────────────────────────────

function readRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function readBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return undefined;
}

function readNestedVolcConfig(
  rawConfig: RealtimeTranscriptionProviderConfig,
): Record<string, unknown> {
  const raw = readRecord(rawConfig);
  const providers = readRecord(raw?.providers);
  return readRecord(providers?.volcengine ?? raw?.volcengine) ?? readRecord(raw) ?? {};
}

function normalizeProviderConfig(config: RealtimeTranscriptionProviderConfig): VolcRealtimeConfig {
  const raw = readNestedVolcConfig(config);
  return {
    appId: trimToUndefined(raw?.appId),
    accessToken:
      normalizeResolvedSecretInputString({
        value: raw?.accessToken,
        path: "plugins.entries.voice-call.config.streaming.providers.volcengine.accessToken",
      }) ?? "",
    resourceId: trimToUndefined(raw?.resourceId) ?? "volc.seedasr.sauc.duration",
    language: trimToUndefined(raw?.language),
    enableItN: readBoolean(raw?.enableItN) ?? true,
    enablePunc: readBoolean(raw?.enablePunc) ?? true,
    enableDdc: readBoolean(raw?.enableDdc) ?? false,
    model: trimToUndefined(raw?.model) ?? "bigmodel",
    resultType: trimToUndefined(raw?.resultType) ?? "full",
    showUtterances: readBoolean(raw?.showUtterances) ?? false,
    enableNonstream: readBoolean(raw?.enableNonstream) ?? false,
  };
}

// ── Session factory ─────────────────────────────────────────────

function createVolcSession(
  req: RealtimeTranscriptionSessionCreateRequest,
  config: VolcRealtimeConfig,
): RealtimeTranscriptionSession {
  let ws: WebSocket | null = null;
  let connected = false;
  let pendingTranscript = "";
  let seq = 2; // seq=1 is reserved for full client request
  let audioAccumulator = Buffer.alloc(0);

  return {
    async connect() {
      const requestId = crypto.randomUUID();

      const headers: Record<string, string> = {
        "X-Api-App-Key": config.appId ?? "",
        "X-Api-Access-Key": config.accessToken,
        "X-Api-Resource-Id": config.resourceId,
        "X-Api-Request-Id": requestId,
        "X-Api-Sequence": "-1",
      };

      ws = new WebSocket(VOLCENGINE_WS_URL, { headers });

      ws.on("open", () => {
        connected = true;

        const configPayload: Record<string, unknown> = {
          user: { uid: "openclaw" },
          audio: {
            format: "pcm",
            codec: "raw",
            rate: 16000,
            bits: 16,
            channel: 1,
          },
          request: {
            model_name: config.model,
            enable_itn: config.enableItN,
            enable_punc: config.enablePunc,
            enable_ddc: config.enableDdc,
            result_type: config.resultType,
            show_utterances: config.showUtterances,
            enable_nonstream: config.enableNonstream,
          },
        };

        // NOTE: bidirectional streaming (bigmodel / bigmodel_async) does NOT
        // support the "language" field. It is silently ignored to avoid 400.
        if (config.language) {
          console.warn(
            `[volcengine] language="${config.language}" is ignored because ` +
              `bidirectional streaming does not support this field.`,
          );
        }

        try {
          ws!.send(buildVolcFullClientRequest(1, configPayload));
        } catch (err) {
          req.onError?.(
            err instanceof Error ? err : new Error("Failed to send full client request"),
          );
        }
      });

      ws.on("message", (data: Buffer, isBinary: boolean) => {
        if (!isBinary) return;
        const resp = parseVolcResponse(data);

        if (resp.errorCode !== 0) {
          req.onError?.(
            new Error(`Volcengine STT error: ${resp.errorMessage} (code: ${resp.errorCode})`),
          );
          return;
        }

        if (resp.text) {
          // Default "full" mode: resp.text is the complete current text.
          pendingTranscript = resp.text;
          req.onPartial?.(pendingTranscript);
        }

        if (resp.isLast) {
          req.onTranscript?.(pendingTranscript);
          pendingTranscript = "";
        }
      });

      ws.on("close", () => {
        connected = false;
      });

      ws.on("error", (err: Error) => {
        req.onError?.(err);
      });

      // Wait for connection (timeout 10s)
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(
            new Error(`Volcengine STT connection timeout (${VOLCENGINE_CONNECT_TIMEOUT_MS}ms)`),
          );
        }, VOLCENGINE_CONNECT_TIMEOUT_MS);

        const onOpen = () => {
          cleanup();
          resolve();
        };
        const onError = (err: Error) => {
          cleanup();
          reject(err);
        };

        const cleanup = () => {
          clearTimeout(timeout);
          ws!.off("open", onOpen);
          ws!.off("error", onError);
        };

        ws!.once("open", onOpen);
        ws!.once("error", onError);
      });
    },

    sendAudio(muLawBuffer: Buffer) {
      if (!connected || !ws) return;

      // μ-law → PCM 16kHz 16bit LE
      const pcmBuffer = convertMulawToPcm16k(muLawBuffer);

      // Accumulate
      audioAccumulator = Buffer.concat([audioAccumulator, pcmBuffer]);

      // Send in 6400-byte chunks (~200ms)
      while (audioAccumulator.length >= AUDIO_CHUNK_BYTES) {
        const chunk = audioAccumulator.subarray(0, AUDIO_CHUNK_BYTES);
        audioAccumulator = audioAccumulator.subarray(AUDIO_CHUNK_BYTES);
        try {
          ws.send(buildVolcAudioRequest(seq++, chunk, false));
        } catch {
          // Socket may have closed between check and send
          break;
        }
      }
    },

    close() {
      if (!ws) return;

      try {
        if (audioAccumulator.length > 0) {
          ws.send(buildVolcAudioRequest(seq, audioAccumulator, true));
        } else {
          ws.send(buildVolcAudioRequest(seq, Buffer.alloc(0), true));
        }
      } catch {
        // Best effort
      }

      // Give server 2s to return final result before closing
      setTimeout(() => {
        ws?.close();
      }, 2000);
    },

    isConnected() {
      return connected;
    },
  };
}

// ── Provider factory ────────────────────────────────────────────

export function buildVolcengineRealtimeTranscriptionProvider(): RealtimeTranscriptionProviderPlugin {
  return {
    id: "volcengine",
    label: "Volcengine Realtime Transcription",
    aliases: ["volcengine-realtime", "doubao-asr"],
    defaultModel: "bigmodel",
    autoSelectOrder: 25,

    resolveConfig: ({ rawConfig }) => normalizeProviderConfig(rawConfig),

    isConfigured: ({ providerConfig }) => {
      const cfg = normalizeProviderConfig(providerConfig);
      return Boolean(cfg.accessToken || process.env.VOLCENGINE_STT_TOKEN);
    },

    createSession: (req) => {
      const config = normalizeProviderConfig(req.providerConfig);
      const accessToken = config.accessToken || process.env.VOLCENGINE_STT_TOKEN;
      if (!accessToken) {
        throw new Error("Volcengine STT access token missing");
      }
      return createVolcSession(req, { ...config, accessToken });
    },
  };
}
