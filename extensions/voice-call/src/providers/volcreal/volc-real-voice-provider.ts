/**
 * VolcReal Realtime Voice Provider (S2S / Realtime API).
 *
 * Bridges OpenClaw voice-call realtime mode to Volcengine's end-to-end
 * realtime speech API via binary WebSocket protocol.
 */

import { randomUUID } from "node:crypto";
import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  convertPcmToMulaw8k,
  mulawToPcm,
  REALTIME_VOICE_AUDIO_FORMAT_G711_ULAW_8KHZ,
  resamplePcm,
} from "openclaw/plugin-sdk/realtime-voice";
import type {
  RealtimeVoiceBridge,
  RealtimeVoiceBridgeCallbacks,
  RealtimeVoiceBridgeCreateRequest,
  RealtimeVoiceProviderCapabilities,
  RealtimeVoiceProviderPlugin,
} from "openclaw/plugin-sdk/realtime-voice";
import WebSocket from "ws";
import {
  buildVolcRealtimeAudioFrame,
  buildVolcRealtimeControlFrame,
  parseVolcRealtimeResponse,
} from "./volc-realtime-protocol.js";

const WS_URL = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue";
const DEFAULT_APP_KEY = "PlgvMymc7f3tQnJ6";
const DEFAULT_RESOURCE_ID = "volc.speech.dialog";
const DEFAULT_SPEAKER = "zh_female_vv_jupiter_bigtts";
const DEFAULT_MODEL = "1.2.1.1";
const DEFAULT_MAX_DIALOG_PAIRS = 20;

// ── Config types ────────────────────────────────────────────────

type VolcRealtimeVoiceConfig = {
  appId?: string;
  accessToken?: string;
  appKey?: string;
  resourceId?: string;
  model?: string;
  speaker?: string;
  instructions?: string;
  dialogContextPath?: string;
  dialogContextMaxPairs?: number;
};

// ── Config normalization ────────────────────────────────────────

/** Keys that belong on a single `realtime.providers.volcReal` block (voice-call) or nested `providers.volcReal` (legacy plugin root). */
const VOLC_REAL_PROVIDER_KEYS = new Set([
  "appId",
  "accessToken",
  "appKey",
  "resourceId",
  "model",
  "speaker",
  "instructions",
  "dialogContextPath",
  "dialogContextMaxPairs",
]);

function recordHasVolcRealKeys(obj: Record<string, unknown>): boolean {
  for (const key of Object.keys(obj)) {
    if (VOLC_REAL_PROVIDER_KEYS.has(key)) {
      return true;
    }
  }
  return false;
}

function normalizeConfig(raw: Record<string, unknown>): VolcRealtimeVoiceConfig {
  let volc: Record<string, unknown> = {};
  const nestedVolc =
    typeof raw.providers === "object" && raw.providers !== null
      ? (raw.providers as Record<string, unknown>).volcReal
      : undefined;
  if (typeof nestedVolc === "object" && nestedVolc !== null) {
    volc = nestedVolc as Record<string, unknown>;
  } else if (recordHasVolcRealKeys(raw)) {
    // Voice-call passes `realtime.providers.<id>` as flat rawConfig; `isConfigured` also re-passes
    // the resolved object from `resolveConfig`, which has no `providers` wrapper.
    volc = raw;
  }

  return {
    appId: typeof volc.appId === "string" ? volc.appId : undefined,
    accessToken: typeof volc.accessToken === "string" ? volc.accessToken : undefined,
    appKey: typeof volc.appKey === "string" ? volc.appKey : undefined,
    resourceId: typeof volc.resourceId === "string" ? volc.resourceId : undefined,
    model: typeof volc.model === "string" ? volc.model : undefined,
    speaker: typeof volc.speaker === "string" ? volc.speaker : undefined,
    instructions: typeof volc.instructions === "string" ? volc.instructions : undefined,
    dialogContextPath:
      typeof volc.dialogContextPath === "string" ? volc.dialogContextPath : undefined,
    dialogContextMaxPairs:
      typeof volc.dialogContextMaxPairs === "number" ? volc.dialogContextMaxPairs : undefined,
  };
}

// ── dialog_context.md parser ────────────────────────────────────

type DialogContextDoc = { title: string; content: string };

function parseDialogContextMd(filePath: string): DialogContextDoc[] {
  if (!existsSync(filePath)) return [];
  const content = readFileSync(filePath, "utf-8");
  const sections = content.split(/^# /m).filter((s) => s.trim());
  const docs: DialogContextDoc[] = [];
  for (const section of sections) {
    const lines = section.split("\n");
    const title = lines[0]?.trim() ?? "";
    const body = lines.slice(1).join("\n").trim();
    if (title && body) {
      docs.push({ title, content: body });
    }
  }
  return docs;
}

function buildDialogContext(
  docs: DialogContextDoc[],
  maxPairs = DEFAULT_MAX_DIALOG_PAIRS,
): Array<{ role: string; text: string }> {
  const context: Array<{ role: string; text: string }> = [];
  for (const doc of docs.slice(0, maxPairs)) {
    context.push({ role: "user", text: `请了解以下知识：${doc.title}` });
    context.push({ role: "assistant", text: doc.content });
  }
  return context;
}

// ── Bridge implementation ───────────────────────────────────────

type VolcRealVoiceBridgeState =
  | "idle"
  | "connecting"
  | "handshaking"
  | "connected"
  | "closing"
  | "closed"
  | "error";

class VolcRealVoiceBridge implements RealtimeVoiceBridge {
  private ws: WebSocket | null = null;
  private state: VolcRealVoiceBridgeState = "idle";
  private sessionId: string | null = null;
  private config: VolcRealtimeVoiceConfig;
  private callbacks: RealtimeVoiceBridgeCallbacks;
  private dialogContext: Array<{ role: string; text: string }> = [];

  // Promise resolvers for handshake events keyed by event number
  private eventResolvers = new Map<number, (() => void)[]>();
  private eventTimeouts = new Map<number, ReturnType<typeof setTimeout>>();

  constructor(req: RealtimeVoiceBridgeCreateRequest & VolcRealtimeVoiceConfig) {
    this.config = {
      appId: req.appId,
      accessToken: req.accessToken,
      appKey: req.appKey,
      resourceId: req.resourceId,
      model: req.model,
      speaker: req.speaker,
      instructions: req.instructions,
      dialogContextPath: req.dialogContextPath,
      dialogContextMaxPairs: req.dialogContextMaxPairs,
    };
    this.callbacks = {
      onAudio: req.onAudio,
      onClearAudio: req.onClearAudio,
      onMark: req.onMark,
      onTranscript: req.onTranscript,
      onEvent: req.onEvent,
      onToolCall: req.onToolCall,
      onReady: req.onReady,
      onError: req.onError,
      onClose: req.onClose,
    };
  }

  // ── Connection lifecycle ──────────────────────────────────────

  async connect(): Promise<void> {
    if (this.state !== "idle") {
      throw new Error(`Cannot connect in state: ${this.state}`);
    }

    this.state = "connecting";

    // 1. Load dialog_context.md
    this.dialogContext = this.loadDialogContext();

    // 2. Build WebSocket headers
    // NOTE: Volcengine's server is case-sensitive on header names.
    // "X-Api-App-ID" (uppercase ID) is required; "X-Api-App-Id" returns 403.
    const headers: Record<string, string> = {
      "X-Api-App-ID": this.config.appId || "",
      "X-Api-Access-Key": this.config.accessToken || "",
      "X-Api-Resource-Id": this.config.resourceId || DEFAULT_RESOURCE_ID,
      "X-Api-App-Key": this.config.appKey || DEFAULT_APP_KEY,
      "X-Api-Connect-Id": randomUUID(),
    };

    // 3. Establish WebSocket
    this.ws = new WebSocket(WS_URL, { headers });

    this.ws.on("message", (data: Buffer) => {
      this.handleMessage(data);
    });

    this.ws.on("error", (error) => {
      this.state = "error";
      this.callbacks.onError?.(error);
    });

    this.ws.on("close", () => {
      this.state = "closed";
      this.callbacks.onClose?.("completed");
    });

    // Wait for open
    await new Promise<void>((resolve, reject) => {
      if (!this.ws) return reject(new Error("WebSocket not initialized"));
      this.ws.once("open", resolve);
      this.ws.once("error", reject);
    });

    this.state = "handshaking";

    // 4. Send StartConnection(1)
    await this.sendControlFrame(1, {});

    // 5. Wait for ConnectionStarted(50)
    await this.waitForEvent(50);

    // 6. Send StartSession(100)
    this.sessionId = randomUUID();
    const payload = this.buildStartSessionPayload();
    await this.sendControlFrame(100, payload, this.sessionId);

    // 7. Wait for SessionStarted(150)
    await this.waitForEvent(150);

    this.state = "connected";
    this.callbacks.onReady?.();
  }

  sendAudio(audio: Buffer): void {
    if (this.state !== "connected" || !this.ws || !this.sessionId) {
      return;
    }

    // Twilio μ-law 8kHz (160B/20ms) → PCM 8kHz int16 LE (320B)
    const pcm8k = mulawToPcm(audio);
    // PCM 8kHz → PCM 16kHz (640B)
    const pcm16k = resamplePcm(pcm8k, 8000, 16000);

    const frame = buildVolcRealtimeAudioFrame(200, pcm16k, this.sessionId);
    this.ws.send(frame);
  }

  setMediaTimestamp(_ts: number): void {
    // No-op for VolcReal provider
  }

  submitToolResult(_callId: string, _result: unknown): void {
    // VolcReal does not support tool calls
  }

  acknowledgeMark(): void {
    // No-op: mark mechanism not supported by VolcReal
  }

  close(): void {
    if (this.state === "closing" || this.state === "closed") {
      return;
    }
    this.state = "closing";

    try {
      // FinishSession(102)
      if (this.ws && this.sessionId && this.ws.readyState === WebSocket.OPEN) {
        const finishSessionFrame = buildVolcRealtimeControlFrame(102, {}, this.sessionId);
        this.ws.send(finishSessionFrame);
      }

      // FinishConnection(2)
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const finishConnectionFrame = buildVolcRealtimeControlFrame(2, {});
        this.ws.send(finishConnectionFrame);
      }
    } catch {
      // Ignore errors during close
    }

    try {
      this.ws?.close();
    } catch {
      // Ignore
    }

    this.state = "closed";
  }

  isConnected(): boolean {
    return this.state === "connected";
  }

  // ── Private helpers ───────────────────────────────────────────

  private loadDialogContext(): Array<{ role: string; text: string }> {
    let path = this.config.dialogContextPath;
    if (!path) {
      // Default: extension directory / dialog_context.md
      try {
        const currentFile = fileURLToPath(import.meta.url);
        path = join(dirname(currentFile), "dialog_context.md");
      } catch {
        return [];
      }
    }
    const docs = parseDialogContextMd(path);
    const maxPairs = this.config.dialogContextMaxPairs ?? DEFAULT_MAX_DIALOG_PAIRS;
    return buildDialogContext(docs, maxPairs);
  }

  private buildStartSessionPayload(): Record<string, unknown> {
    const docs = this.dialogContext;

    const payload: Record<string, unknown> = {
      asr: {
        audio_info: {
          format: "pcm",
          sample_rate: 16000,
          channel: 1,
        },
        extra: {
          end_smooth_window_ms: 1500,
        },
      },
      tts: {
        speaker: this.config.speaker || DEFAULT_SPEAKER,
        audio_config: {
          format: "pcm_s16le",
          sample_rate: 24000,
          channel: 1,
        },
      },
      dialog: {
        bot_name: "助手",
        system_role: this.config.instructions || "你是一个智能助手。",
        extra: {
          model: this.config.model || DEFAULT_MODEL,
          input_mod: "keep_alive",
        },
      },
    };

    if (docs.length > 0) {
      (payload.dialog as Record<string, unknown>).dialog_context = docs;
    }

    return payload;
  }

  private sendControlFrame(
    event: number,
    payload: Record<string, unknown>,
    sessionId?: string,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error("WebSocket not open"));
        return;
      }
      const frame = buildVolcRealtimeControlFrame(event, payload, sessionId);
      this.ws.send(frame, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private waitForEvent(expectedEvent: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.eventResolvers.delete(expectedEvent);
        this.eventTimeouts.delete(expectedEvent);
        reject(new Error(`Timeout waiting for event ${expectedEvent}`));
      }, 10000);

      this.eventTimeouts.set(expectedEvent, timeout);

      const existing = this.eventResolvers.get(expectedEvent) ?? [];
      existing.push(() => {
        clearTimeout(timeout);
        this.eventResolvers.delete(expectedEvent);
        this.eventTimeouts.delete(expectedEvent);
        resolve();
      });
      this.eventResolvers.set(expectedEvent, existing);
    });
  }

  private resolveEvent(event: number): void {
    const resolvers = this.eventResolvers.get(event);
    if (resolvers) {
      for (const r of resolvers) {
        r();
      }
      this.eventResolvers.delete(event);
    }
    const timeout = this.eventTimeouts.get(event);
    if (timeout) {
      clearTimeout(timeout);
      this.eventTimeouts.delete(event);
    }
  }

  private handleMessage(data: Buffer): void {
    const resp = parseVolcRealtimeResponse(data);

    if (resp.messageType === "SERVER_ERROR_RESPONSE") {
      this.callbacks.onError?.(new Error(`VolcReal server error: code=${resp.code ?? "unknown"}`));
      return;
    }

    if (resp.messageType !== "SERVER_FULL_RESPONSE" && resp.messageType !== "SERVER_ACK") {
      return;
    }

    const event = resp.event ?? 0;

    // ── Audio data (SERVER_ACK) ─────────────────────────────────
    if (resp.messageType === "SERVER_ACK" && event === 352) {
      // TTSResponse audio payload is raw PCM 24kHz int16 LE bytes
      const audioBuf = resp.payloadMsg;
      if (Buffer.isBuffer(audioBuf)) {
        const mulaw = convertPcmToMulaw8k(audioBuf, 24000);
        this.callbacks.onAudio(mulaw);
      }
      return;
    }

    // ── Control events (SERVER_FULL_RESPONSE) ───────────────────
    const payload = resp.payloadMsg as Record<string, unknown> | undefined;

    switch (event) {
      case 50: // ConnectionStarted
        this.resolveEvent(50);
        break;

      case 150: // SessionStarted
        this.resolveEvent(150);
        break;

      case 352: {
        // TTSResponse (fallback JSON path if not SERVER_ACK)
        const audioData = payload?.audio;
        if (audioData && typeof audioData === "string") {
          const pcm24k = Buffer.from(audioData, "base64");
          const mulaw = convertPcmToMulaw8k(pcm24k, 24000);
          this.callbacks.onAudio(mulaw);
        }
        break;
      }

      case 359: // TTSEnded
        this.callbacks.onEvent?.({ type: "response.done", direction: "server", detail: "" });
        break;

      case 450: // ASRInfo (user started speaking)
        this.callbacks.onEvent?.({
          type: "input_audio_buffer.speech_started",
          direction: "server",
          detail: "",
        });
        this.callbacks.onClearAudio();
        break;

      case 451: {
        // ASRResponse
        const text = typeof payload?.text === "string" ? payload.text : "";
        const isInterim = payload?.is_interim === true;
        this.callbacks.onTranscript?.("user", text, !isInterim);
        break;
      }

      case 459: // ASREnded
        this.callbacks.onEvent?.({
          type: "input_audio_buffer.speech_stopped",
          direction: "server",
          detail: "",
        });
        break;

      case 550: {
        // ChatResponse
        const text = typeof payload?.text === "string" ? payload.text : "";
        this.callbacks.onTranscript?.("assistant", text, false);
        break;
      }

      case 559: {
        // ChatEnded
        const text = typeof payload?.text === "string" ? payload.text : "";
        this.callbacks.onTranscript?.("assistant", text, true);
        break;
      }

      case 599: {
        // DialogCommonError
        const message = typeof payload?.message === "string" ? payload.message : "VolcReal error";
        this.callbacks.onError?.(new Error(message));
        break;
      }

      default:
        // Unknown event, ignore
        break;
    }
  }
}

// ── Provider factory ────────────────────────────────────────────

const CAPABILITIES: RealtimeVoiceProviderCapabilities = {
  transports: ["gateway-relay"],
  inputAudioFormats: [REALTIME_VOICE_AUDIO_FORMAT_G711_ULAW_8KHZ],
  outputAudioFormats: [REALTIME_VOICE_AUDIO_FORMAT_G711_ULAW_8KHZ],
  supportsBrowserSession: false,
  supportsBargeIn: true,
  supportsToolCalls: false,
  supportsVideoFrames: false,
  supportsSessionResumption: false,
};

export function buildVolcRealVoiceProvider(): RealtimeVoiceProviderPlugin {
  return {
    id: "volcReal",
    label: "VolcReal Realtime Voice",
    aliases: ["volcengine-realtime", "doubao-s2s"],
    autoSelectOrder: 20,
    capabilities: CAPABILITIES,

    resolveConfig: ({ rawConfig }) => {
      return normalizeConfig(rawConfig);
    },

    isConfigured: ({ providerConfig }) => {
      const cfg = normalizeConfig(providerConfig);
      return Boolean(cfg.accessToken || process.env.VOLCENGINE_ACCESS_TOKEN);
    },

    createBridge: (req) => {
      const cfg = normalizeConfig(req.providerConfig);
      const accessToken = cfg.accessToken || process.env.VOLCENGINE_ACCESS_TOKEN || "";
      if (!accessToken) {
        throw new Error("VolcReal access token missing");
      }
      return new VolcRealVoiceBridge({
        ...req,
        ...cfg,
        accessToken,
      });
    },
  };
}
