import { afterEach, describe, expect, it, vi } from "vitest";
import { buildVolcRealVoiceProvider } from "./volc-real-voice-provider.js";

describe("volcReal voice provider config", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("treats flat realtime.providers.volcReal-shaped rawConfig as configured", () => {
    vi.stubEnv("VOLCENGINE_ACCESS_TOKEN", "");
    const provider = buildVolcRealVoiceProvider();
    const raw = { appId: "app-1", accessToken: "token-1" };
    const providerConfig = provider.resolveConfig?.({
      cfg: {} as never,
      rawConfig: raw,
    });
    expect(providerConfig).toBeDefined();
    expect(
      provider.isConfigured?.({
        cfg: undefined,
        providerConfig: providerConfig!,
      }),
    ).toBe(true);
  });

  it("still reads legacy nested providers.volcReal", () => {
    vi.stubEnv("VOLCENGINE_ACCESS_TOKEN", "");
    const provider = buildVolcRealVoiceProvider();
    const raw = {
      providers: {
        volcReal: { appId: "app-2", accessToken: "token-2" },
      },
    };
    const providerConfig = provider.resolveConfig?.({
      cfg: {} as never,
      rawConfig: raw,
    });
    expect(
      provider.isConfigured?.({
        cfg: undefined,
        providerConfig: providerConfig!,
      }),
    ).toBe(true);
  });

  it("honors VOLCENGINE_ACCESS_TOKEN when flat config omits accessToken", () => {
    vi.stubEnv("VOLCENGINE_ACCESS_TOKEN", "env-token");
    const provider = buildVolcRealVoiceProvider();
    const raw = { appId: "app-3" };
    const providerConfig = provider.resolveConfig?.({
      cfg: {} as never,
      rawConfig: raw,
    });
    expect(
      provider.isConfigured?.({
        cfg: undefined,
        providerConfig: providerConfig!,
      }),
    ).toBe(true);
  });
});
