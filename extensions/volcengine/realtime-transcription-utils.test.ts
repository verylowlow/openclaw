import { describe, expect, it } from "vitest";
import { resolveInProgressPartialFromUtterances } from "./realtime-transcription-utils.js";

describe("resolveInProgressPartialFromUtterances", () => {
  it("returns only non-definite utterance text", () => {
    expect(
      resolveInProgressPartialFromUtterances([
        { text: "你好。", definite: true },
        { text: "那你可以说话了哦。", definite: false },
      ]),
    ).toBe("那你可以说话了哦。");
  });

  it("returns empty string when all utterances are definite", () => {
    expect(resolveInProgressPartialFromUtterances([{ text: "你好。", definite: true }])).toBe("");
  });

  it("returns undefined for non-array input", () => {
    expect(resolveInProgressPartialFromUtterances(undefined)).toBeUndefined();
  });
});
