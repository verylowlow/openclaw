import { describe, expect, it } from "vitest";
import {
  isRedundantFollowUpTranscript,
  looksLikeGoodbyeIntent,
  mergeStreamTranscripts,
  shouldSkipFillerTranscript,
} from "./stream-transcript-policy.js";

describe("stream-transcript-policy", () => {
  it("skips filler-only transcripts", () => {
    expect(shouldSkipFillerTranscript("哦。")).toBe(true);
    expect(shouldSkipFillerTranscript("啊，那我没别的事情了，那就这样吧。")).toBe(false);
  });

  it("detects goodbye intent", () => {
    expect(looksLikeGoodbyeIntent("啊，那我没别的事情了，那就这样吧。")).toBe(true);
    expect(looksLikeGoodbyeIntent("北京今天气温多少？")).toBe(false);
  });

  it("merges rapid consecutive transcripts in one turn", () => {
    expect(mergeStreamTranscripts("嗯，播放完了，然后呢？", "后面呢？")).toBe(
      "嗯，播放完了，然后呢？ 后面呢？",
    );
  });

  it("treats substring follow-ups as redundant", () => {
    expect(
      isRedundantFollowUpTranscript(
        "你抢到多少钱的红包呀？",
        "你刚才跟我说你们老板发红包了，那你抢到多少钱的红包呀？",
      ),
    ).toBe(true);
  });
});
